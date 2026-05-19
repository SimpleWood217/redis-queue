<?php

namespace Wood\RedisQueue;

use co;
use FilesystemIterator;
use Predis\Client as RedisClient;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use Throwable;
use Wood\RedisQueue\Contracts\ConsumerInterface;
use Wood\RedisQueue\Exceptions\InvalidConsumerDirException;
use function Co\run;
use function Co\go;

class Manager
{
    const string QUEUE_WAITING = 'queue-waiting';
    const string QUEUE_DELAYED = 'queue-delayed';
    const string QUEUE_FAILED  = 'queue-failed';
    protected RedisClient $_redis;
    protected array       $_subscribeQueues = [];
    protected array       $_failedQueues    = [];
    protected bool        $_isPulling       = false;
    public bool           $_isRunning       = false;
    protected array       $_config          = [
        'max_attempts'  => 3,
        'retry_seconds' => 5,
    ];

    /**
     * @param array  $config       Redis 连接配置（host/port/password 等，透传给 Predis）
     *                             可选参数：max_attempts（最大重试次数，默认 3）、retry_seconds（重试间隔基数，默认 5 秒）
     * @param string $consumer_dir Consumer 类文件存放目录（供 bootstrap 自动扫描用）
     *
     * @throws InvalidConsumerDirException
     */
    public function __construct(
        array            $config = [],
        protected string $consumer_dir = ''
    ) {
        $this->_redis = new RedisClient($config);

        $this->_config = array_merge($this->_config, $config);

        if (!is_dir($consumer_dir)) {
            throw new InvalidConsumerDirException($consumer_dir);
        }
    }

    /**
     * 订阅指定队列，当队列中有消息到达时通过回调进行消费
     *
     * 首次订阅时会自动启动消息拉取引擎（pull），后续订阅复用已有引擎。
     * 回调接收一个 {@link Message} 实例作为参数。
     *
     * @param string   $queue_name 队列名称
     * @param callable $callback   消费回调，签名为 function(Message $message): void
     */
    public function subscribe(string $queue_name, callable $callback): void
    {
        // 注册订阅队列
        $key = self::QUEUE_WAITING . ':' . $queue_name;
        $this->_subscribeQueues[$key] = $callback;
        dump($this->_subscribeQueues);


        // 如果还没启动拉取引擎，则被动唤醒它
        if (!$this->_isPulling) {
            $this->pull();
        }
    }

    /**
     * 取消订阅指定队列，停止消费该队列中的消息
     *
     * @param string $queue_name 队列名称
     */
    public function unsubscribe(string $queue_name): void
    {
        $key = self::QUEUE_WAITING . ':' . $queue_name;
        unset($this->_subscribeQueues[$key]);
    }

    /**
     * 为指定队列注册消费失败处理器
     *
     * 当 consume 回调抛出异常时，会调用此回调进行失败处理。
     * 回调中可根据 {@link Message} 实例的错误信息决定后续操作。
     *
     * @param string   $queue_name 队列名称
     * @param callable $callback   失败处理回调，签名为 function(Message $message): void
     */
    public function setFailedQueue(string $queue_name, callable $callback): void
    {
        $key = self::QUEUE_WAITING . ':' . $queue_name;
        $this->_failedQueues[$key] = $callback;
    }

    /**
     * 向指定队列发送一条消息
     *
     * @param string $queue_name 目标队列名称
     * @param array  $data       消息体（关联数组）
     * @param int    $delay      延迟秒数，0 表示立即投递，>0 则为延迟消息（单位：秒）
     */
    public function send(string $queue_name, array $data, int $delay = 0): void
    {
        $id = uniqid('msg_', true);

        $message = new Message($id, $queue_name, $data);
        $package_str = $message->toJson();

        if ($delay == 0) {
            $this->_redis->lpush(
                self::QUEUE_WAITING . ':' . $queue_name,
                [$package_str]
            );
        } else {
            $this->_redis->zadd(self::QUEUE_DELAYED, [
                $package_str => $message->getTimestamp() + $delay
            ]);
        }
    }

    private function pull(): void
    {
        $this->_isPulling = true;
        $this->_isRunning = true;


        run(function () {
            $this->tryToPullDelayedQueue();
            if (empty($this->_subscribeQueues)) {
                return;
            }
            $redis = new RedisClient($this->_config);

            while ($this->_isRunning) {
                $result = $redis->brpop(array_keys($this->_subscribeQueues), 5);
                if (empty($result)) {
                    continue;
                }
                $key = $result[0];
                $package_str = $result[1];

                try {
                    $message = Message::fromJson($package_str);
                } catch (Throwable) {
                    $redis->lpush(self::QUEUE_FAILED, [$package_str]);
                    continue;
                }

                if (!isset($this->_subscribeQueues[$key])) {
                    $redis->lpush($key, [$package_str]);
                } else {
                    $callback = $this->_subscribeQueues[$key];
                    go(function () use ($callback, $message) {
                        try {
                            call_user_func($callback, $message);
                        } catch (Throwable $e) {
                            $message->incrementAttempts();
                            $message->setErrorMsg($e->getMessage());

                            if (isset($this->_failedQueues[$key])) {
                                try {
                                    call_user_func($this->_failedQueues[$key], $message);
                                } catch (Throwable $e) {
                                    $message->setFallbackErrorMsg($e->getMessage());
                                }

                                if ($message->getAttempts() > $this->_config['max_attempts']) {
                                    $this->fail($message);
                                } else {
                                    $this->retry($message);
                                }
                            }
                        }
                    });
                }
            }
            $this->_isPulling = false;
        });
    }

    private function tryToPullDelayedQueue(): void
    {
        go(function () {
            $redis = new RedisClient($this->_config);
            $options = ['LIMIT' => [0, 128]];
            while ($this->_isRunning) {
                $now = time();
                $result = $redis->zrangebyscore(self::QUEUE_DELAYED, '-inf', $now, $options);
                if ($result) {
                    foreach ($result as $package_str) {
                        $result = $redis->zrem(self::QUEUE_DELAYED, $package_str);
                        if ($result !== 1) {
                            continue;
                        }

                        try {
                            $message = Message::fromJson($package_str);
                        } catch (Throwable $e) {
                            $redis->lpush(self::QUEUE_FAILED, [$package_str]);
                            continue;
                        }

                        $redis->lpush(self::QUEUE_WAITING . ':' . $message->getQueueName(), [$package_str]);
                    }
                }
                Co::sleep(1);
            }
        });
    }

    private function retry(Message $message): void
    {
        $delay = time() + $this->_config['retry_seconds'] * $message->getAttempts();
        $this->_redis->zadd(self::QUEUE_DELAYED, [
            $message->toJson() => $delay
        ]);
    }

    private function fail(Message $message): void
    {
        $this->_redis->lpush(self::QUEUE_FAILED, [$message->toJson()]);
    }

    /**
     * 自动扫描 consumer_dir 并启动消息队列服务
     *
     * 递归遍历 $consumer_dir 下的所有 PHP 文件，发现所有实现了
     * {@link ConsumerInterface} 的类，自动完成订阅和失败处理器注册。
     * Consumer 类需声明 public string $name 属性作为队列名，
     * 未声明则默认使用 'default'。
     *
     * 调用此方法后进程会进入常驻运行状态（Swoole 协程事件循环）。
     *
     * @return void
     */
    public function bootstrap(): void
    {
        $result = [];

        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($this->consumer_dir, FilesystemIterator::SKIP_DOTS)
        );

        foreach ($iterator as $file) {
            if ($file->getExtension() !== 'php') {
                continue;
            }
            $filePath = $file->getRealPath();
            $content = file_get_contents($filePath);

            // 匹配 namespace
            if (preg_match('/namespace\s+([^;]+);/', $content, $nsMatch)) {
                $namespace = $nsMatch[1];
            } else {
                $namespace = '';
            }

            // 匹配 class 名称
            if (preg_match('/class\s+([^\s{]+)/', $content, $classMatch)) {
                $className = $classMatch[1];
                $fullClass = $namespace ? $namespace . '\\' . $className : $className;
                // 尝试加载类（确保已 autoload）
                if (!class_exists($fullClass)) {
                    continue;
                }
                // 判断是否实现接口
                if (in_array(ConsumerInterface::class, class_implements($fullClass), true)) {
                    $result[] = $fullClass;
                }
            }
        }
        foreach ($result as $class) {
            $consumer = Container::get($class);
            $consumer_name = $consumer->name ?? 'default';

            $this->subscribe($consumer_name, [$consumer, 'consume']);
            $this->setFailedQueue($consumer_name, [$consumer, 'onConsumptionFailure']);
        }
    }
}