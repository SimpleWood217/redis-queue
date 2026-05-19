# Redis Queue

基于 PHP + Swoole 协程的轻量级 Redis 消息队列服务，支持延迟消息、自动重试、失败队列等特性。

## 依赖

- PHP >= 8.3
- [ext-swoole](https://github.com/swoole/swoole-src)
- [predis/predis](https://github.com/predis/predis) ^3.4

## 安装

```bash
composer require wood/redis-queue
```

## 架构概览

系统内部维护三类队列，均基于 Redis 数据结构：

| 队列 | Redis 类型 | 说明 |
| --- | --- | --- |
| `queue-waiting:{name}` | List | 待消费队列，消息进入后由消费者 `brpop` 阻塞拉取 |
| `queue-delayed` | ZSet | 延迟队列，按到期时间戳排序，到期后自动转移到 waiting 队列 |
| `queue-failed` | List | 失败队列，超过最大重试次数或数据损坏的消息落入此处 |

**消息生命周期**：

```
send() ──delay=0──> queue-waiting ──brpop──> consumer.consume()
                        ↑                        │ (异常)
                        │                        ▼
send() ──delay>0──> queue-delayed ──(到期)── 重试/失败处理
                                              │          │
                                    (未超次数) ▼          ▼ (超次数)
                                    queue-delayed    queue-failed
```

## CLI 演示工具

项目内置了一个终端演示脚本，方便快速测试和体验：

```bash
# 查看帮助
php bin/demo help

# 启动消费服务（bootstrap 模式）
php bin/demo bootstrap --host=127.0.0.1 --consumer-dir=./test/Consumer/

# 发送一条消息
php bin/demo send --queue=test --data='{"uid":12345}'

# 发送一条延迟 10 秒的消息
php bin/demo send --queue=test --delay=10

# 运行完整演示（每 2 秒自动发一条消息 + bootstrap 消费）
php bin/demo demo --host=127.0.0.1 --consumer-dir=./test/Consumer/

# 手动订阅模式演示（不使用 bootstrap，手动 subscribe）
php bin/demo demo --host=127.0.0.1 --mode=subscribe
```

> 运行前请确保本地 Redis 服务已启动。

## 使用方式

### 方式一：手动订阅

适合需要精细控制订阅与回调的场景。

```php
<?php

use Wood\RedisQueue\Manager;
use Wood\RedisQueue\Message;

$manager = new Manager([
    'host' => '127.0.0.1',
]);

// 注册消费者
$manager->subscribe('test', function (Message $message) {
    // 消费逻辑
    $payload = $message->getPayload();
    echo "消费成功: " . json_encode($payload) . PHP_EOL;
});

// 注册失败处理器（可选）
$manager->setFailedQueue('test', function (Message $message) {
    echo "消费失败: " . $message->getErrorMsg() . PHP_EOL;
});

// 发送消息
$manager->send('test', ['key' => 'value']);       // 即时消息
$manager->send('test', ['key' => 'value'], 10);    // 延迟 10 秒
```

> **注意**：`subscribe()` 首次调用时会自动启动消息拉取引擎，调用后在协程环境中即可持续消费。

### 方式二：bootstrap 自动扫描（推荐）

将 Consumer 类按约定放置在统一目录下，由 `bootstrap()` 自动发现并注册。

```php
<?php

use Wood\RedisQueue\Manager;

$manager = new Manager(
    ['host' => '127.0.0.1'],
    __DIR__ . '/Consumer/'   // Consumer 类文件目录
);

$manager->bootstrap();  // 自动扫描目录，注册订阅，进入常驻运行
```

## 编写 Consumer

Consumer 必须实现 `Wood\RedisQueue\Contracts\ConsumerInterface` 接口，并声明 `$name` 属性（用作队列名称）。

```php
<?php

namespace Your\Namespace\Consumer;

use Wood\RedisQueue\Contracts\ConsumerInterface;
use Wood\RedisQueue\Message;

class TestConsumer implements ConsumerInterface
{
    public string $name = 'test';

    public function consume(Message $message): void
    {
        $payload = $message->getPayload();
        // 正常消费逻辑
    }

    public function onConsumptionFailure(Message $message): void
    {
        // 消费失败（consume 中抛出异常）时的处理
        echo $message->getErrorMsg() . PHP_EOL;
    }
}
```

`Message` 对象提供以下方法：

| 方法 | 说明 |
| --- | --- |
| `getId(): string` | 消息唯一 ID |
| `getQueueName(): string` | 所属队列名 |
| `getPayload(): array` | 消息体 |
| `getAttempts(): int` | 当前重试次数 |
| `getTimestamp(): int` | 创建时间戳 |
| `getDate(): ?string` | 创建日期字符串 |
| `getErrorMsg(): ?string` | 消费异常信息 |
| `getFallbackErrorMsg(): ?string` | 失败处理器异常信息 |

## 配置

构造 `Manager` 时传入的 `$config` 数组支持以下选项：

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | string | - | Redis 主机地址 |
| `port` | int | `6379` | Redis 端口 |
| `password` | string | - | Redis 密码 |
| `max_attempts` | int | `3` | 最大重试次数 |
| `retry_seconds` | int | `5` | 重试间隔基数（秒） |

所有 Redis 连接参数会透传给 Predis 客户端。

## 重试机制

当 `consume()` 抛出异常时：

1. 递增消息的 `attempts` 计数，记录错误信息
2. 调用 `onConsumptionFailure()`（失败处理器）
3. 未超过 `max_attempts`：按 `retry_seconds × attempts` 计算延迟后重新入队
4. 超过 `max_attempts`：消息移入 `queue-failed` 永久保留

失败处理器本身抛出异常不会影响重试流程，异常信息会记录到消息的 `fallback_error_msg` 中。
