<?php
date_default_timezone_set('Asia/Shanghai');

use Swoole\Timer;
use Wood\RedisQueue\Manager;
use Wood\RedisQueue\Test\Consumer\TestConsumer;

require_once __DIR__ . '/../vendor/autoload.php';

$c = new Manager(
    [
        'host' => '127.0.0.1',
    ],
    \Wood\RedisQueue\PathHelper::getHostRoot() . "test/Consumer/"
);
$cb = function () use ($c) {
    $c->send('test', ['123' => '456'], 15);
    echo '[' . date('Y-m-d H:i:s') . "] send delayed message" . PHP_EOL;
};
$id = Timer::tick(2000, $cb);
$cb();
Timer::after(4000, function () use ($id) {
//    Manager::shutdown();
    Timer::clear($id);
    echo '[' . date('Y-m-d H:i:s') . "] clear timer" . PHP_EOL;
});
$c->bootstrap();

//Timer::after(3000, function () use ($c) {
//    $c->send('test', [123], 3);
//    echo '[' . date('Y-m-d H:i:s') . "]" . PHP_EOL;
//});
//
//Container::getInstance()->bind('test_consumer', TestConsumer::class);
//$test_consumer = Container::getInstance()->get('test_consumer');
//
//$c->subscribe('test', [$test_consumer, 'consume']);