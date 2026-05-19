<?php

namespace Wood\RedisQueue\Test\Consumer;

use Wood\RedisQueue\Contracts\ConsumerInterface;
use Wood\RedisQueue\Message;

class TestConsumer implements ConsumerInterface
{
    public string $name = 'test';

    public function consume(Message $message): void
    {
        static $count = 0;
        $count++;
        echo '[' . date('Y-m-d H:i:s') . "] Consume successfully, count: " . $count . PHP_EOL;
    }

    public function onConsumptionFailure(Message $message): void
    {
        echo '[' . date('Y-m-d H:i:s') . "] On consumption failure, message: " . $message->getErrorMsg() . PHP_EOL;
    }
}