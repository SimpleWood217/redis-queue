<?php

namespace Wood\RedisQueue\Contracts;

use Wood\RedisQueue\Message;

interface ConsumerInterface
{
    public function consume(Message $message): void;

    public function onConsumptionFailure(Message $message): void;
}