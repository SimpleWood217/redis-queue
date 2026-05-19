<?php

namespace Wood\RedisQueue\Exceptions;

use Exception;

class InvalidConsumerDirException extends Exception
{
    public function __construct(string $message = 'Consumer directory is not exist')
    {
        parent::__construct($message);
    }
}