<?php

namespace Wood\RedisQueue;

class Container
{
    private static array $instances = [];

    public static function get(string $class): object
    {
        if (isset(self::$instances[$class])) {
            return self::$instances[$class];
        }

        $instance = new $class();
        self::$instances[$class] = $instance;
        return $instance;
    }

    public static function count()
    {
        return count(self::$instances);
    }
}