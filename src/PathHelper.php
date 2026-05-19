<?php

namespace Wood\RedisQueue;

class PathHelper
{
    /**
     * 工业级黑魔法：精准定位宿主项目根目录
     */
    public static function getHostRoot(): string
    {
        // 1. 寻找 Composer 自动加载器的类存在的位置
        if (class_exists(\Composer\Autoload\ClassLoader::class)) {
            $reflection = new \ReflectionClass(\Composer\Autoload\ClassLoader::class);
            // 拿到 vendor/composer/ClassLoader.php 的物理路径
            $vendorDir = dirname($reflection->getFileName(), 2);
            // 再往上一层，就是宿主项目根目录
            return dirname($vendorDir) . DIRECTORY_SEPARATOR;
        }

        // 2. 如果是本地开发、没通过 composer 加载（比如在包的 test/index.php 里直接 require）
        // 那就认为包的根目录就是项目根目录
        return realpath(dirname(__DIR__, 2)) . DIRECTORY_SEPARATOR;
    }
}