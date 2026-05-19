<?php

namespace Wood\RedisQueue\Contracts;

interface MessageInterface
{
    // 获取基础信息
    public function getId(): string;

    public function getQueueName(): string;

    public function getPayload(): array;

    // 重试机制相关
    public function getAttempts(): int;

    public function incrementAttempts(): void;

    public function getDate(): ?string;

    public function getTimestamp(): int;

    // 序列化能力（因为存入 Redis 必须是字符串）
    public function toJson(): string;
}