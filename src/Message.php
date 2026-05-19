<?php

namespace Wood\RedisQueue;

use InvalidArgumentException;
use Wood\RedisQueue\Contracts\MessageInterface;

class Message implements MessageInterface
{
    /**
     * @param string   $id         消息 ID
     * @param string   $queue_name 队列名称
     * @param array    $payload    消息负载
     * @param int      $attempts   重试次数
     * @param int|null $timestamp  创建时间
     */
    public function __construct(
        private readonly string $id,
        private readonly string $queue_name,
        private readonly array  $payload,
        private int             $attempts = 0,
        private ?string         $date = null,
        private ?int            $timestamp = null,
        private ?string         $error_msg = null,
        private ?string         $fallback_error_msg = null,
    ) {
        $this->timestamp = $this->timestamp ?? time();
        $this->date = $this->date ?? date('Y-m-d H:i:s', $this->timestamp);
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getQueueName(): string
    {
        return $this->queue_name;
    }

    public function getPayload(): array
    {
        return $this->payload;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function incrementAttempts(): void
    {
        $this->attempts++;
    }

    public function getDate(): ?string
    {
        return $this->date;
    }


    public function getTimestamp(): int
    {
        return $this->timestamp;
    }

    /**
     * @param string|null $fallback_error_msg
     */
    public function setFallbackErrorMsg(?string $fallback_error_msg): void
    {
        $this->fallback_error_msg = $fallback_error_msg;
    }

    /**
     * @return string|null
     */
    public function getFallbackErrorMsg(): ?string
    {
        return $this->fallback_error_msg;
    }

    /**
     * @return string|null
     */
    public function getErrorMsg(): ?string
    {
        return $this->error_msg;
    }

    /**
     * @param string|null $error_msg
     */
    public function setErrorMsg(?string $error_msg): void
    {
        $this->error_msg = $error_msg;
    }

    /**
     * 将 Message 实例转换为 JSON 字符串
     *
     * @return string
     */
    public function toJson(): string
    {
        return json_encode([
            'id'         => $this->id,
            'date'       => $this->date,
            'payload'    => $this->payload,
            'attempts'   => $this->attempts,
            'timestamp'  => $this->timestamp,
            'queue_name' => $this->queue_name,
        ], JSON_UNESCAPED_UNICODE);
    }

    /**
     * 将 Redis 里取出的 JSON  实例化为 Message 实例
     *
     * @param string $json
     *
     * @return self
     */
    public static function fromJson(string $json): self
    {
        $package = json_decode($json, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new InvalidArgumentException("JSON格式错误");
        }

        return new self(
            $package['id'],
            $package['queue_name'],
            $package['payload'] ?? [],
            $package['attempts'] ?? 0,
            $package['date'] ?? null,
            $package['timestamp'] ?? time(),
        );
    }
}