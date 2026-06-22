<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Components\Commiter;

use DateTimeImmutable;
use Illuminate\Redis\Connections\PhpRedisConnection;
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

final readonly class RedisRepositorySource implements RepositorySourceInterface
{
    public function __construct(
        private PhpRedisConnection $connection,
        private string $prefix = 'kafka_bus_commits',
        private int $ttl = 14 * 86400,
    ) {
    }

    public function get(string $key): ?Attempt
    {
        /** @var array<string, string> $data */
        $data = $this->connection->command('hgetall', [$this->hashKey($key)]);

        if (empty($data)) {
            return null;
        }

        return new Attempt(
            key: $key,
            number: (int) $data['number'],
            commitedAt: $data['commited_at'] !== ''
                ? new DateTimeImmutable($data['commited_at'])
                : null,
        );
    }

    public function increment(string $key): void
    {
        $this->connection->eval(
            <<<'LUA'
            local key = KEYS[1]
            local ttl = tonumber(ARGV[1])
            if redis.call('HEXISTS', key, 'number') == 0 then
                redis.call('HSET', key, 'number', 1, 'commited_at', '')
            else
                redis.call('HINCRBY', key, 'number', 1)
            end
            if ttl and ttl > 0 then
                redis.call('EXPIRE', key, ttl)
            end
            LUA,
            1,
            $this->hashKey($key),
            (string) $this->ttl,
        );
    }

    public function commit(string $key): void
    {
        $commitedAt = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        $this->connection->eval(
            <<<'LUA'
            local key = KEYS[1]
            local committed_at = ARGV[1]
            local ttl = tonumber(ARGV[2])
            if redis.call('HEXISTS', key, 'number') == 0 then
                redis.call('HSET', key, 'number', 1, 'commited_at', committed_at)
            else
                redis.call('HINCRBY', key, 'number', 1)
                redis.call('HSET', key, 'commited_at', committed_at)
            end
            if ttl and ttl > 0 then
                redis.call('EXPIRE', key, ttl)
            end
            LUA,
            1,
            $this->hashKey($key),
            $commitedAt,
            (string) $this->ttl,
        );
    }

    private function hashKey(string $key): string
    {
        return "{$this->prefix}:{$key}";
    }
}
