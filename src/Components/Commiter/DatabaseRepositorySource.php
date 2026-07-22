<?php

namespace KafkaBus\Laravel\Components\Commiter;

use DateTimeImmutable;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Builder;
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

final readonly class DatabaseRepositorySource implements RepositorySourceInterface, PurgeInterface
{
    public function __construct(
        private ConnectionInterface $connection,
        private string $table = 'kafka_bus_commits',
    ) {
    }

    public function get(string $key): ?Attempt
    {
        $row = $this->query()
            ->where('key', $key)
            ->first();

        if ($row === null) {
            return null;
        }

        return new Attempt(
            key: $row->key,
            number: (int) $row->number,
            commitedAt: $row->commited_at !== null
                ? new DateTimeImmutable($row->commited_at)
                : null,
        );
    }

    public function increment(string $key): void
    {
        $this->query()->upsert(
            [['key' => $key, 'number' => 1, 'commited_at' => null]],
            ['key'],
            ['number' => $this->connection->raw('excluded.number + 1')]
        );
    }

    public function commit(string $key): void
    {
        $commitedAt = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        $this->query()->upsert(
            [['key' => $key, 'number' => 1, 'commited_at' => $commitedAt]],
            ['key'],
            ['number' => $this->connection->raw('excluded.number + 1'), 'commited_at' => $commitedAt]
        );
    }

    public function purge(\DateTimeImmutable $before): int
    {
        return $this->query()
            ->whereNotNull('commited_at')
            ->where('commited_at', '<', $before->format('Y-m-d H:i:s'))
            ->delete();
    }

    private function query(): Builder
    {
        return $this->connection->table($this->table);
    }
}
