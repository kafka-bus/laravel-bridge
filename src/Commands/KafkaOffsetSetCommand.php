<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Bus\Listeners\Partitions\CommitOffset;
use KafkaBus\Core\Bus\Listeners\Partitions\Offset;
use KafkaBus\Core\Exceptions\Listeners\CannotCommitOffsetException;
use KafkaBus\Core\Exceptions\Listeners\ListenerException;
use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Topics\TopicRegistry;

final class KafkaOffsetSetCommand extends Command
{
    protected $signature = 'kafka:offset:set
        {workerName : Worker name from kafka-bus.consumers.workers}
        {topicKey   : Topic key from kafka-bus.topics}
        {offset     : earliest, latest, or numeric offset}
        {--partition= : Partition id, omit to apply to all partitions}';

    protected $description = 'Set committed offsets for a worker topic';

    public function handle(BusInterface $bus, TopicRegistry $topicRegistry): int
    {
        $workerName = $this->argument('workerName');
        $topicKey = $this->argument('topicKey');
        $offsetInput = $this->argument('offset');
        $partitionInput = $this->option('partition');

        try {
            $topic = $topicRegistry->get($topicKey);
            $offset = $this->resolveOffset($offsetInput);

            if ($offset === null) {
                $this->error("Invalid offset '$offsetInput'. Use 'earliest', 'latest', or a non-negative integer.");

                return self::FAILURE;
            }

            $partition = $partitionInput === null ? RD_KAFKA_PARTITION_UA : (int) $partitionInput;

            $results = $bus->listener($workerName)
                ->partitions()
                ->setOffset(new CommitOffset($topic, $partition, $offset));

            $rows = array_map(
                fn ($result) => [
                    $result->topic->key,
                    $result->topic->name,
                    (string) $result->partition,
                    (string) $result->oldOffset,
                    (string) $result->newOffset,
                ],
                $results
            );

            $this->table(
                ['Topic key', 'Topic name', 'Partition', 'Old', 'New'],
                $rows
            );

            return self::SUCCESS;
        }
        catch (CannotCommitOffsetException|TopicCannotResolvedException|ListenerException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }
    }

    private function resolveOffset(string $value): Offset|int|null
    {
        return match (strtolower($value)) {
            'earliest', 'early', 'beginning' => Offset::Early,
            'latest', 'end' => Offset::Latest,
            default => ctype_digit($value) ? (int) $value : null,
        };
    }
}
