<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use KafkaBus\Metadata\Exceptions\CannotCommitOffsetException;
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\Offset;
use RdKafka\Exception as RdKafkaException;

final class KafkaConsumerOffsetSetCommand extends Command
{
    protected $signature = 'kafka:consumer:offset:set
        {topicKey   : Topic key from kafka-bus.topics}
        {offset     : earliest, latest, or numeric offset}
        {--partition= : Partition id, omit to apply to all partitions}';

    protected $description = 'Set committed offsets for a consumed topic';

    public function handle(PartitionsFactory $partitionsFactory, TopicRegistry $topicRegistry): int
    {
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

            $results = $partitionsFactory->create([$topicKey])
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
        catch (CannotCommitOffsetException|TopicCannotResolvedException|RdKafkaException $exception) {
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
