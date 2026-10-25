<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use RdKafka\Exception as RdKafkaException;

final class KafkaConsumerOffsetShowCommand extends Command
{
    protected $signature = 'kafka:consumer:offset:show {topicKey? : Topic key from kafka-bus.topics, omit to show all consumed topics}';
    protected $description = 'Show current/min/max offsets for each partition of consumed topics';

    public function handle(PartitionsFactory $partitionsFactory): int
    {
        $topicKey = $this->argument('topicKey');

        try {
            $partitions = $partitionsFactory->create($topicKey === null ? null : [$topicKey])
                ->list();

            $rows = [];

            foreach ($partitions as $partition) {
                $rows[] = [
                    $partition->topic->key,
                    $partition->topic->name,
                    $partition->id === -1 ? 'n/a' : (string) $partition->id,
                    (string) $partition->currentOffset,
                    (string) $partition->minOffset,
                    (string) $partition->maxOffset,
                ];
            }

            if ($rows === []) {
                $this->warn('No partitions to display');

                return self::SUCCESS;
            }

            $this->table(
                ['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max'],
                $rows
            );

            return self::SUCCESS;
        }
        catch (TopicCannotResolvedException|RdKafkaException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }
    }
}
