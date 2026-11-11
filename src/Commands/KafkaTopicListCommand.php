<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use RdKafka\Exception as RdKafkaException;

final class KafkaTopicListCommand extends Command
{
    protected $signature = 'kafka:topic:list';
    protected $description = 'Show configured topics, partition offsets and availability';

    public function handle(TopicRegistry $topics, PartitionsFactory $partitionsFactory): int
    {
        $rows = [];
        $failed = false;

        foreach ($topics->all() as $topic) {
            $topicRows = [];

            try {
                foreach ($partitionsFactory->create([$topic->key])->list() as $partition) {
                    $available = $partition->id >= 0 && $partition->minOffset >= 0 && $partition->maxOffset >= 0;
                    $failed = $failed || ! $available;

                    $topicRows[] = [
                        $topic->key,
                        $topic->name,
                        $partition->id < 0 ? 'n/a' : (string) $partition->id,
                        $partition->currentOffset < 0 ? 'n/a' : (string) $partition->currentOffset,
                        $partition->minOffset < 0 ? 'n/a' : (string) $partition->minOffset,
                        $partition->maxOffset < 0 ? 'n/a' : (string) $partition->maxOffset,
                        $available ? 'OK' : 'Unavailable: topic missing or inaccessible',
                    ];
                }

                if ($topicRows === []) {
                    $failed = true;
                    $topicRows[] = [$topic->key, $topic->name, 'n/a', 'n/a', 'n/a', 'n/a', 'Unavailable: no partitions returned'];
                }
            }
            catch (RdKafkaException $exception) {
                $failed = true;
                $topicRows = [[$topic->key, $topic->name, 'n/a', 'n/a', 'n/a', 'n/a', 'Error: '.$exception->getMessage()]];
            }

            array_push($rows, ...$topicRows);
        }

        if ($rows === []) {
            $this->warn('No topics configured');

            return self::SUCCESS;
        }

        $this->table(['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max', 'Status'], $rows);

        return $failed ? self::FAILURE : self::SUCCESS;
    }
}
