<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Exceptions\Listeners\ListenerException;
use KafkaBus\Core\Interfaces\Bus\BusInterface;

final class KafkaOffsetShowCommand extends Command
{
    protected $signature = 'kafka:offset:show {workerName}';
    protected $description = 'Show current/min/max offsets for each partition of a worker topics';

    public function handle(BusInterface $bus): int
    {
        $workerName = $this->argument('workerName');

        try {
            $partitions = $bus->listener($workerName)
                ->partitions()
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
                $this->warn("Worker '$workerName' has no partitions to display");

                return self::SUCCESS;
            }

            $this->table(
                ['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max'],
                $rows
            );

            return self::SUCCESS;
        }
        catch (ListenerException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }
    }
}
