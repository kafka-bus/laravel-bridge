<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Laravel\Listeners\WorkerFactory;

final class KafkaWorkerListCommand extends Command
{
    protected $signature = 'kafka:worker:list';
    protected $description = 'Display the list of registered Kafka workers and their topic bindings';

    public function handle(WorkerFactory $workerFactory): int
    {
        $workerNames = $workerFactory->names();

        if ($workerNames === []) {
            $this->warn('No workers registered');

            return self::SUCCESS;
        }

        $rows = [];

        foreach ($workerNames as $workerName) {
            $worker = $workerFactory->create($workerName);

            foreach ($worker->topics() as $topic) {
                $route = $worker->routes->get($topic->name);

                $rows[] = [
                    $workerName,
                    $topic->key,
                    $topic->name,
                    is_object($route?->handler) ? get_class($route->handler) : '?',
                    implode("\n", array_map(get_class(...), $worker->options->middleware)),
                    implode("\n", array_map(get_class(...), $route?->middleware ?: [])),
                ];
            }
        }

        $this->table(
            ['Worker', 'Topic key', 'Topic name', 'Handler', 'Consumer Middleware', 'Route Middleware'],
            $rows
        );

        return self::SUCCESS;
    }
}
