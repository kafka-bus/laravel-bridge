<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Laravel\Workers\WorkerFactory;

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
        $routes = config('kafka-bus.consumers.routes', []);

        foreach ($workerNames as $workerName) {
            $worker = $workerFactory->create($workerName);

            foreach ($worker->topics as $topic) {
                $route = $routes[$topic->key] ?? null;

                $rows[] = [
                    $workerName,
                    $topic->key,
                    $topic->name,
                    is_array($route) ? ($route['handler'] ?? '?') : ($route ?? '?'),
                ];
            }
        }

        $this->table(
            ['Worker', 'Topic key', 'Topic name', 'Handler'],
            $rows
        );

        return self::SUCCESS;
    }
}
