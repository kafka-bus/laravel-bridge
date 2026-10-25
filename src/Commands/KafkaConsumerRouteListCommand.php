<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Laravel\Receivers\ReceiverRoutesFactory;

final class KafkaConsumerRouteListCommand extends Command
{
    protected $signature = 'kafka:consumer:route:list';
    protected $description = 'Display the list of routes registered in the receiver (topic → handler) and middleware';

    public function handle(ReceiverRoutesFactory $factory): int
    {
        $routes = $factory->create()
            ->all();

        if ($routes === []) {
            $this->warn('No consumer routes registered');

            return self::SUCCESS;
        }

        $rows = [];

        foreach ($routes as $route) {
            $rows[] = [
                $route->topic->key,
                $route->topic->name,
                is_object($route->handler) ? get_class($route->handler) : '?',
                implode("\n", array_map(get_class(...), $route->middleware)),
            ];
        }

        $this->table(
            ['Topic key', 'Topic name', 'Handler', 'Middleware'],
            $rows
        );

        return self::SUCCESS;
    }
}
