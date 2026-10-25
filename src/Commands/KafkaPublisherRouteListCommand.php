<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Laravel\Publishers\PublisherRoutesFactory;

final class KafkaPublisherRouteListCommand extends Command
{
    protected $signature = 'kafka:publisher:route:list';
    protected $description = 'Display the list of registered publisher message routes';

    public function handle(PublisherRoutesFactory $routesFactory): int
    {
        $routes = $routesFactory->create()
            ->all();

        if ($routes === []) {
            $this->warn('No routes registered');

            return self::SUCCESS;
        }

        $rows = [];

        foreach ($routes as $route) {
            $rows[] = [
                $route->messageClass,
                $route->topic->key,
                $route->topic->name,
                implode("\n", array_map(get_class(...), $route->options->middleware)),
            ];
        }

        $this->table(
            ['Message', 'Topic key', 'Topic name', 'Middleware'],
            $rows
        );

        return self::SUCCESS;
    }
}
