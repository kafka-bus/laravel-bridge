<?php

namespace KafkaBus\Laravel\Commands;

use KafkaBus\Laravel\Workers\WorkerRouteFactory;

final class KafkaConsumeRoutesCommand extends AbstractKafkaConsumeCommand
{
    protected $signature = 'kafka:consume:routes {--expect=*}';
    protected $description = 'Reading messages from Apache Kafka for all routes';

    protected string $factoryClass = WorkerRouteFactory::class;

    protected function getWorkerNames(): array
    {
        return array_values(array_diff(
            app(WorkerRouteFactory::class)->names(),
            $this->option('expect'),
        ));
    }
}
