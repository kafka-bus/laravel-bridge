<?php

namespace KafkaBus\Laravel\Commands;

final class KafkaConsumeAllCommand extends AbstractKafkaConsumeCommand
{
    protected $signature = 'kafka:consume:all {--expect=*}';
    protected $description = 'Reading messages from Apache Kafka for all workers';

    protected function getWorkerNames(): array
    {
        return array_diff(
            array_keys(config('kafka-bus.consumers.workers')),
            $this->option('expect'),
        );
    }
}
