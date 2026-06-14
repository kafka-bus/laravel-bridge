<?php

namespace KafkaBus\Laravel\Commands;

final class KafkaConsumeCommand extends AbstractKafkaConsumeCommand
{
    protected $signature = 'kafka:consume {workerName*}';
    protected $description = 'Reading messages from Apache Kafka by worker name';

    protected function getWorkerNames(): array
    {
        return $this->argument('workerName');
    }
}
