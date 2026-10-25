<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Workers;

use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Worker\Worker;

interface WorkerFactoryInterface
{
    /**
     * @return list<string>
     */
    public function names(): array;

    /**
     * @param string $workerName
     * @return bool
     */
    public function has(string $workerName): bool;

    /**
     * @param string $workerName
     * @return Worker
     *
     * @throws KafkaBusConfigurationException
     */
    public function create(string $workerName): Worker;
}
