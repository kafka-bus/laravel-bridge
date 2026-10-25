<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Workers;

use KafkaBus\Worker\Registry\WorkerRegistryInterface;
use KafkaBus\Worker\Worker;

/**
 * @internal
 */
final class WorkerRegistry implements WorkerRegistryInterface
{
    /**
     * @var array<string, Worker>
     */
    private array $cached = [];

    public function __construct(
        private readonly WorkerFactoryInterface $workerFactory
    ) {
    }

    public function get(string $workerName): ?Worker
    {
        if (! $this->workerFactory->has($workerName)) {
            return null;
        }

        return $this->cached[$workerName] ??= $this->workerFactory->create($workerName);
    }
}
