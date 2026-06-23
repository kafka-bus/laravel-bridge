<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Listeners;

use Illuminate\Contracts\Container\BindingResolutionException;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;
use KafkaBus\Core\Interfaces\Bus\Listeners\WorkerRegistryInterface;

/**
 * @internal
 */
final class LaravelWorkerRegistry implements WorkerRegistryInterface
{
    /**
     * @var array<string, Worker>
     */
    private array $cached = [];

    public function __construct(
        private readonly WorkerFactory $workerFactory
    ) {
    }

    /**
     * @param string $workerName
     * @return Worker
     *
     * @throws BindingResolutionException
     */
    public function get(string $workerName): Worker
    {
        return $this->cached[$workerName] ??= $this->workerFactory->create($workerName);
    }
}
