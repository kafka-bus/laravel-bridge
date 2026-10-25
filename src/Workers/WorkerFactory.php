<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Workers;

use Illuminate\Config\Repository;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Factories\OptionsMerger;
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Worker;

/**
 * @internal
 */
final readonly class WorkerFactory implements WorkerFactoryInterface
{
    /**
     * @var array<string, array<string, mixed>>
     */
    private array $workers;

    private OptionsMerger $optionsMerger;

    public function __construct(
        private TopicRegistry $topicRegistry,
        private WorkerRouteFactory $workerRouteFactory,
        Repository $config,
    ) {
        $this->optionsMerger = new OptionsMerger($config->get('kafka-bus.consumers', []));
        $this->workers = $config->get('kafka-bus.consumers.workers', []);
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_values(array_unique([
            ...array_keys($this->workers),
            ...$this->workerRouteFactory->names(),
        ]));
    }

    public function has(string $workerName): bool
    {
        return isset($this->workers[$workerName]) || $this->workerRouteFactory->has($workerName);
    }

    /**
     * @param string $workerName
     * @return Worker
     *
     * @throws KafkaBusConfigurationException
     */
    public function create(string $workerName): Worker
    {
        if ($this->has($workerName) && !isset($this->workers[$workerName])) {
            return $this->workerRouteFactory
                ->create($workerName);
        }

        $worker = $this->workers[$workerName]
            ?? throw new KafkaBusConfigurationException("Worker [$workerName] not registered in kafka-bus.php");

        $topicsKeys = $worker['topics'] ?? [$workerName];
        $options = $this->optionsMerger->merge($worker);

        return new Worker(
            name: $workerName,
            topics: array_map($this->topicRegistry->get(...), $topicsKeys),
            options: new Options(
                additionalOptions: $options['additional_options'],
                autoCommit: $options['auto_commit'] ?? true,
                consumerTimeout: $options['consumer_timeout'] ?? 5000,
            ),
        );
    }
}
