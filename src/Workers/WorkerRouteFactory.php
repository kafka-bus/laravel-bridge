<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Workers;

use Illuminate\Config\Repository;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Factories\OptionsMerger;
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Worker;

final readonly class WorkerRouteFactory implements WorkerFactoryInterface
{
    /**
     * @var array<string, mixed>
     */
    private array $routes;

    private OptionsMerger $optionsMerger;

    public function __construct(
        private TopicRegistry $topicRegistry,
        Repository $config,
    ) {
        $this->optionsMerger = new OptionsMerger($config->get('kafka-bus.consumers', []));
        $this->routes = $config->get('kafka-bus.consumers.routes', []);
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_keys($this->routes);
    }

    public function has(string $workerName): bool
    {
        return isset($this->routes[$workerName]);
    }

    public function create(string $workerName): Worker
    {
        if (!$this->has($workerName)) {
            throw new KafkaBusConfigurationException("Route [$workerName] not registered in kafka-bus.php");
        }

        $options = $this->optionsMerger->merge([]);

        return new Worker(
            name: $workerName,
            topics: [$this->topicRegistry->get($workerName)],
            options: new Options(
                additionalOptions: $options['additional_options'],
                autoCommit: $options['auto_commit'] ?? true,
                consumerTimeout: $options['consumer_timeout'] ?? 5000,
            ),
        );
    }
}
