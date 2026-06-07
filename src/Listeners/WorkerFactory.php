<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Listeners;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Container\BindingResolutionException;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;
use KafkaBus\Core\Consumers\Messages\NativeMessageFactory;
use KafkaBus\Core\Consumers\Router\ConsumerRoutes;
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Factories\OptionsMerger;

/**
 * @internal
 */
final readonly class WorkerFactory
{
    private array $workers;

    private OptionsMerger $optionsMerger;

    public function __construct(
        private Container $container,
        private TopicRegistry $topicRegistry,
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
        return array_keys($this->workers);
    }

    /**
     * @param string $workerName
     * @return Worker
     *
     * @throws BindingResolutionException
     */
    public function create(string $workerName): Worker
    {
        $worker = $this->workers[$workerName]
            ?? throw new KafkaBusConfigurationException("Worker [$workerName] not registered in kafka-bus.php");

        if (is_string($worker)) {
            return new Worker(
                name: $workerName,
                routes: $this->makeConsumerRoutes([$workerName => ['handler' => $worker]]),
                options: $this->makeOptions()
            );
        }

        if (isset($worker['handler'])) {
            $topicKey = $worker['topic_key']
                ?? $workerName;

            return new Worker(
                name: $workerName,
                routes: $this->makeConsumerRoutes([$topicKey => ['handler' => $worker['handler']]]),
                options: $this->makeOptions($worker)
            );
        }

        $topics = array_map(
            fn (string|array $handler) => is_string($handler) ? ['handler' => $handler] : $handler,
            $worker['topics'] ?? []
        );

        return new Worker(
            name: $workerName,
            routes: $this->makeConsumerRoutes($topics),
            options: $this->makeOptions($worker)
        );
    }

    /**
     * @param array<string, array{
     *          handler: string,
     *          middleware?: list<class-string>
     *      }> $routes
     * @return ConsumerRoutes
     *
     * @throws BindingResolutionException
     */
    private function makeConsumerRoutes(array $routes): ConsumerRoutes
    {
        $builder = ConsumerRoutesBuilder::make($this->topicRegistry, new NativeMessageFactory());

        foreach ($routes as $topicKey => $topic) {
            $handler = $this->container->make($topic['handler']);
            $middleware = array_map($this->container->make(...), $topic['middleware'] ?? []);

            $builder->add(new RouteInfo($topicKey, $handler, $middleware));
        }

        return $builder->build();
    }

    private function makeOptions(array $workerOptions = []): Options
    {
        $options = $this->optionsMerger
            ->merge($workerOptions);

        $middleware = array_map($this->container->make(...), $options['middleware']);

        return new Options(
            additionalOptions: $options['additional_options'],
            middleware: $middleware,
            autoCommit: $options['auto_commit'] ?? true,
            consumerTimeout: $options['consumer_timeout'] ?? 5000,
        );
    }
}
