<?php

namespace KafkaBus\Laravel\Publishers;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use KafkaBus\Commiter\Middleware\PublisherIdempotencyMiddleware;
use KafkaBus\Core\Publishers\Routing\Options;
use KafkaBus\Core\Publishers\Routing\Route;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Factories\MiddlewareMerger;
use KafkaBus\Laravel\Factories\OptionsMerger;
use KafkaBus\TypedTopics\PublisherTypedTopicMiddleware;

final readonly class PublisherRouteFactory
{
    public const DEFAULT = [
        PublisherIdempotencyMiddleware::class,
        PublisherTypedTopicMiddleware::class,
    ];

    /**
     * @var array<class-string, mixed>
     */
    private array $routes;

    private OptionsMerger $optionsMerger;

    public function __construct(
        private Container $container,
        private TopicRegistry $topicRegistry,
        Repository $config
    ) {
        $globalMiddleware = (new MiddlewareMerger(self::DEFAULT))
            ->merge(config('kafka-bus.producers.middleware', []));

        $options = array_merge(
            $config->get('kafka-bus.producers', []),
            ['middleware' => $globalMiddleware]
        );

        $this->optionsMerger = new OptionsMerger($options);
        $this->routes = $config->get('kafka-bus.producers.routes', []);
    }

    public function create(string $messageClass): Route
    {
        $route = $this->routes[$messageClass]
            ?? throw new KafkaBusConfigurationException("Message [$messageClass] not registered in kafka-bus.php");

        if (is_string($route)) {
            return new Route(
                messageClass: $messageClass,
                topic: $this->topicRegistry->get($route),
                options: $this->makeOptions()
            );
        }

        $topicKey = $route['topic_key']
            ?? throw new KafkaBusConfigurationException("Param [kafka-bus.producers.routes.$messageClass.topic_key] is required]");

        return new Route(
            messageClass: $messageClass,
            topic: $this->topicRegistry->get($topicKey),
            options: $this->makeOptions($route)
        );
    }

    private function makeOptions(array $routeOptions = []): Options
    {
        $options = $this->optionsMerger
            ->merge($routeOptions);

        return new Options(
            additionalOptions: $options['additional_options'],
            middleware: array_map($this->container->make(...), $options['middleware']),
            flushTimeout: $options['flush_timeout'] ?? 5000,
            flushRetries: $options['flush_retries'] ?? 5,
        );
    }
}
