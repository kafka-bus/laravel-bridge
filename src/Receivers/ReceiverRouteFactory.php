<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Receivers;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Container\BindingResolutionException;
use KafkaBus\Core\Receivers\Routing\HandlerExecutor;
use KafkaBus\Core\Receivers\Routing\Route;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Factories\MiddlewareMerger;

/**
 * @internal
 */
final readonly class ReceiverRouteFactory
{
    /**
     * @var array<class-string, mixed>
     */
    private array $routes;

    private MiddlewareMerger $middlewareMerger;

    public function __construct(
        private Container $container,
        Repository $config
    ) {
        $this->middlewareMerger = new MiddlewareMerger(config('kafka-bus.consumers.middleware', []));
        $this->routes = $config->get('kafka-bus.consumers.routes', []);
    }

    /**
     * @param Topic $topic
     * @return Route
     *
     * @throws BindingResolutionException
     */
    public function create(Topic $topic): Route
    {
        $route = $this->routes[$topic->key]
            ?? throw new KafkaBusConfigurationException("Route [$topic->key] not registered in kafka-bus.php");

        if (is_string($route)) {
            $route = [
                'handler' => $route,
                'middleware' => [],
            ];
        }

        $middleware = $this->middlewareMerger->merge($route['middleware'] ?? []);

        return new Route(
            topic: $topic,
            handler: HandlerExecutor::fromHandler($this->container->make($route['handler'])),
            middleware: array_map($this->container->make(...), $middleware),
        );
    }
}
