<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Receivers;

use KafkaBus\Core\Receivers\Routing\Route;
use KafkaBus\Core\Routing\LazyRoute;
use KafkaBus\Core\Routing\RouteCollection;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;

/**
 * Bus routes received messages by topic, so handlers of all workers are
 * collected into a single collection. Handlers and middleware are resolved
 * from the container only when the first message for the topic arrives.
 *
 * @internal
 */
final readonly class ReceiverRoutesFactory
{
    public function __construct(
        private TopicRegistry        $topicRegistry,
        private ReceiverRouteFactory $routeFactory,
    ) {
    }

    /**
     * @return RouteCollection<Route>
     *
     * @throws KafkaBusConfigurationException
     */
    public function create(): RouteCollection
    {
        $routes = new RouteCollection(Route::class);
        $topicKeys = array_keys(config('kafka-bus.consumers.routes'));

        foreach ($topicKeys as $topicKey) {
            $topic = $this->topicRegistry->get($topicKey);
            $routes->add(new LazyRoute($topic->name, fn () => $this->routeFactory->create($topic)));
        }

        return $routes;
    }
}
