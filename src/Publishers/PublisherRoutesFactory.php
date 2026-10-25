<?php

namespace KafkaBus\Laravel\Publishers;

use Illuminate\Config\Repository;
use KafkaBus\Core\Publishers\Routing\Route;
use KafkaBus\Core\Routing\LazyRoute;
use KafkaBus\Core\Routing\RouteCollection;

final readonly class PublisherRoutesFactory
{
    public function __construct(
        private Repository            $config,
        private PublisherRouteFactory $routeFactory,
    ) {
    }

    /**
     * @return RouteCollection<Route>
     */
    public function create(): RouteCollection
    {
        $routes = new RouteCollection(Route::class);
        $messageClasses = array_keys($this->config->get('kafka-bus.producers.routes', []));

        foreach ($messageClasses as $messageClass) {
            $routes->add(new LazyRoute($messageClass, fn () => $this->routeFactory->create($messageClass)));
        }

        return $routes;
    }
}
