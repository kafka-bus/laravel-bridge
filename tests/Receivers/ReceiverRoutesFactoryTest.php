<?php

declare(strict_types=1);

use Illuminate\Contracts\Container\BindingResolutionException;
use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Receivers\Routing\Route;
use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Core\Testing\Messages\VoidConsumerHandlerFaker;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Receivers\ReceiverRoutesFactory;
use KafkaBus\Laravel\Tests\Fixtures\MessageMiddlewareFake;
use KafkaBus\Laravel\Tests\Fixtures\OtherMessageMiddlewareFake;

beforeEach(function () {
    config()->set('kafka-bus.topics', [
        'products' => 'test-products-topic',
        'orders' => 'test-orders-topic',
    ]);
});

it('collects routes keyed by topic name', function () {
    config()->set('kafka-bus.consumers.routes', [
        'products' => ConsumerHandlerFaker::class,
        'orders' => ['handler' => VoidConsumerHandlerFaker::class, 'middleware' => []],
    ]);

    $routes = resolve(ReceiverRoutesFactory::class)->create();

    /** @var Route $route */
    $route = $routes->get('testing.test-orders-topic');

    expect($route->topic->key)->toBe('orders')
        ->and($route->handler)->toBeInstanceOf(VoidConsumerHandlerFaker::class)
        ->and(array_map(fn (Route $route) => $route->topic->key, $routes->all()))->toBe(['products', 'orders']);
});

it('puts global consumer middleware before route middleware', function () {
    config()->set('kafka-bus.consumers.middleware', [MessageMiddlewareFake::class]);
    config()->set('kafka-bus.consumers.routes', [
        'products' => [
            'handler' => ConsumerHandlerFaker::class,
            'middleware' => [OtherMessageMiddlewareFake::class],
        ],
    ]);

    $route = resolve(ReceiverRoutesFactory::class)->create()
        ->get('testing.test-products-topic');

    expect(array_map(get_class(...), $route->middleware))->toBe([
        MessageMiddlewareFake::class,
        OtherMessageMiddlewareFake::class,
    ]);
});

it('does not duplicate global middleware repeated in the route', function () {
    config()->set('kafka-bus.consumers.middleware', [MessageMiddlewareFake::class]);
    config()->set('kafka-bus.consumers.routes', [
        'products' => [
            'handler' => ConsumerHandlerFaker::class,
            'middleware' => [MessageMiddlewareFake::class],
        ],
    ]);

    $route = resolve(ReceiverRoutesFactory::class)->create()
        ->get('testing.test-products-topic');

    expect(array_map(get_class(...), $route->middleware))->toBe([MessageMiddlewareFake::class]);
});

it('does not resolve handlers until the route is requested', function () {
    config()->set('kafka-bus.consumers.routes', [
        'products' => 'App\\Handlers\\ThatDoesNotExist',
    ]);

    $routes = resolve(ReceiverRoutesFactory::class)->create();

    expect(fn () => $routes->get('testing.test-products-topic'))
        ->toThrow(BindingResolutionException::class);
});

it('fails when route topic is not registered in topics', function () {
    config()->set('kafka-bus.consumers.routes', [
        'unknown' => ConsumerHandlerFaker::class,
    ]);

    resolve(ReceiverRoutesFactory::class)->create();
})->throws(TopicCannotResolvedException::class);

it('fails when route is not registered for topic', function () {
    config()->set('kafka-bus.consumers.routes', []);

    resolve(\KafkaBus\Laravel\Receivers\ReceiverRouteFactory::class)
        ->create(resolve(\KafkaBus\Core\Topics\TopicRegistry::class)->get('products'));
})->throws(KafkaBusConfigurationException::class);
