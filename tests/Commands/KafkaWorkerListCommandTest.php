<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Laravel\Tests\Fixtures\MessageMiddlewareFake;

const WORKER_LIST_HEADERS = ['Worker', 'Topic key', 'Topic name', 'Handler'];

it('shows warning when no workers configured', function () {
    config()->set('kafka-bus.consumers.workers', []);

    $this->artisan('kafka:worker:list')
        ->expectsOutput('No workers registered')
        ->assertExitCode(0);
});

it('shows worker with the topic of the same name', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.routes', ['products' => ConsumerHandlerFaker::class]);
    config()->set('kafka-bus.consumers.workers', ['products' => []]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            WORKER_LIST_HEADERS,
            [['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class]]
        )
        ->assertExitCode(0);
});

it('shows handler of route defined as array', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.routes', [
        'products' => ['handler' => ConsumerHandlerFaker::class, 'middleware' => [MessageMiddlewareFake::class]],
    ]);
    config()->set('kafka-bus.consumers.workers', ['products' => []]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            WORKER_LIST_HEADERS,
            [['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class]]
        )
        ->assertExitCode(0);
});

it('shows multiple topics per worker', function () {
    config()->set('kafka-bus.topics', [
        'products' => 'test-products-topic',
        'orders' => 'test-orders-topic',
    ]);
    config()->set('kafka-bus.consumers.routes', [
        'products' => ConsumerHandlerFaker::class,
        'orders' => ConsumerHandlerFaker::class,
    ]);
    config()->set('kafka-bus.consumers.workers', [
        'default' => ['topics' => ['products', 'orders']],
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            WORKER_LIST_HEADERS,
            [
                ['default', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class],
                ['default', 'orders', 'testing.test-orders-topic', ConsumerHandlerFaker::class],
                ['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class],
                ['orders', 'orders', 'testing.test-orders-topic', ConsumerHandlerFaker::class],
            ]
        )
        ->assertExitCode(0);
});

it('shows automatic workers created from consumer routes', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic', 'orders' => 'test-orders-topic']);
    config()->set('kafka-bus.consumers.routes', [
        'products' => ConsumerHandlerFaker::class,
        'orders' => ConsumerHandlerFaker::class,
    ]);
    config()->set('kafka-bus.consumers.workers', [
        'orders' => [],
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            WORKER_LIST_HEADERS,
            [
                ['orders', 'orders', 'testing.test-orders-topic', ConsumerHandlerFaker::class],
                ['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class],
            ]
        )
        ->assertExitCode(0);
});
