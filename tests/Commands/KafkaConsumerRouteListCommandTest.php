<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Laravel\Tests\Fixtures\MessageMiddlewareFake;
use KafkaBus\Laravel\Tests\Fixtures\OtherMessageMiddlewareFake;

const CONSUME_ROUTE_LIST_HEADERS = ['Topic key', 'Topic name', 'Handler', 'Middleware'];

it('shows warning when no consumer routes configured', function () {
    config()->set('kafka-bus.consumers.routes', []);

    $this->artisan('kafka:consumer:route:list')
        ->expectsOutput('No consumer routes registered')
        ->assertExitCode(0);
});

it('shows consumer routes in table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.routes', ['products' => ConsumerHandlerFaker::class]);

    $this->artisan('kafka:consumer:route:list')
        ->expectsTable(
            CONSUME_ROUTE_LIST_HEADERS,
            [['products', 'testing.test-products-topic', ConsumerHandlerFaker::class, '']]
        )
        ->assertExitCode(0);
});

it('shows global middleware before route middleware', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.middleware', [MessageMiddlewareFake::class]);
    config()->set('kafka-bus.consumers.routes', [
        'products' => ['handler' => ConsumerHandlerFaker::class, 'middleware' => [OtherMessageMiddlewareFake::class]],
    ]);

    $this->artisan('kafka:consumer:route:list')
        ->expectsTable(
            CONSUME_ROUTE_LIST_HEADERS,
            [['products', 'testing.test-products-topic', ConsumerHandlerFaker::class, MessageMiddlewareFake::class."\n".OtherMessageMiddlewareFake::class]]
        )
        ->assertExitCode(0);
});
