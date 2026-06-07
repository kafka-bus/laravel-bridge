<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Core\Testing\Messages\ConsumerMiddleware;
use KafkaBus\Laravel\Tests\Fixtures\MessageMiddlewareFake;

it('shows warning when no workers configured', function () {
    config()->set('kafka-bus.consumers.workers', []);

    $this->artisan('kafka:worker:list')
        ->expectsOutput('No workers registered')
        ->assertExitCode(0);
});

it('shows workers in table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => ConsumerHandlerFaker::class,
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            ['Worker', 'Topic key', 'Topic name', 'Handler', 'Consumer Middleware', 'Route Middleware'],
            [['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class, '', '']]
        )
        ->assertExitCode(0);
});

it('shows consumer middleware in worker table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers', [
        'workers' => [
            'products' => [
                'middleware' => [ConsumerMiddleware::class],
                'handler' => ConsumerHandlerFaker::class,
            ],
        ],
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            ['Worker', 'Topic key', 'Topic name', 'Handler', 'Consumer Middleware', 'Route Middleware'],
            [['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class, ConsumerMiddleware::class, '']]
        )
        ->assertExitCode(0);
});

it('shows route middleware in worker table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => [
            'topics' => [
                'products' => [
                    'handler' => ConsumerHandlerFaker::class,
                    'middleware' => [MessageMiddlewareFake::class],
                ],
            ],
        ],
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            ['Worker', 'Topic key', 'Topic name', 'Handler', 'Consumer Middleware', 'Route Middleware'],
            [['products', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class, '', MessageMiddlewareFake::class]]
        )
        ->assertExitCode(0);
});

it('shows multiple topics per worker', function () {
    config()->set('kafka-bus.topics', [
        'products' => 'test-products-topic',
        'orders' => 'test-orders-topic',
    ]);
    config()->set('kafka-bus.consumers.workers', [
        'default' => [
            'topics' => [
                'products' => ConsumerHandlerFaker::class,
                'orders' => ConsumerHandlerFaker::class,
            ],
        ],
    ]);

    $this->artisan('kafka:worker:list')
        ->expectsTable(
            ['Worker', 'Topic key', 'Topic name', 'Handler', 'Consumer Middleware', 'Route Middleware'],
            [
                ['default', 'products', 'testing.test-products-topic', ConsumerHandlerFaker::class, '', ''],
                ['default', 'orders', 'testing.test-orders-topic', ConsumerHandlerFaker::class, '', ''],
            ]
        )
        ->assertExitCode(0);
});
