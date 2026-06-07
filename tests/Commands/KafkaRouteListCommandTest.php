<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Laravel\Tests\Fixtures\ProducerMiddlewareFake;

it('shows warning when no routes configured', function () {
    config()->set('kafka-bus.producers.routes', []);

    $this->artisan('kafka:route:list')
        ->expectsOutput('No routes registered')
        ->assertExitCode(0);
});

it('shows routes in table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.producers.routes', [
        ProducerMessageFaker::class => 'products',
    ]);

    $this->artisan('kafka:route:list')
        ->expectsTable(
            ['Message', 'Topic key', 'Topic name', 'Middleware'],
            [[ProducerMessageFaker::class, 'products', 'testing.test-products-topic', '']]
        )
        ->assertExitCode(0);
});

it('shows middleware class names in route table', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.producers.routes', [
        ProducerMessageFaker::class => [
            'topic_key' => 'products',
            'middleware' => [ProducerMiddlewareFake::class],
        ],
    ]);

    $this->artisan('kafka:route:list')
        ->expectsTable(
            ['Message', 'Topic key', 'Topic name', 'Middleware'],
            [[ProducerMessageFaker::class, 'products', 'testing.test-products-topic', ProducerMiddlewareFake::class]]
        )
        ->assertExitCode(0);
});
