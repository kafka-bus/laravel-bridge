<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Testing\Messages\VoidConsumerHandlerFaker;
use KafkaBus\Laravel\Facades\KafkaBus;

beforeEach(function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => VoidConsumerHandlerFaker::class,
    ]);
});

it('exits with success when there are no messages', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume', ['workerName' => ['products']])
        ->assertExitCode(0);
});

it('outputs start consuming message', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume', ['workerName' => ['products']])
        ->expectsOutput('Start consuming for "products"')
        ->assertExitCode(0);
});

it('outputs consumer finished message on success', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume', ['workerName' => ['products']])
        ->expectsOutput('Consumer finished')
        ->assertExitCode(0);
});

it('processes and commits messages from the queue', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('hello')
    );

    $this->artisan('kafka:consume products')
        ->assertExitCode(0);

    KafkaBus::assertCommitted('products');
});

it('accepts multiple worker names and outputs them comma-separated', function () {
    config()->set('kafka-bus.topics', [
        'products' => 'test-products-topic',
        'orders'   => 'test-orders-topic',
    ]);
    config()->set('kafka-bus.consumers.workers', [
        'products' => VoidConsumerHandlerFaker::class,
        'orders'   => VoidConsumerHandlerFaker::class,
    ]);

    KafkaBus::fake();

    $this->artisan('kafka:consume', ['workerName' => ['products', 'orders']])
        ->expectsOutput('Start consuming for "products, orders"')
        ->assertExitCode(0);
});
