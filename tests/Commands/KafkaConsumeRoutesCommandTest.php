<?php

declare(strict_types=1);

use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Testing\Messages\VoidConsumerHandlerFaker;
use KafkaBus\Laravel\Facades\KafkaBus;

beforeEach(function () {
    config()->set('kafka-bus.topics', [
        'products' => 'test-products-topic',
        'orders'   => 'test-orders-topic',
    ]);
    config()->set('kafka-bus.consumers.routes', [
        'products' => VoidConsumerHandlerFaker::class,
        'orders'   => VoidConsumerHandlerFaker::class,
    ]);
});

it('exits with success consuming all routes', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:routes')
        ->assertExitCode(0);
});

it('outputs start consuming message with all route names', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:routes')
        ->expectsOutput('Start consuming for "products, orders"')
        ->assertExitCode(0);
});

it('processes and commits messages from all routes', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('product payload')
    );
    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('orders')
            ->make('order payload')
    );

    $this->artisan('kafka:consume:routes')
        ->assertExitCode(0);

    KafkaBus::assertCommitted('products');
    KafkaBus::assertCommitted('orders');
});

it('excludes routes specified via --expect option', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:routes', ['--expect' => ['orders']])
        ->expectsOutput('Start consuming for "products"')
        ->assertExitCode(0);
});

it('excludes multiple routes via --expect option', function () {
    config()->set('kafka-bus.topics', [
        'products'      => 'test-products-topic',
        'orders'        => 'test-orders-topic',
        'notifications' => 'test-notifications-topic',
    ]);
    config()->set('kafka-bus.consumers.routes', [
        'products'      => VoidConsumerHandlerFaker::class,
        'orders'        => VoidConsumerHandlerFaker::class,
        'notifications' => VoidConsumerHandlerFaker::class,
    ]);

    KafkaBus::fake();

    $this->artisan('kafka:consume:routes', ['--expect' => ['orders', 'notifications']])
        ->expectsOutput('Start consuming for "products"')
        ->assertExitCode(0);
});

it('fails when there are no routes to consume', function () {
    config()->set('kafka-bus.consumers.routes', []);

    KafkaBus::fake();

    $this->artisan('kafka:consume:routes')
        ->expectsOutput('No workers for consuming.')
        ->assertExitCode(1);
});

it('fails when all routes are excluded', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:routes', ['--expect' => ['products', 'orders']])
        ->expectsOutput('No workers for consuming.')
        ->assertExitCode(1);
});

it('ignores configured workers and consumes only routes', function () {
    config()->set('kafka-bus.consumers.workers', [
        'products' => ['topics' => ['products', 'orders']],
    ]);

    KafkaBus::fake();

    $this->artisan('kafka:consume:routes')
        ->expectsOutput('Start consuming for "products, orders"')
        ->assertExitCode(0);
});
