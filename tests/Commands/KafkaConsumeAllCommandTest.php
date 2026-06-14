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
    config()->set('kafka-bus.consumers.workers', [
        'products' => VoidConsumerHandlerFaker::class,
        'orders'   => VoidConsumerHandlerFaker::class,
    ]);
});

it('exits with success consuming all configured workers', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:all')
        ->assertExitCode(0);
});

it('outputs start consuming message with all worker names', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:all')
        ->expectsOutput('Start consuming for "products, orders"')
        ->assertExitCode(0);
});

it('processes and commits messages from all workers', function () {
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

    $this->artisan('kafka:consume:all')
        ->assertExitCode(0);

    KafkaBus::assertCommitted('products');
    KafkaBus::assertCommitted('orders');
});

it('excludes workers specified via --expect option', function () {
    KafkaBus::fake();

    $this->artisan('kafka:consume:all', ['--expect' => ['orders']])
        ->expectsOutput('Start consuming for "products"')
        ->assertExitCode(0);
});

it('excludes multiple workers via --expect option', function () {
    config()->set('kafka-bus.consumers.workers', [
        'products'      => VoidConsumerHandlerFaker::class,
        'orders'        => VoidConsumerHandlerFaker::class,
        'notifications' => VoidConsumerHandlerFaker::class,
    ]);

    KafkaBus::fake();

    $this->artisan('kafka:consume:all', ['--expect' => ['orders', 'notifications']])
        ->expectsOutput('Start consuming for "products"')
        ->assertExitCode(0);
});
