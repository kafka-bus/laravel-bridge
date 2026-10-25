<?php

declare(strict_types=1);


use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;
use KafkaBus\Laravel\Workers\WorkerFactory;

it('returns empty array when no workers configured', function () {
    config()->set('kafka-bus.consumers.workers', []);

    $names = resolve(WorkerFactory::class)->names();

    expect($names)->toBe([]);
});

it('returns all registered worker names', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => [],
        'orders' => [],
        'payments' => [],
    ]);

    $names = resolve(WorkerFactory::class)->names();

    expect($names)->toBe(['products', 'orders', 'payments']);
});

it('creates a worker automatically for a topic key with a consumer route', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', []);
    config()->set('kafka-bus.consumers.routes', ['products' => ConsumerHandlerFaker::class]);
    config()->set('kafka-bus.consumers.additional_options', ['group.id' => 'group']);

    $factory = resolve(WorkerFactory::class);
    $worker = $factory->create('products');

    expect($factory->has('products'))->toBeTrue()
        ->and($factory->names())->toBe(['products'])
        ->and(array_column($worker->topics, 'name'))->toBe(['testing.test-products-topic'])
        ->and($worker->options->additionalOptions)->toBe(['group.id' => 'group']);
});

it('consumes the topic with the worker name when worker has no topics', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => ['auto_commit' => false, 'consumer_timeout' => 1000],
    ]);

    $worker = resolve(WorkerFactory::class)->create('products');

    expect(array_column($worker->topics, 'key'))->toBe(['products'])
        ->and($worker->options->autoCommit)->toBeFalse()
        ->and($worker->options->consumerTimeout)->toBe(1000);
});

it('creates a worker with several topics', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic', 'orders' => 'test-orders-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'default' => ['topics' => ['products', 'orders']],
    ]);

    $worker = resolve(WorkerFactory::class)->create('default');

    expect(array_column($worker->topics, 'key'))->toBe(['products', 'orders']);
});

it('prefers a configured worker over the automatic one', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic', 'orders' => 'test-orders-topic']);
    config()->set('kafka-bus.consumers.routes', [
        'products' => ConsumerHandlerFaker::class,
        'orders' => ConsumerHandlerFaker::class,
    ]);
    config()->set('kafka-bus.consumers.workers', [
        'products' => ['topics' => ['products', 'orders']],
    ]);

    $worker = resolve(WorkerFactory::class)->create('products');

    expect(array_column($worker->topics, 'key'))->toBe(['products', 'orders']);
});

it('does not create a worker without configuration and route', function () {
    config()->set('kafka-bus.consumers.workers', []);
    config()->set('kafka-bus.consumers.routes', []);

    $factory = resolve(WorkerFactory::class);

    expect($factory->has('unknown'))->toBeFalse();
    $factory->create('unknown');
})->throws(KafkaBusConfigurationException::class);
