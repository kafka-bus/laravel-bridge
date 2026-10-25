<?php

use KafkaBus\Laravel\Workers\WorkerRegistry;

use function PHPUnit\Framework\assertEquals;

it('create worker', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.consumers', [
        'additional_options' => [
            'test.option' => 'bar',
            'not.override' => 'test-value',
        ],

        'workers' => [
            'default-worker' => [
                'additional_options' => [
                    'test.option' => 'foo',
                    'new.option' => 'bar',
                ],

                'topics' => ['products'],
            ],
        ]
    ]);

    $worker = resolve(WorkerRegistry::class)
        ->get('default-worker');

    assertEquals($worker->options->additionalOptions, [
        'test.option' => 'foo',
        'not.override' => 'test-value',
        'new.option' => 'bar',
    ]);

    assertEquals('default-worker', $worker->name);
    assertEquals(['products'], array_column($worker->topics, 'key'));
    assertEquals(['testing.test-products-topic'], array_column($worker->topics, 'name'));
});

it('create worker for the topic with the same name', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.consumers', [
        'workers' => [
            'products' => [],
        ]
    ]);

    $worker = resolve(WorkerRegistry::class)
        ->get('products');

    assertEquals(['products'], array_column($worker->topics, 'key'));
});

it('caches created workers', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', ['products' => []]);

    $registry = resolve(WorkerRegistry::class);

    expect($registry->get('products'))->toBe($registry->get('products'));
});

it('returns null for unknown worker', function () {
    config()->set('kafka-bus.consumers.workers', []);

    expect(resolve(WorkerRegistry::class)->get('unknown'))
        ->toBeNull();
});
