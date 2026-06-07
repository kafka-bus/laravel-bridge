<?php

use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Laravel\Listeners\LaravelWorkerRegistry;

use function PHPUnit\Framework\assertEquals;
use function PHPUnit\Framework\assertInstanceOf;

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

                'topics' => [
                    'products' => ConsumerHandlerFaker::class,
                ],
            ],
        ]
    ]);

    /** @var \KafkaBus\Core\Bus\Listeners\Workers\Worker $worker */
    $worker = resolve(LaravelWorkerRegistry::class)
        ->get('default-worker');

    assertEquals($worker->options->additionalOptions, [
        'test.option' => 'foo',
        'not.override' => 'test-value',
        'new.option' => 'bar',
    ]);

    $route = $worker->routes->get('testing.test-products-topic');

    assertEquals('products', $route->topic->key);
    assertInstanceOf(ConsumerHandlerFaker::class, $route->handler);
});

it('create worker with short configuration', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.consumers', [
        'workers' => [
            'products' => ConsumerHandlerFaker::class,
        ]
    ]);

    /** @var \KafkaBus\Core\Bus\Listeners\Workers\Worker $worker */
    $worker = resolve(LaravelWorkerRegistry::class)
        ->get('products');

    $route = $worker->routes->get('testing.test-products-topic');

    assertEquals('products', $route->topic->key);
    assertInstanceOf(ConsumerHandlerFaker::class, $route->handler);
});

it('create worker with consume one topic', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.consumers', [
        'workers' => [
            'products' => ['handler' => ConsumerHandlerFaker::class],
        ]
    ]);

    /** @var \KafkaBus\Core\Bus\Listeners\Workers\Worker $worker */
    $worker = resolve(LaravelWorkerRegistry::class)
        ->get('products');

    $route = $worker->routes->get('testing.test-products-topic');

    assertEquals('products', $route->topic->key);
    assertInstanceOf(ConsumerHandlerFaker::class, $route->handler);
});

it('create worker with consume one topic with custom topic key', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.consumers', [
        'workers' => [
            'products_other' => [
                'topic_key' => 'products',
                'handler' => ConsumerHandlerFaker::class,
            ],
        ]
    ]);

    /** @var \KafkaBus\Core\Bus\Listeners\Workers\Worker $worker */
    $worker = resolve(LaravelWorkerRegistry::class)
        ->get('products_other');

    $route = $worker->routes->get('testing.test-products-topic');

    assertEquals('products', $route->topic->key);
    assertInstanceOf(ConsumerHandlerFaker::class, $route->handler);
});
