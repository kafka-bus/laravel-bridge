<?php

use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutes;
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Laravel\Publishers\LaravelPublisherRoutesFactory;

it('create publisher routes', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.producers', [
        'additional_options' => [
            'test.option' => 'bar',
            'not.override' => 'test-value',
        ],

        'flush_timeout' => 10_000,
        'flush_retries' => 10,

        'routes' => [
            ProducerMessageFaker::class => [
                'topic_key' => 'products',

                'additional_options' => [
                    'test.option' => 'foo',
                    'new.option' => 'bar',
                ],

                'flush_timeout' => 15_000,
                'flush_retries' => 15,
            ],
        ]
    ]);

    /** @var PublisherRoutes $routes */
    $routes = resolve(LaravelPublisherRoutesFactory::class)
        ->create();

    $route = $routes->get(ProducerMessageFaker::class);

    expect($route->topic->key)->toBe('products')
        ->and($route->options->additionalOptions)->toEqual([
            'test.option' => 'foo',
            'not.override' => 'test-value',
            'new.option' => 'bar',
        ])
        ->and($route->options->flushTimeout)->toEqual(15_000)
        ->and($route->options->flushRetries)->toEqual(15);
});

it('create publisher routes by short config', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);

    config()->set('kafka-bus.producers', [
        'routes' => [
            ProducerMessageFaker::class => 'products',
        ]
    ]);

    /** @var PublisherRoutes $routes */
    $routes = resolve(LaravelPublisherRoutesFactory::class)
        ->create();

    $route = $routes->get(ProducerMessageFaker::class);

    expect($route->topic->key)->toBe('products');
});
