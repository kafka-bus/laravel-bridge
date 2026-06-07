<?php

use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Factories\TopicRegistryFactory;

it('can create a topic registry', function () {
    config()->set('kafka-bus.topic_prefix', 'production.');

    config()->set('kafka-bus.topics', [
        'products' => 'fact.products.1',
    ]);

    /** @var TopicRegistry $topicRegistry */
    $topicRegistry = resolve(TopicRegistryFactory::class)
        ->create();

    $topic = $topicRegistry->get('products');

    expect($topic)->toBeInstanceOf(Topic::class)
        ->and($topic->name)->toBe('production.fact.products.1')
        ->and($topic->key)->toBe('products');
});
