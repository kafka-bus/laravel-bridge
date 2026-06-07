<?php

use KafkaBus\Core\Connections\KafkaConnection;
use KafkaBus\Core\Interfaces\Connections\ConnectionRegistryInterface;
use KafkaBus\Core\Topics\TopicRegistry;

it('resolve topic name', function () {
    config()->set('kafka-bus.topic_prefix', 'production');
    config()->set('kafka-bus.topics', ['products' => '.fact.products.1']);

    $topicRegistry = resolve(TopicRegistry::class);

    expect($topicRegistry->getTopicName('products'))
        ->toEqual('production.fact.products.1');
});

it('can create connection', function () {
    $connection = resolve(ConnectionRegistryInterface::class)
        ->connection('kafka');

    expect($connection)
        ->toBeInstanceOf(KafkaConnection::class);
});
