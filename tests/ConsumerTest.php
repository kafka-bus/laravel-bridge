<?php

use KafkaBus\Core\Consumers\Messages\ConsumerMessage;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Testing\Messages\VoidConsumerHandlerFaker;
use KafkaBus\Laravel\Facades\KafkaBus;

it('can consume message from kafka', function () {
    config()->set('kafka-bus.topics', ['products' => 'production.fact.products.1']);
    config()->set('kafka-bus.consumers.routes', ['products' => VoidConsumerHandlerFaker::class]);

    KafkaBus::fake();

    $message = MessageFactory::for()
        ->withTopicKey('products')
        ->make('test-payload');

    KafkaBus::addMessage($message);
    KafkaBus::addMessage($message);

    KafkaBus::listen('products');

    KafkaBus::assertCommitted('products');
    KafkaBus::assertCommittedTimes('products', 2);
});

it('can dispatch message from kafka without fake worker', function () {
    config()->set('kafka-bus.topics', ['products' => 'production.fact.products.1']);
    config()->set('kafka-bus.consumers.routes', ['products' => VoidConsumerHandlerFaker::class]);

    KafkaBus::fake();

    $message = MessageFactory::for()
        ->withTopicKey('products')
        ->make('test-payload');

    KafkaBus::dispatch(new ConsumerMessage($message));
    KafkaBus::dispatch(new ConsumerMessage($message));

    KafkaBus::assertDispatched('products');
    KafkaBus::assertDispatchedTimes('products', 2);
});
