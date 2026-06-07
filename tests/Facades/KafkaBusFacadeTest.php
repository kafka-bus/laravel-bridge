<?php

declare(strict_types=1);

use KafkaBus\Core\Bus;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Interfaces\Connections\ConnectionRegistryInterface;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Laravel\Facades\KafkaBus;
use KafkaBus\Laravel\Testing\FakeBus;
use PHPUnit\Framework\AssertionFailedError;

it('fake() replaces BusInterface binding with FakeBus', function () {
    KafkaBus::fake();

    expect(app(BusInterface::class))
        ->toBeInstanceOf(FakeBus::class);
});

it('fake() replaces ConnectionRegistryInterface with ConnectionRegistryFaker', function () {
    KafkaBus::fake();

    expect(app(ConnectionRegistryInterface::class))
        ->toBeInstanceOf(ConnectionRegistryFaker::class);
});

it('facade resolves real Bus before fake() is called', function () {
    expect(app(BusInterface::class))
        ->toBeInstanceOf(Bus::class);
});

it('publish() goes through FakeBus and is trackable', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.producers.routes', [ProducerMessageFaker::class => 'products']);

    KafkaBus::fake();
    KafkaBus::publish(new ProducerMessageFaker('hello'));
    KafkaBus::assertPublished(ProducerMessageFaker::class);
});

it('assertNotPublished passes when nothing published', function () {
    KafkaBus::fake();
    KafkaBus::assertNotPublished(ProducerMessageFaker::class);
});

it('assertNothingPublished passes when nothing published', function () {
    KafkaBus::fake();
    KafkaBus::assertNothingPublished();
});

it('assertPublishedTimes counts correctly', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.producers.routes', [ProducerMessageFaker::class => 'products']);

    KafkaBus::fake();

    KafkaBus::publish(new ProducerMessageFaker('a'));
    KafkaBus::publish(new ProducerMessageFaker('b'));

    KafkaBus::assertPublishedTimes(ProducerMessageFaker::class, 2);
});

it('assertNothingPublished fails when message was published', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.producers.routes', [ProducerMessageFaker::class => 'products']);

    KafkaBus::fake();
    KafkaBus::publish(new ProducerMessageFaker('hello'));

    expect(fn () => KafkaBus::assertNothingPublished())
        ->toThrow(AssertionFailedError::class);
});

it('listen() processes queued messages via FakeBus', function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.workers', [
        'products' => ConsumerHandlerFaker::class,
    ]);

    KafkaBus::fake();

    $factory = MessageFactory::for()
        ->withTopicKey('products');

    KafkaBus::addMessage($factory->make('hello'));

    ob_start();
    KafkaBus::listen('products');
    ob_end_clean();

    KafkaBus::assertCommitted('products');
});
