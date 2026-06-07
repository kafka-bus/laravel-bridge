<?php

use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Laravel\Facades\KafkaBus;

it('can produce message to kafka', function () {
    config()->set('kafka-bus.topics', ['products' => 'production.fact.products.1']);
    config()->set('kafka-bus.producers.routes', [ProducerMessageFaker::class => 'products']);

    KafkaBus::fake();

    resolve(BusInterface::class)
        ->publish(new ProducerMessageFaker('test-message'));

    KafkaBus::assertPublished(ProducerMessageFaker::class);
});
