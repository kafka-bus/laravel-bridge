<?php

declare(strict_types=1);

use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Testing\Messages\ConsumerHandlerFaker;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use KafkaBus\Metadata\Partitions\Partitions;

beforeEach(function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
    config()->set('kafka-bus.consumers.routes', ['products' => ConsumerHandlerFaker::class]);
});

it('creates partitions for consumed topics', function () {
    expect(resolve(PartitionsFactory::class)->create())->toBeInstanceOf(Partitions::class);
});

it('creates partitions for the given topic keys', function () {
    expect(resolve(PartitionsFactory::class)->create(['products']))->toBeInstanceOf(Partitions::class);
});

it('fails for unknown topic key', function () {
    resolve(PartitionsFactory::class)->create(['unknown']);
})->throws(TopicCannotResolvedException::class);
