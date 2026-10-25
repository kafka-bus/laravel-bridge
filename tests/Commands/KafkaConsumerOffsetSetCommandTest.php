<?php

declare(strict_types=1);

use KafkaBus\Core\Topics\Topic;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadataInterface;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;
use KafkaBus\Metadata\Partitions\Partitions;
use RdKafka\Exception as RdKafkaException;

beforeEach(function () {
    config()->set('kafka-bus.topics', ['products' => 'test-products-topic']);
});

function bindOffsetSetPartitions(ConsumerGroupMetadataInterface $group): void
{
    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldReceive('create')
        ->with(['products'])
        ->andReturn(new Partitions([new Topic('testing.test-products-topic', 'products')], $group));

    app()->instance(PartitionsFactory::class, $factory);
}

function offsetSetGroup(): ConsumerGroupMetadataInterface
{
    $group = Mockery::mock(ConsumerGroupMetadataInterface::class);
    $group->shouldReceive('partitions')->with('testing.test-products-topic')->andReturn([
        new ConsumerPartition(id: 0, topicName: 'testing.test-products-topic', currentOffset: 5, minOffset: 1, maxOffset: 10),
        new ConsumerPartition(id: 1, topicName: 'testing.test-products-topic', currentOffset: 7, minOffset: 2, maxOffset: 20),
    ]);

    return $group;
}

it('sets a numeric offset for all partitions', function () {
    $group = offsetSetGroup();
    $group->shouldReceive('commit')->once()->with(Mockery::on(
        fn (array $offsets) => count($offsets) === 2 && $offsets[0] instanceof PartitionOffset
    ));
    bindOffsetSetPartitions($group);

    $this->artisan('kafka:consumer:offset:set', ['topicKey' => 'products', 'offset' => '5'])
        ->expectsTable(
            ['Topic key', 'Topic name', 'Partition', 'Old', 'New'],
            [
                ['products', 'testing.test-products-topic', '0', '5', '5'],
                ['products', 'testing.test-products-topic', '1', '7', '5'],
            ]
        )
        ->assertExitCode(0);
});

it('sets the earliest offset for a single partition', function () {
    $group = offsetSetGroup();
    $group->shouldReceive('commit')->once();
    bindOffsetSetPartitions($group);

    $this->artisan('kafka:consumer:offset:set', [
        'topicKey'    => 'products',
        'offset'      => 'earliest',
        '--partition' => 1,
    ])
        ->expectsTable(
            ['Topic key', 'Topic name', 'Partition', 'Old', 'New'],
            [['products', 'testing.test-products-topic', '1', '7', '2']]
        )
        ->assertExitCode(0);
});

it('sets the latest offset', function () {
    $group = offsetSetGroup();
    $group->shouldReceive('commit')->once();
    bindOffsetSetPartitions($group);

    $this->artisan('kafka:consumer:offset:set', [
        'topicKey'    => 'products',
        'offset'      => 'latest',
        '--partition' => 0,
    ])
        ->expectsTable(
            ['Topic key', 'Topic name', 'Partition', 'Old', 'New'],
            [['products', 'testing.test-products-topic', '0', '5', '10']]
        )
        ->assertExitCode(0);
});

it('fails on an invalid offset', function () {
    $this->artisan('kafka:consumer:offset:set', ['topicKey' => 'products', 'offset' => 'abc'])
        ->expectsOutput("Invalid offset 'abc'. Use 'earliest', 'latest', or a non-negative integer.")
        ->assertExitCode(1);
});

it('fails when the topic key is unknown', function () {
    $this->artisan('kafka:consumer:offset:set', ['topicKey' => 'unknown', 'offset' => '1'])
        ->assertExitCode(1);
});

it('fails when the offset is out of bounds', function () {
    bindOffsetSetPartitions(offsetSetGroup());

    $this->artisan('kafka:consumer:offset:set', ['topicKey' => 'products', 'offset' => '100'])
        ->expectsOutputToContain('out of bounds')
        ->assertExitCode(1);
});

it('fails when the partition does not exist', function () {
    bindOffsetSetPartitions(offsetSetGroup());

    $this->artisan('kafka:consumer:offset:set', [
        'topicKey'    => 'products',
        'offset'      => '5',
        '--partition' => 9,
    ])
        ->expectsOutputToContain('Partition [9] not found')
        ->assertExitCode(1);
});

it('fails on a kafka error', function () {
    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldReceive('create')->andThrow(new RdKafkaException('Broker down'));
    $this->app->instance(PartitionsFactory::class, $factory);

    $this->artisan('kafka:consumer:offset:set', ['topicKey' => 'products', 'offset' => '5'])
        ->expectsOutput('Broker down')
        ->assertExitCode(1);
});
