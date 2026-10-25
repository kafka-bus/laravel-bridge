<?php

declare(strict_types=1);

use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadataInterface;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;
use KafkaBus\Metadata\Partitions\Partitions;
use RdKafka\Exception as RdKafkaException;

function bindOffsetShowPartitions(array $topics, ConsumerGroupMetadataInterface $group): void
{
    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldReceive('create')->andReturn(new Partitions($topics, $group));

    app()->instance(PartitionsFactory::class, $factory);
}

it('shows offsets of every partition', function () {
    $group = Mockery::mock(ConsumerGroupMetadataInterface::class);
    $group->shouldReceive('partitions')->with('test-products-topic')->andReturn([
        new ConsumerPartition(id: 0, topicName: 'test-products-topic', currentOffset: 5, minOffset: 1, maxOffset: 10),
        new ConsumerPartition(id: 1, topicName: 'test-products-topic', currentOffset: 7, minOffset: 2, maxOffset: 20),
    ]);
    bindOffsetShowPartitions([new Topic('test-products-topic', 'products')], $group);

    $this->artisan('kafka:consumer:offset:show', ['topicKey' => 'products'])
        ->expectsTable(
            ['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max'],
            [
                ['products', 'test-products-topic', '0', '5', '1', '10'],
                ['products', 'test-products-topic', '1', '7', '2', '20'],
            ]
        )
        ->assertExitCode(0);
});

it('shows n/a when the topic does not exist on the broker', function () {
    $group = Mockery::mock(ConsumerGroupMetadataInterface::class);
    $group->shouldReceive('partitions')->andThrow(new TopicNotFoundException('not found'));
    bindOffsetShowPartitions([new Topic('test-products-topic', 'products')], $group);

    $this->artisan('kafka:consumer:offset:show')
        ->expectsTable(
            ['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max'],
            [['products', 'test-products-topic', 'n/a', '-1', '-1', '-1']]
        )
        ->assertExitCode(0);
});

it('warns when there are no partitions to display', function () {
    bindOffsetShowPartitions([], Mockery::mock(ConsumerGroupMetadataInterface::class));

    $this->artisan('kafka:consumer:offset:show')
        ->expectsOutput('No partitions to display')
        ->assertExitCode(0);
});

it('fails when the topic key cannot be resolved', function () {
    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldReceive('create')->andThrow(new TopicCannotResolvedException('Unknown topic'));
    $this->app->instance(PartitionsFactory::class, $factory);

    $this->artisan('kafka:consumer:offset:show', ['topicKey' => 'unknown'])
        ->expectsOutput('Unknown topic')
        ->assertExitCode(1);
});

it('fails on a kafka error', function () {
    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldReceive('create')->andThrow(new RdKafkaException('Broker down'));
    $this->app->instance(PartitionsFactory::class, $factory);

    $this->artisan('kafka:consumer:offset:show')
        ->expectsOutput('Broker down')
        ->assertExitCode(1);
});
