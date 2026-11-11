<?php

declare(strict_types=1);

use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Factories\PartitionsFactory;
use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadataInterface;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;
use KafkaBus\Metadata\Partitions\Partitions;

it('continues past unavailable topics and includes topics without consumer routes', function () {
    $registry = new TopicRegistry();
    $missing = new Topic('missing-topic', 'missing');
    $available = new Topic('publisher-topic', 'publisher');

    $registry
        ->add($missing)
        ->add($available);

    app()->instance(TopicRegistry::class, $registry);

    $group = Mockery::mock(ConsumerGroupMetadataInterface::class);

    $group->shouldReceive('partitions')
        ->with('missing-topic')
        ->andThrow(new TopicNotFoundException('Missing'));

    $group->shouldReceive('partitions')
        ->with('publisher-topic')
        ->andReturn([new ConsumerPartition(id: 0, topicName: 'publisher-topic', currentOffset: -1001, minOffset: 0, maxOffset: 0)]);

    $factory = Mockery::mock(PartitionsFactory::class);

    $factory->shouldReceive('create')
        ->with(['missing'])
        ->andReturn(new Partitions([$missing], $group));

    $factory->shouldReceive('create')
        ->with(['publisher'])
        ->andReturn(new Partitions([$available], $group));

    app()->instance(PartitionsFactory::class, $factory);

    $this->artisan('kafka:topic:list')->expectsTable(
        ['Topic key', 'Topic name', 'Partition', 'Current', 'Min', 'Max', 'Status'],
        [
            ['missing', 'missing-topic', 'n/a', 'n/a', 'n/a', 'n/a', 'Unavailable: topic missing or inaccessible'],
            ['publisher', 'publisher-topic', '0', 'n/a', '0', '0', 'OK'],
        ],
    )->assertExitCode(1);
});

it('handles an empty topic registry', function () {
    app()->instance(TopicRegistry::class, new TopicRegistry());

    $factory = Mockery::mock(PartitionsFactory::class);
    $factory->shouldNotReceive('create');

    app()->instance(PartitionsFactory::class, $factory);

    $this->artisan('kafka:topic:list')
        ->expectsOutput('No topics configured')
        ->assertExitCode(0);
});
