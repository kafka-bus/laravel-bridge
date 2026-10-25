<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Factories;

use Illuminate\Config\Repository;
use KafkaBus\Core\BusInterface;
use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Core\Exceptions\TopicCannotResolvedException;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Metadata\Metadata;
use KafkaBus\Metadata\Partitions\Partitions;

/**
 * Partitions of consumed topics (`kafka-bus.consumers.routes`) in the consumer group
 * from the global `kafka-bus.consumers` options.
 *
 * @internal
 */
class PartitionsFactory
{
    public function __construct(
        private readonly BusInterface $bus,
        private readonly TopicRegistry $topicRegistry,
        private readonly Repository $config,
    ) {
    }

    /**
     * @param list<string>|null $topicKeys all consumed topics when null
     *
     * @throws TopicCannotResolvedException
     */
    public function create(?array $topicKeys = null): Partitions
    {
        $topicKeys ??= array_keys($this->config->get('kafka-bus.consumers.routes', []));

        $options = (new OptionsMerger($this->config->get('kafka-bus.consumers', [])))
            ->merge([]);

        return Metadata::fromConnection($this->bus->connection())
            ->partitions(
                array_map(fn (string $key): Topic => $this->topicRegistry->get($key), $topicKeys),
                new ConsumerConfig(
                    additionalOptions: $options['additional_options'],
                    autoCommit: $options['auto_commit'] ?? true,
                    consumerTimeout: $options['consumer_timeout'] ?? 5000,
                )
            );
    }
}
