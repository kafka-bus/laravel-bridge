<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Testing;

use BadMethodCallException;
use KafkaBus\Core\BusInterface;
use KafkaBus\Core\Connections\ConnectionInterface;
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Publishers\MessageBatch;
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Receivers\ReceiverInterface;
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Worker\WorkerRunnerFactory;

/**
 * Wraps {@see BusFaker}: every assertion and helper of the faker (assertPublished, assertCommitted,
 * assertDispatched, addMessage, getPublished, ...) is available on the fake as is.
 *
 * @mixin BusFaker
 */
final readonly class FakeBus implements BusInterface
{
    private function __construct(
        private BusFaker $faker,
    ) {
    }

    public static function make(): self
    {
        $topicRegistry = app(TopicRegistry::class);

        $faker = BusFaker::make(
            $topicRegistry,
            new PHPUnitAssertionDriver(),
            app(PublisherFactory::class),
            app(ReceiverInterface::class),
        );

        $fake = new self($faker);

        app()->instance(ConnectionInterface::class, $faker->connection());
        app()->instance(BusInterface::class, $fake);

        return $fake;
    }

    public function connection(): ConnectionInterface
    {
        return $this->faker->connection();
    }

    public function publish(ProducerMessageInterface $message): void
    {
        $this->faker->publish($message);
    }

    public function publishBatch(MessageBatch $messageBatch): void
    {
        $this->faker->publishBatch($messageBatch);
    }

    public function dispatch(ConsumerMessageInterface $message): void
    {
        $this->faker->dispatch($message);
    }

    /**
     * Run the full consumer pipeline for the worker(s) over the messages queued via addMessage():
     * connection -> consumer stream -> receiver (middleware -> handler) -> commit.
     *
     * @param string|non-empty-list<string> $workerName
     */
    public function listen(string|array $workerName): void
    {
        app(WorkerRunnerFactory::class)
            ->create($this, $workerName)
            ->run();
    }

    /**
     * @param array<array-key, mixed> $arguments
     */
    public function __call(string $method, array $arguments): mixed
    {
        if (! method_exists($this->faker, $method)) {
            throw new BadMethodCallException(sprintf('Method %s::%s does not exist.', self::class, $method));
        }

        return $this->faker->{$method}(...$arguments);
    }
}
