<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Testing;

use KafkaBus\Core\Bus\Listeners\Listener;
use KafkaBus\Core\Bus\MessageBatch;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Interfaces\Bus\ThreadInterface;
use KafkaBus\Core\Interfaces\Connections\ConnectionRegistryInterface;
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Producers\Messages\ProducerMessage;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Topics\TopicRegistry;
use PHPUnit\Framework\Assert;
use RdKafka\Message;

final readonly class FakeBus implements BusInterface
{
    private function __construct(
        private BusInterface    $bus,
        private ConnectionFaker $connectionFaker,
        private TopicRegistry   $topicRegistry,
    ) {
    }

    public static function make(): self
    {
        $topicRegistry = app(TopicRegistry::class);
        $connectionFaker = new ConnectionFaker($topicRegistry);

        app()->instance(ConnectionRegistryInterface::class, new ConnectionRegistryFaker($connectionFaker));
        app()->forgetInstance(ThreadRegistry::class);
        app()->forgetInstance(BusInterface::class);

        $bus = app(BusInterface::class);

        $fake = new self($bus, $connectionFaker, $topicRegistry);

        app()->instance(BusInterface::class, $fake);

        return $fake;
    }

    #[\Override]
    public function routes(): array
    {
        return $this->bus->routes();
    }

    #[\Override]
    public function publish(ProducerMessageInterface $message): void
    {
        $this->bus->publish($message);
    }

    #[\Override]
    public function publishBatch(MessageBatch $messageBatch): void
    {
        $this->bus->publishBatch($messageBatch);
    }

    #[\Override]
    public function listener(string $listenerWorkerName): Listener
    {
        return $this->bus->listener($listenerWorkerName);
    }

    #[\Override]
    public function onConnection(string $connectionName): ThreadInterface
    {
        return $this->bus->onConnection($connectionName);
    }
    /**
     * Assert that a message of the given class was published,
     * optionally matching a callback condition on the resulting ProducerMessage.
     *
     * @param class-string $messageClass
     * @param callable(ProducerMessage): bool|null $callback
     */
    public function assertPublished(string $messageClass, ?callable $callback = null): void
    {
        $messages = $this->getPublished($messageClass);

        Assert::assertNotEmpty(
            $messages,
            "Expected [$messageClass] to be published, but it was not."
        );

        if ($callback !== null) {
            $matched = array_values(array_filter($messages, $callback));

            Assert::assertNotEmpty(
                $matched,
                "[$messageClass] was published but no message matched the given condition."
            );
        }
    }

    /**
     * Assert that a message of the given class was published exactly $times times.
     *
     * @param class-string $messageClass
     */
    public function assertPublishedTimes(string $messageClass, int $times): void
    {
        $count = count($this->getPublished($messageClass));

        Assert::assertSame(
            $times,
            $count,
            "Expected [$messageClass] to be published $times time(s), but it was published $count time(s)."
        );
    }

    /**
     * Assert that a message of the given class was NOT published.
     *
     * @param class-string $messageClass
     */
    public function assertNotPublished(string $messageClass): void
    {
        Assert::assertEmpty(
            $this->getPublished($messageClass),
            "Expected [$messageClass] not to be published, but it was."
        );
    }

    /**
     * Assert that no messages were published at all.
     */
    public function assertNothingPublished(): void
    {
        $all = array_merge(...array_values($this->connectionFaker->publishedMessages) ?: [[]]);

        Assert::assertEmpty($all, 'Expected no messages to be published, but some were.');
    }

    // -------------------------------------------------------------------------
    // Published message access
    // -------------------------------------------------------------------------

    /**
     * Return all ProducerMessage objects published for the given message class.
     * The returned objects represent the message after the full producer pipeline
     * (serialisation + middleware), so headers and payload reflect any modifications.
     *
     * @param class-string $messageClass
     * @return list<ProducerMessage>
     */
    public function getPublished(string $messageClass): array
    {
        $topicName = $this->topicNameFor($messageClass);

        if ($topicName === null) {
            return [];
        }

        return $this->connectionFaker->publishedMessages[$topicName] ?? [];
    }

    /**
     * Return all published ProducerMessage objects for every topic, flat.
     *
     * @return list<ProducerMessage>
     */
    public function allPublished(): array
    {
        if (empty($this->connectionFaker->publishedMessages)) {
            return [];
        }

        return array_merge(...array_values($this->connectionFaker->publishedMessages));
    }

    /**
     * Assert that at least one message on the given topic was committed,
     * optionally matching a callback condition on the ConsumerMessageInterface.
     *
     * The callback receives a ConsumerMessageInterface instance, so you can inspect
     * payload(), headers(), key(), and — for the addMessage() path — original().
     *
     * @param callable(ConsumerMessageInterface): bool|null $callback
     */
    public function assertCommitted(string $topicKey, ?callable $callback = null): void
    {
        $messages = $this->getCommitted($topicKey);

        Assert::assertNotEmpty(
            $messages,
            "Expected a message on topic [$topicKey] to be committed, but none was."
        );

        if ($callback !== null) {
            $matched = array_values(array_filter($messages, $callback));

            Assert::assertNotEmpty(
                $matched,
                "A message on topic [$topicKey] was committed but none matched the given condition."
            );
        }
    }

    /**
     * Assert that exactly $times messages were committed on the given topic.
     */
    public function assertCommittedTimes(string $topicKey, int $times): void
    {
        $count = count($this->getCommitted($topicKey));

        Assert::assertSame(
            $times,
            $count,
            "Expected $times committed message(s) on topic [$topicKey], but got $count."
        );
    }

    /**
     * Assert that no messages were committed at all.
     */
    public function assertNothingCommitted(): void
    {
        $all = array_merge(...array_values($this->connectionFaker->committedMessages) ?: [[]]);

        Assert::assertEmpty($all, 'Expected no messages to be committed, but some were.');
    }

    /**
     * Return all committed ConsumerMessageInterface objects for the given topic key.
     *
     * @return list<ConsumerMessageInterface>
     */
    public function getCommitted(string $topicKey): array
    {
        $topicName = $this->topicRegistry->getTopicName($topicKey);

        return $this->connectionFaker->committedMessages[$topicName] ?? [];
    }

    public function addMessage(Message $message): void
    {
        $this->connectionFaker->addMessage($message);
    }

    public function listen(string $workerName): void
    {
        app(BusInterface::class)
            ->listener($workerName)
            ->listen();
    }

    /**
     * Resolve the full topic name for a given message class via the bus routes.
     * Returns null when no route is configured for the class.
     *
     * @param class-string $messageClass
     */
    private function topicNameFor(string $messageClass): ?string
    {
        foreach (app(BusInterface::class)->routes() as $route) {
            if ($route->messageClass === $messageClass) {
                return $route->topic->name;
            }
        }

        return null;
    }
}
