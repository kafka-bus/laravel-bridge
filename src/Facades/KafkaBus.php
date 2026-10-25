<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Facades;

use Illuminate\Support\Facades\Facade;
use KafkaBus\Core\BusInterface;
use KafkaBus\Core\Connections\ConnectionInterface;
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Producers\Messages\ProducerMessage;
use KafkaBus\Core\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Publishers\MessageBatch;
use KafkaBus\Laravel\Testing\FakeBus;
use RdKafka\Message;

/**
 * @method static ConnectionInterface connection()
 * @method static void publish(ProducerMessageInterface $message)
 * @method static void publishBatch(MessageBatch $messageBatch)
 * @method static void dispatch(ConsumerMessageInterface $message)
 *
 * @method static void assertPublished(string $messageClass, callable|null $callback = null)
 * @method static void assertPublishedTimes(string $messageClass, int $times)
 * @method static void assertNotPublished(string $messageClass)
 * @method static void assertNothingPublished()
 * @method static void assertCommitted(string $topicKey, callable|null $callback = null)
 * @method static void assertCommittedTimes(string $topicKey, int $times)
 * @method static void assertNothingCommitted()
 * @method static void assertDispatched(string $topicKey, callable|null $callback = null)
 * @method static void assertDispatchedTimes(string $topicKey, int $times)
 * @method static void assertNotDispatched(string $topicKey)
 * @method static void assertNothingDispatched()
 * @method static list<ProducerMessage> getPublished(string $messageClass)
 * @method static list<ProducerMessage> allPublished()
 * @method static list<ConsumerMessageInterface> getCommitted(string $topicKey)
 * @method static list<ConsumerMessageInterface> getDispatched(string $topicKey)
 * @method static list<ConsumerMessageInterface> allDispatched()
 * @method static void addMessage(Message $message)
 * @method static void listen(string|list<string> $workerName)
 *
 * @see FakeBus
 */
class KafkaBus extends Facade
{
    public static function fake(): void
    {
        self::swap(FakeBus::make());
    }

    protected static function getFacadeAccessor(): string
    {
        return BusInterface::class;
    }
}
