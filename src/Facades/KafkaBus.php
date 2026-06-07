<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Facades;

use Illuminate\Support\Facades\Facade;
use KafkaBus\Core\Bus\Listeners\Listener;
use KafkaBus\Core\Bus\MessageBatch;
use KafkaBus\Core\Consumers\Router\Route;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Interfaces\Bus\ThreadInterface;
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Producers\Messages\ProducerMessage;
use KafkaBus\Laravel\Testing\FakeBus;
use RdKafka\Message;

/**
 * @method static list<Route> routes()
 * @method static void publish(ProducerMessageInterface $message)
 * @method static void publishBatch(MessageBatch $messageBatch)
 * @method static Listener listener(string $listenerWorkerName)
 * @method static ThreadInterface onConnection(string $connectionName)
 *
 * @method static void assertPublished(string $messageClass, callable|null $callback = null)
 * @method static void assertPublishedTimes(string $messageClass, int $times)
 * @method static void assertNotPublished(string $messageClass)
 * @method static void assertNothingPublished()
 * @method static void assertCommitted(string $topicKey, callable|null $callback = null)
 * @method static void assertCommittedTimes(string $topicKey, int $times)
 * @method static void assertNothingCommitted()
 * @method static list<ProducerMessage> getPublished(string $messageClass)
 * @method static list<ProducerMessage> allPublished()
 * @method static list<ConsumerMessageInterface> getCommitted(string $topicKey)
 * @method static void addMessage(Message $message)
 * @method static void listen(string $workerName)
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
