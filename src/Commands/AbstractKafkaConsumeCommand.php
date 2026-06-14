<?php

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Bus\Listeners\Listener;
use KafkaBus\Core\Exceptions\Consumers\ConsumerException;
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Command\SignalableCommandInterface;

abstract class AbstractKafkaConsumeCommand extends Command implements SignalableCommandInterface
{
    protected ?Listener $listener;

    /**
     * @param BusInterface $bus
     * @param LoggerInterface $logger
     * @return int
     *
     * @throws MessageConsumerNotHandledException
     */
    public function handle(BusInterface $bus, LoggerInterface $logger): int
    {
        $workerNames = $this->getWorkerNames();
        $outputName = implode(', ', $workerNames);

        if (count($workerNames) == 0) {
            $this->error('No workers for consuming.');

            return self::FAILURE;
        }

        try {
            $this->info("Start consuming for \"$outputName\"");

            $this->listener = $bus->listener($workerNames);
            $this->listener->listen();

            $this->info('Consumer finished');

            return self::SUCCESS;
        }
        catch (ConsumerException $exception) {
            $logger->error($exception->getMessage(), ['exception' => $exception]);

            $this->error("Consumer stopped. Error: {$exception->getMessage()}");

            return self::FAILURE;
        }
    }

    public function getSubscribedSignals(): array
    {
        return array_values(array_filter([
            defined('SIGTERM') ? SIGTERM : null,
            defined('SIGINT') ? SIGINT : null,
            defined('SIGQUIT') ? SIGQUIT : null,
        ]));
    }

    public function handleSignal(int $signal, false|int $previousExitCode = 0): int|false
    {
        $this->listener?->forceStop();

        return $previousExitCode;
    }

    /**
     * @return list<string>
     */
    abstract protected function getWorkerNames(): array;
}
