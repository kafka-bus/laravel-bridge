<?php

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Container\BindingResolutionException;
use KafkaBus\Core\BusInterface;
use KafkaBus\Core\Exceptions\Consumers\ConsumerException;
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;
use KafkaBus\Laravel\Workers\WorkerFactory;
use KafkaBus\Laravel\Workers\WorkerFactoryInterface;
use KafkaBus\Laravel\Workers\WorkerRegistry;
use KafkaBus\Worker\Consumers\ConsumerStreamFactoryInterface;
use KafkaBus\Worker\WorkerRunner;
use KafkaBus\Worker\WorkerRunnerFactory;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Command\SignalableCommandInterface;

abstract class AbstractKafkaConsumeCommand extends Command implements SignalableCommandInterface
{
    protected ?WorkerRunner $runner = null;

    /**
     * @var class-string<WorkerFactoryInterface>
     */
    protected string $factoryClass = WorkerFactory::class;

    /**
     * @param BusInterface $bus
     * @param LoggerInterface $logger
     * @return int
     *
     * @throws MessageConsumerNotHandledException
     * @throws BindingResolutionException
     */
    public function handle(BusInterface $bus, LoggerInterface $logger): int
    {
        $workerNames = $this->getWorkerNames();
        $outputName = implode(', ', $workerNames);

        $runnerFactory = new WorkerRunnerFactory(
            workerRegistry: new WorkerRegistry(app()->make($this->factoryClass)),
            streamFactory: app()->make(ConsumerStreamFactoryInterface::class),
        );

        if (count($workerNames) == 0) {
            $this->error('No workers for consuming.');

            return self::FAILURE;
        }

        try {
            $this->info("Start consuming for \"$outputName\"");

            $this->runner = $runnerFactory->create($bus, $workerNames);
            $this->runner->run();

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
        $this->runner?->forceStop();

        return $previousExitCode;
    }

    /**
     * @return list<string>
     */
    abstract protected function getWorkerNames(): array;
}
