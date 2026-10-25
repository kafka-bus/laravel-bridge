<?php

namespace KafkaBus\Laravel;

use Illuminate\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use KafkaBus\Core\Bus;
use KafkaBus\Core\BusInterface;
use KafkaBus\Core\Connections\ConnectionInterface;
use KafkaBus\Core\Connections\Registry\ConnectionRegistryInterface;
use KafkaBus\Core\Connections\Registry\DriverRegistry;
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Publishers\PublisherStreamFactory;
use KafkaBus\Core\Publishers\PublisherStreamFactoryInterface;
use KafkaBus\Core\Receivers\LazyReceiver;
use KafkaBus\Core\Receivers\Receiver;
use KafkaBus\Core\Receivers\ReceiverInterface;
use KafkaBus\Core\Receivers\Routing\ReceiverRouter;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Commands\KafkaCommitCommand;
use KafkaBus\Laravel\Commands\KafkaCommitPurgeCommand;
use KafkaBus\Laravel\Commands\KafkaConsumeRoutesCommand;
use KafkaBus\Laravel\Commands\KafkaConsumeCommand;
use KafkaBus\Laravel\Commands\KafkaConsumerRouteListCommand;
use KafkaBus\Laravel\Commands\KafkaConsumerOffsetSetCommand;
use KafkaBus\Laravel\Commands\KafkaConsumerOffsetShowCommand;
use KafkaBus\Laravel\Commands\KafkaPublisherRouteListCommand;
use KafkaBus\Laravel\Commands\KafkaWorkerListCommand;
use KafkaBus\Laravel\Components\Octane\EnsureBusIsWarm;
use KafkaBus\Laravel\Connections\ConnectionRegistryFactory;
use KafkaBus\Laravel\Factories\TopicRegistryFactory;
use KafkaBus\Laravel\Publishers\PublisherRoutesFactory;
use KafkaBus\Laravel\Receivers\ReceiverRoutesFactory;
use KafkaBus\Laravel\Workers\WorkerFactory;
use KafkaBus\Laravel\Workers\WorkerFactoryInterface;
use KafkaBus\Laravel\Workers\WorkerRegistry;
use KafkaBus\Worker\Consumers\ConsumerStreamFactory;
use KafkaBus\Worker\Consumers\ConsumerStreamFactoryInterface;
use KafkaBus\Worker\Registry\WorkerRegistryInterface;
use Laravel\Octane\Events\WorkerStarting;

class KafkaBusServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/kafka-bus.php', 'kafka-bus');

        $this->app->bind(TopicRegistry::class, $this->makeTopicRegistry(...));
        $this->app->bind(PublisherFactory::class, $this->makePublisherFactory(...));
        $this->app->bind(ReceiverRouter::class, $this->makeReceiverRouter(...));
        $this->app->bind(ReceiverInterface::class, $this->makeReceiver(...));

        $this->app->singleton(ConnectionRegistryInterface::class, $this->makeConnectionRegistry(...));
        $this->app->singleton(ConnectionInterface::class, $this->makeConnection(...));
        $this->app->singleton(WorkerRegistryInterface::class, $this->makeWorkerRegistry(...));

        $this->app->singleton(DriverRegistry::class, DriverRegistry::class);
        $this->app->singleton(BusInterface::class, Bus::class);

        $this->app->bind(WorkerFactoryInterface::class, WorkerFactory::class);

        $this->app->bind(PublisherStreamFactoryInterface::class, PublisherStreamFactory::class);
        $this->app->bind(ConsumerStreamFactoryInterface::class, ConsumerStreamFactory::class);
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/kafka-bus.php' => config_path('kafka-bus.php'),
        ], 'kafka-bus');

        if (class_exists(WorkerStarting::class)) {
            $this->app['events']->listen(
                WorkerStarting::class,
                EnsureBusIsWarm::class,
            );
        }

        if ($this->app->runningInConsole()) {
            $this->commands([
                KafkaCommitCommand::class,
                KafkaCommitPurgeCommand::class,
                KafkaConsumeCommand::class,
                KafkaConsumeRoutesCommand::class,
                KafkaConsumerRouteListCommand::class,
                KafkaConsumerOffsetShowCommand::class,
                KafkaConsumerOffsetSetCommand::class,
                KafkaWorkerListCommand::class,
                KafkaPublisherRouteListCommand::class,
            ]);
        }
    }

    protected function makeTopicRegistry(Application $app): TopicRegistry
    {
        return $app->make(TopicRegistryFactory::class)
            ->create();
    }

    protected function makePublisherFactory(Application $app): PublisherFactory
    {
        return new PublisherFactory(
            $app->make(PublisherStreamFactoryInterface::class),
            $app->make(PublisherRoutesFactory::class)->create(),
        );
    }

    protected function makeWorkerRegistry(Application $app): WorkerRegistryInterface
    {
        return new WorkerRegistry($app->make(WorkerFactory::class));
    }

    protected function makeReceiver(Application $app): ReceiverInterface
    {
        return new LazyReceiver(static fn () => new Receiver($app->make(ReceiverRouter::class)));
    }

    protected function makeReceiverRouter(Application $app): ReceiverRouter
    {
        return new ReceiverRouter($app->make(ReceiverRoutesFactory::class)->create());
    }

    protected function makeConnectionRegistry(Application $app): ConnectionRegistryInterface
    {
        return $app->make(ConnectionRegistryFactory::class)
            ->create($app->make(DriverRegistry::class));
    }

    protected function makeConnection(Application $app): ConnectionInterface
    {
        return $app->make(ConnectionRegistryInterface::class)
            ->connection($app['config']->get('kafka-bus.default'));
    }
}
