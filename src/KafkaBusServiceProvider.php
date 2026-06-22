<?php

namespace KafkaBus\Laravel;

use Illuminate\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use KafkaBus\Core\Bus;
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Bus\Publishers\PublisherFactory;
use KafkaBus\Core\Bus\ThreadFactory;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Connections\Registry\DriverRegistry;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Core\Interfaces\Connections\ConnectionRegistryInterface;
use KafkaBus\Core\Producers\ProducerStreamFactory;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Laravel\Commands\KafkaCommitCommand;
use KafkaBus\Laravel\Commands\KafkaCommitPurgeCommand;
use KafkaBus\Laravel\Commands\KafkaConsumeAllCommand;
use KafkaBus\Laravel\Commands\KafkaConsumeCommand;
use KafkaBus\Laravel\Commands\KafkaOffsetSetCommand;
use KafkaBus\Laravel\Commands\KafkaOffsetShowCommand;
use KafkaBus\Laravel\Commands\KafkaRouteListCommand;
use KafkaBus\Laravel\Commands\KafkaWorkerListCommand;
use KafkaBus\Laravel\Connections\ConnectionRegistryFactory;
use KafkaBus\Laravel\Factories\TopicRegistryFactory;
use KafkaBus\Laravel\Listeners\LaravelWorkerRegistry;
use KafkaBus\Laravel\Listeners\WorkerFactory;
use KafkaBus\Laravel\Publishers\LaravelPublisherRoutesFactory;

class KafkaBusServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/kafka-bus.php', 'kafka-bus');

        $this->app->bind(TopicRegistry::class, $this->makeTopicRegistry(...));

        $this->app->bind(PublisherFactory::class, $this->makePublisherFactory(...));
        $this->app->bind(ListenerFactory::class, $this->makeListenerFactory(...));

        $this->app->singleton(DriverRegistry::class, $this->makeDriverRegistry(...));
        $this->app->singleton(ThreadRegistry::class, $this->makeThreadRegistry(...));
        $this->app->singleton(ConnectionRegistryInterface::class, $this->makeConnectionRegistry(...));

        $this->app->singleton(BusInterface::class, $this->makeBus(...));
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/kafka-bus.php' => config_path('kafka-bus.php'),
        ], 'kafka-bus');

        if ($this->app->runningInConsole()) {
            $this->commands([
                KafkaCommitCommand::class,
                KafkaCommitPurgeCommand::class,
                KafkaConsumeCommand::class,
                KafkaConsumeAllCommand::class,
                KafkaOffsetShowCommand::class,
                KafkaOffsetSetCommand::class,
                KafkaWorkerListCommand::class,
                KafkaRouteListCommand::class,
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
            new ProducerStreamFactory(),
            $app->make(LaravelPublisherRoutesFactory::class)->create(),
        );
    }

    protected function makeListenerFactory(Application $app): ListenerFactory
    {
        return new ListenerFactory(
            new ConsumerStreamFactory(),
            new LaravelWorkerRegistry($app->make(WorkerFactory::class)),
        );
    }

    protected function makeThreadRegistry(Application $app): ThreadRegistry
    {
        return new ThreadRegistry(
            $app->make(ConnectionRegistryInterface::class),
            new ThreadFactory(
                $app->make(ListenerFactory::class),
                $app->make(PublisherFactory::class),
            )
        );
    }

    protected function makeBus(Application $app): BusInterface
    {
        return new Bus(
            $app->make(ThreadRegistry::class),
            $app['config']->get('kafka-bus.default')
        );
    }

    protected function makeDriverRegistry(): DriverRegistry
    {
        return new DriverRegistry();
    }

    protected function makeConnectionRegistry(Application $app): ConnectionRegistryInterface
    {
        return $app->make(ConnectionRegistryFactory::class)
            ->create($app->make(DriverRegistry::class));
    }
}
