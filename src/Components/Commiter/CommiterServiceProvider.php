<?php

namespace KafkaBus\Laravel\Components\Commiter;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Database\ConnectionResolverInterface;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;
use KafkaBus\Commiter\Interfaces\ConsumerMessageRepositoryInterface;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Commiter\Repositories\NativeMessageRepository;

final class CommiterServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../../../config/kafka-bus-commiter.php', 'kafka-bus-commiter');

        $this->app->singleton(RepositorySourceInterface::class, $this->makeRepositorySource(...));
        $this->app->singleton(ConsumerMessageRepositoryInterface::class, $this->makeConsumerMessageRepository(...));
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../../../config/kafka-bus-commiter.php' => $this->app->configPath('kafka-bus-commiter.php'),
            __DIR__.'/../../../database/migrations' => $this->app->databasePath('migrations'),
        ], 'kafka-bus-commiter');
    }

    private function makeRepositorySource(Application $app): DatabaseRepositorySource
    {
        $config = $app['config']->get('kafka-bus-commiter', []);

        return new DatabaseRepositorySource(
            connection: $app->make(ConnectionResolverInterface::class)
                ->connection($config['connection'] ?? null),
            table: $config['table'] ?? 'kafka_bus_commits',
        );
    }

    private function makeConsumerMessageRepository(Application $app): ConsumerMessageRepositoryInterface
    {
        $config = $app['config']->get('kafka-bus-commiter', []);

        $name = $config['repository'] ?? 'idempotency';
        $class = $config['repositories'][$name] ?? match ($name) {
            'native' => NativeMessageRepository::class,
            'idempotency' => IdempotencyMessageRepository::class,
            default => throw new InvalidArgumentException(
                "Unsupported kafka-bus-commiter repository [{$name}]."
            ),
        };

        return $app->make($class);
    }
}
