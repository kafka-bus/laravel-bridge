<?php

namespace KafkaBus\Laravel\Components\Commiter;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Database\ConnectionResolverInterface;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\RedisManager;
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

    private function makeRepositorySource(Application $app): RepositorySourceInterface
    {
        $config = $app['config']->get('kafka-bus-commiter', []);

        $sourceName = $config['source'] ?? 'default';
        $sourceConfig = $config['sources'][$sourceName] ?? null;

        if ($sourceConfig === null) {
            throw new InvalidArgumentException(
                "Undefined kafka-bus-commiter source [{$sourceName}]."
            );
        }

        $driver = $sourceConfig['driver'] ?? '';
        $options = $sourceConfig['options'] ?? [];

        return match ($driver) {
            'database' => new DatabaseRepositorySource(
                connection: $app->make(ConnectionResolverInterface::class)
                    ->connection($options['connection'] ?? null),
                table: $options['table'] ?? 'kafka_bus_commits',
            ),
            'redis' => $this->makeRedisRepositorySource($app, $options, $config),
            default => throw new InvalidArgumentException(
                "Unsupported kafka-bus-commiter source driver [{$driver}]."
            ),
        };
    }

    /**
     * @param  array<string, mixed>  $options
     * @param  array<string, mixed>  $config
     */
    private function makeRedisRepositorySource(Application $app, array $options, array $config): RedisRepositorySource
    {
        $connection = $app->make(RedisManager::class)
            ->connection($options['connection'] ?? 'default');

        if (! $connection instanceof PhpRedisConnection) {
            throw new InvalidArgumentException(
                'Redis repository source requires a PhpRedis connection (driver=phpredis).'
            );
        }

        return new RedisRepositorySource(
            connection: $connection,
            prefix: $options['prefix'] ?? 'kafka_bus_commits',
            ttl: (int) ($config['purge']['days'] ?? 14) * 86400,
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
