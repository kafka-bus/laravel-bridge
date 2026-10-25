<?php

declare(strict_types=1);

use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\RedisManager;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;
use KafkaBus\Laravel\Components\Commiter\CommiterServiceProvider;
use KafkaBus\Laravel\Components\Commiter\RedisRepositorySource;

beforeEach(function () {
    $this->app->register(CommiterServiceProvider::class);
});

it('merges the default commiter config', function () {
    expect(config('kafka-bus-commiter.source'))->toBe('default')
        ->and(config('kafka-bus-commiter.repository'))->toBe('idempotency');
});

it('publishes config and migrations', function () {
    $paths = CommiterServiceProvider::pathsToPublish(CommiterServiceProvider::class, 'kafka-bus-commiter');

    expect($paths)->toHaveCount(2);
});

it('fails for an undefined source', function () {
    config()->set('kafka-bus-commiter.source', 'missing');

    $this->app->make(RepositorySourceInterface::class);
})->throws(InvalidArgumentException::class, 'Undefined kafka-bus-commiter source [missing].');

it('fails for an unsupported source driver', function () {
    config()->set('kafka-bus-commiter.sources.default.driver', 'memcached');

    $this->app->make(RepositorySourceInterface::class);
})->throws(InvalidArgumentException::class, 'Unsupported kafka-bus-commiter source driver [memcached].');

it('builds a redis source from a phpredis connection', function () {
    config()->set('kafka-bus-commiter.source', 'cache');

    $redis = Mockery::mock(RedisManager::class);
    $redis->shouldReceive('connection')->with('default')->andReturn(Mockery::mock(PhpRedisConnection::class));
    $this->app->instance(RedisManager::class, $redis);

    expect($this->app->make(RepositorySourceInterface::class))->toBeInstanceOf(RedisRepositorySource::class);
});

it('rejects a redis connection that is not phpredis', function () {
    config()->set('kafka-bus-commiter.source', 'cache');

    $redis = Mockery::mock(RedisManager::class);
    $redis->shouldReceive('connection')->andReturn(new stdClass());
    $this->app->instance(RedisManager::class, $redis);

    $this->app->make(RepositorySourceInterface::class);
})->throws(InvalidArgumentException::class, 'Redis repository source requires a PhpRedis connection');
