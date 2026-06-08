<?php

declare(strict_types=1);

use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

it('commits the given key via repository', function () {
    $repository = Mockery::mock(RepositorySourceInterface::class);
    $repository->shouldReceive('get')->once()->with('order-123')->andReturnNull();
    $repository->shouldReceive('commit')->once()->with('order-123');

    $this->app->instance(RepositorySourceInterface::class, $repository);

    $this->artisan('kafka:commit', ['key' => 'order-123'])
        ->assertExitCode(0);
});

it('outputs confirmation message with the key', function () {
    $repository = Mockery::mock(RepositorySourceInterface::class);
    $repository->shouldReceive('get')->once()->with('order-123')->andReturnNull();
    $repository->shouldReceive('commit')->once()->with('order-123');

    $this->app->instance(RepositorySourceInterface::class, $repository);

    $this->artisan('kafka:commit', ['key' => 'order-123'])
        ->expectsOutput('Commit for key [order-123] has been saved.')
        ->assertExitCode(0);
});

it('passes the exact key argument to the repository', function () {
    $key = 'some-unique-key-456';

    $repository = Mockery::mock(RepositorySourceInterface::class);
    $repository->shouldReceive('get')->once()->with($key)->andReturnNull();
    $repository->shouldReceive('commit')->once()->with($key);

    $this->app->instance(RepositorySourceInterface::class, $repository);

    $this->artisan('kafka:commit', ['key' => $key])
        ->assertExitCode(0);
});

it('warns and exits with failure when commit already exists', function () {
    $attempt = new Attempt(key: 'order-123', commitedAt: new DateTimeImmutable());

    $repository = Mockery::mock(RepositorySourceInterface::class);
    $repository->shouldReceive('get')->once()->with('order-123')->andReturn($attempt);
    $repository->shouldNotReceive('commit');

    $this->app->instance(RepositorySourceInterface::class, $repository);

    $this->artisan('kafka:commit', ['key' => 'order-123'])
        ->expectsOutput('Commit for key [order-123] already exists.')
        ->assertExitCode(1);
});

it('commits when attempt exists but has not been committed yet', function () {
    $attempt = new Attempt(key: 'order-123', commitedAt: null);

    $repository = Mockery::mock(RepositorySourceInterface::class);
    $repository->shouldReceive('get')->once()->with('order-123')->andReturn($attempt);
    $repository->shouldReceive('commit')->once()->with('order-123');

    $this->app->instance(RepositorySourceInterface::class, $repository);

    $this->artisan('kafka:commit', ['key' => 'order-123'])
        ->expectsOutput('Commit for key [order-123] has been saved.')
        ->assertExitCode(0);
});
