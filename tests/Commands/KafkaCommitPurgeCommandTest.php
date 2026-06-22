<?php

declare(strict_types=1);

use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;
use KafkaBus\Laravel\Components\Commiter\PurgeInterface;

it('purges commits using the configured days', function () {
    $source = Mockery::mock(RepositorySourceInterface::class, PurgeInterface::class);
    $source->shouldReceive('purge')
        ->once()
        ->withArgs(fn (\DateTimeImmutable $before) => $before < new \DateTimeImmutable('-13 days'))
        ->andReturn(5);

    $this->app->instance(RepositorySourceInterface::class, $source);

    $this->artisan('kafka:commit:purge')
        ->expectsOutput('Purged 5 commit(s) older than 14 day(s).')
        ->assertExitCode(0);
});

it('uses the --days option when provided', function () {
    $source = Mockery::mock(RepositorySourceInterface::class, PurgeInterface::class);
    $source->shouldReceive('purge')
        ->once()
        ->withArgs(fn (\DateTimeImmutable $before) => $before < new \DateTimeImmutable('-29 days'))
        ->andReturn(3);

    $this->app->instance(RepositorySourceInterface::class, $source);

    $this->artisan('kafka:commit:purge', ['--days' => 30])
        ->expectsOutput('Purged 3 commit(s) older than 30 day(s).')
        ->assertExitCode(0);
});

it('outputs zero when nothing was purged', function () {
    $source = Mockery::mock(RepositorySourceInterface::class, PurgeInterface::class);
    $source->shouldReceive('purge')->once()->andReturn(0);

    $this->app->instance(RepositorySourceInterface::class, $source);

    $this->artisan('kafka:commit:purge')
        ->expectsOutput('Purged 0 commit(s) older than 14 day(s).')
        ->assertExitCode(0);
});

it('fails when the source does not support purging', function () {
    $source = Mockery::mock(RepositorySourceInterface::class);

    $this->app->instance(RepositorySourceInterface::class, $source);

    $this->artisan('kafka:commit:purge')
        ->expectsOutput('The active repository source does not support purging.')
        ->assertExitCode(1);
});
