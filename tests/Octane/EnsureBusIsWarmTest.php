<?php

use KafkaBus\Core\BusInterface;
use KafkaBus\Laravel\Components\Octane\EnsureBusIsWarm;
use Laravel\Octane\Events\WorkerStarting;

it('resolves BusInterface when octane worker starts', function () {
    $listener = new EnsureBusIsWarm();
    $event = new WorkerStarting($this->app);

    expect($this->app->resolved(BusInterface::class))->toBeFalse();

    $listener->handle($event);

    expect($this->app->resolved(BusInterface::class))->toBeTrue();
});

it('registers WorkerStarting listener when octane is installed', function () {
    $listeners = $this->app['events']
        ->getListeners(WorkerStarting::class);

    expect($listeners)->not->toBeEmpty();
});
