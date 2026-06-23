<?php

use KafkaBus\Core\Interfaces\Bus\BusInterface;
use KafkaBus\Laravel\Octane\EnsureBusIsWarm;
use Laravel\Octane\Events\WorkerStarting;

it('resolves BusInterface when octane worker starts', function () {
    $listener = new EnsureBusIsWarm();
    $event = new WorkerStarting($this->app);

    expect($this->app->resolved(BusInterface::class))->toBeFalse();

    $listener->handle($event);

    expect($this->app->resolved(BusInterface::class))->toBeTrue();
    expect($this->app->make(BusInterface::class))->toBeInstanceOf(BusInterface::class);
});

it('registers WorkerStarting listener when octane is installed', function () {
    $listeners = $this->app['events']->getListeners(WorkerStarting::class);

    expect($listeners)->not->toBeEmpty();
});
