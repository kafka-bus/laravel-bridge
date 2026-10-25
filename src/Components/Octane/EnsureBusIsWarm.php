<?php

namespace KafkaBus\Laravel\Components\Octane;

use KafkaBus\Core\BusInterface;

class EnsureBusIsWarm
{
    public function handle(object $event): void
    {
        $event->app->make(BusInterface::class);
    }
}
