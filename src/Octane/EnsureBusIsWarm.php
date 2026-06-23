<?php

namespace KafkaBus\Laravel\Octane;

use KafkaBus\Core\Interfaces\Bus\BusInterface;

class EnsureBusIsWarm
{
    public function handle(object $event): void
    {
        $event->app->make(BusInterface::class);
    }
}
