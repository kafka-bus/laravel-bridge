<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Components\Commiter;

interface PurgeInterface
{
    public function purge(\DateTimeImmutable $before): int;
}
