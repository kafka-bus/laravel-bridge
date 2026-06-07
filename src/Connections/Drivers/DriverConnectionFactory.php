<?php

namespace KafkaBus\Laravel\Connections\Drivers;

use KafkaBus\Core\Interfaces\Connections\ConnectionConfigInterface;

abstract class DriverConnectionFactory
{
    abstract public function create(array $options): ConnectionConfigInterface;
}
