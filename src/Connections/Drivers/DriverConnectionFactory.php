<?php

namespace KafkaBus\Laravel\Connections\Drivers;

use KafkaBus\Core\Connections\Config\ConnectionConfigInterface;

abstract class DriverConnectionFactory
{
    abstract public function create(array $options): ConnectionConfigInterface;
}
