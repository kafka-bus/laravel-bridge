<?php

namespace KafkaBus\Laravel\Connections\Drivers;

use KafkaBus\Core\Connections\Config\NullConnectionConfig;
use KafkaBus\Core\Interfaces\Connections\ConnectionConfigInterface;

final class NullDriverConnectionFactory extends DriverConnectionFactory
{
    public function create(array $options): ConnectionConfigInterface
    {
        return new NullConnectionConfig();
    }
}
