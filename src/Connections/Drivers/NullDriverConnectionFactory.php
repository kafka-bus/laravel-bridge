<?php

namespace KafkaBus\Laravel\Connections\Drivers;

use KafkaBus\Core\Connections\Config\ConnectionConfigInterface;
use KafkaBus\Core\Connections\Config\NullConnectionConfig;

final class NullDriverConnectionFactory extends DriverConnectionFactory
{
    public function create(array $options): ConnectionConfigInterface
    {
        return new NullConnectionConfig();
    }
}
