<?php

namespace KafkaBus\Laravel\Connections\Drivers;

use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Interfaces\Connections\ConnectionConfigInterface;

final class KafkaDriverConnectionFactory extends DriverConnectionFactory
{
    public function create(array $options): ConnectionConfigInterface
    {
        $debug = $options['debug'] ?? false;

        if (isset($options['debug'])) {
            unset($options['debug']);
        }

        return new KafkaConnectionConfig(
            brokerList: $options['metadata.broker.list'],
            debug: $debug,
            extra: $options
        );
    }
}
