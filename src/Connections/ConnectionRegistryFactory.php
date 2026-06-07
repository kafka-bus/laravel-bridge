<?php

namespace KafkaBus\Laravel\Connections;

use Illuminate\Config\Repository;
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;
use KafkaBus\Core\Connections\Registry\DriverRegistry;

final readonly class ConnectionRegistryFactory
{
    public function __construct(
        private Repository $config,
        private ConnectionFactory $connectionFactory,
    ) {
    }

    public function create(DriverRegistry $driverRegistry): ConnectionRegistry
    {
        $connections = $this->config->get('kafka-bus.connections', []);
        $connections = array_map(
            fn (array $connection) => $this->connectionFactory->create($connection['driver'], $connection['options']),
            $connections
        );

        return new ConnectionRegistry($connections, $driverRegistry);
    }
}
