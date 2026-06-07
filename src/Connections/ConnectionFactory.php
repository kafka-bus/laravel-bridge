<?php

namespace KafkaBus\Laravel\Connections;

use Illuminate\Container\Container;
use KafkaBus\Core\Interfaces\Connections\ConnectionConfigInterface;
use KafkaBus\Laravel\Connections\Drivers\DriverConnectionFactory;
use KafkaBus\Laravel\Connections\Drivers\KafkaDriverConnectionFactory;
use KafkaBus\Laravel\Connections\Drivers\NullDriverConnectionFactory;
use KafkaBus\Laravel\Exceptions\KafkaBusConfigurationException;

final readonly class ConnectionFactory
{
    public function __construct(
        private Container $container,
    ) {
    }

    public function create(string $driver, array $options): ConnectionConfigInterface
    {
        $factoryClass = match ($driver) {
            'null' => NullDriverConnectionFactory::class,
            'kafka' => KafkaDriverConnectionFactory::class,
            default => $driver,
        };

        if (is_subclass_of($factoryClass, DriverConnectionFactory::class)) {
            return $this->container->make($factoryClass)
                ->create($options);
        }

        throw new KafkaBusConfigurationException("Unsupported driver [$driver]");
    }
}
