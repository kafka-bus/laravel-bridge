<?php

use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Commiter\Repositories\NativeMessageRepository;

return [
    /*
     | Active repository source name (must match a key in "sources").
     */
    'source' => env('KAFKA_COMMITER_SOURCE', 'default'),

    'sources' => [
        'default' => [
            'driver' => 'database',
            'options' => [
                /*
                 | Database connection used by DatabaseRepositorySource.
                 | Null means the application's default connection.
                 */
                'connection' => env('KAFKA_COMMITER_CONNECTION'),

                /*
                 | Table that stores consumed message commits.
                 */
                'table' => 'kafka_bus_commits',
            ],
        ],

        'cache' => [
            'driver' => 'redis',
            'options' => [
                'connection' => 'default',
                'prefix' => 'kafka_bus_commits',
            ],
        ],
    ],

    /*
     | Settings for the kafka:commit:purge command.
     | Only applies to sources that support purging (e.g. database).
     | Redis sources use key TTL instead.
     */
    'purge' => [
        'days' => env('KAFKA_COMMITER_PURGE_DAYS', 14),
    ],

    /*
     | Consumer message repository implementation.
     | Supported: "idempotency", "native".
     |
     | idempotency — resolves key from "x-idempotency-key" header combined with topic name,
     |               falls back to message id when the header is missing.
     | native      — uses raw message id as the key.
     */
    'repository' => env('KAFKA_COMMITER_REPOSITORY', 'idempotency'),

    'repositories' => [
        'idempotency' => IdempotencyMessageRepository::class,
        'native' => NativeMessageRepository::class,
    ],
];
