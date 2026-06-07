<?php

use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Commiter\Repositories\NativeMessageRepository;

return [
    /*
     | Database connection used by DatabaseRepositorySource.
     | Null means the application's default connection.
     */
    'connection' => env('KAFKA_COMMITER_CONNECTION'),

    /*
     | Table that stores consumed message commits.
     */
    'table' => 'kafka_bus_commits',

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
