<?php

return [
    'default' => env('KAFKA_CONNECTION', 'kafka'),

    'connections' => [
        'testing' => [
            'driver' => 'null',
            'options' => [],
        ],

        'kafka' => [
            'driver' => 'kafka',
            'options' => [
                /*
                 | Your kafka brokers url.
                 */
                'metadata.broker.list' => env('KAFKA_BROKER_LIST', 'localhost:9092'),

                /*
                 | Default security protocol
                 */
                'security.protocol' => env('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
                'sasl.mechanisms' => env('KAFKA_SASL_MECHANISMS', 'PLAIN'),
                'sasl.username' => env('KAFKA_SASL_USERNAME'),
                'sasl.password' => env('KAFKA_SASL_PASSWORD'),

                /*
                 | Choose if debug is enabled or not.
                 */
                'debug' => env('KAFKA_DEBUG', false),
            ],
        ],

//        'outbox' => [
//            'driver' => 'kafka_outbox',
//            'options' => [
//                /*
//                 | Name connection to published transactional messages
//                 | from Database
//                 */
//                'connection_for' => 'kafka'
//            ],
//        ],
    ],

    'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local').'.'),

    'topics' => [
        //'products' => 'fact.products.1',
    ],

    'consumers' => [
        /*
         | Optional, defaults to empty array.
         | Array of middleware.
        */
        'middleware' => [
            // KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class
        ],

        /*
         | This defines Workers that will be run in separate processes in order to
         | subscribe to Apache Kafka topics.
         */
        'workers' => [
            /*
             | This is worker name.
             | To start Worker, run the command:
             | php artisan kafka:consume default
             */
            'default' => [
                /*
                 | Optional, defaults to empty array.
                 | Array of middleware.
                */
                'middleware' => [
                    // KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class
                ],

                //'additional_options' => [],
                //'auto_commit' => false, // Override global option, remove if not need
                //'consumer_timeout' => 20000, // Override global option, remove if not need

                /*
                 | A list of topics that will be subscribed to by the current employee.
                 | For each of them, you need to create a PHP class that will handle Apache Kafka messages.
                 */
                'topics' => [
                    //'products' => App\Kafka\Consumers\ProductsTopicConsumer::class,

                    //'products' => [
                    //    'handler' => App\Kafka\Consumers\ProductsTopicConsumer::class,
                    //    'middleware' => []
                    //],
                ],
            ],

            // consume one topic when topic key == worker name
            //'products' => App\Kafka\Consumers\ProductsTopicConsumer::class,

            // consume one topic with options and topic key == worker name
            //'products' => [
            //    'middleware' => [],
            //    'handler' => App\Kafka\Consumers\ProductsTopicConsumer::class,
            //],

            // consume one topic when topic key != worker name
            //'products-other-name' => [
            //    'middleware' => [],
            //    'topic_key' => 'products',
            //    'handler' => App\Kafka\Consumers\ProductsTopicConsumer::class,
            //],
        ],

        /*
         | If you set enable.auto.commit (which is the default), then the consumer will automatically commit offsets periodically at the
         | interval set by auto.commit.interval.ms.
         */
        'auto_commit' => env('KAFKA_CONSUMER_AUTO_COMMIT', false),

        /*
         | Optional, defaults to 5000.
         | Kafka consume timeout in milliseconds.
         */
        'consumer_timeout' => 5_000,

        /*
         | Options for Kafka Consumer
         | https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
         */
        'additional_options' => [
            /*
             | Kafka consumers belonging to the same consumer group share a group id.
             | The consumers in a group then divides the topic partitions as fairly amongst themselves as possible by
             | establishing that each partition is only consumed by a single consumer from the group.
             | This config defines the consumer group id you want to use for your project.
             */
            'group.id' => env('KAFKA_CONSUMER_GROUP_ID', env('APP_NAME')),

            /*
             | Maximum allowed time between calls to consume messages for high-level consumers.
             */
            'max.poll.interval.ms' => env('KAFKA_MAX_POLL_INTERVAL_MS', 300_000),

            /*
             | Client group session and failure detection timeout.
             */
            'session.timeout.ms' => env('KAFKA_SESSION_TIMEOUT_MS', 45_000),

            /*
             | Group session keepalive heartbeat interval.
             */
            'heartbeat.interval.ms' => env('KAFKA_HEARTBEAT_INTERVAL_MS', 3_000),

            'auto.offset.reset' => 'beginning',
        ],
    ],

    'producers' => [
        /*
         | Optional, defaults to empty array.
         | Array of middleware.
        */
        'middleware' => [
            // KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class
        ],

        /*
         | Producer message routes to topic kafka
         */
        'routes' => [
            //App\Kafka\Messages\ProductMessage::class => [
            //    'topic_key' => 'products',
            //    'middleware' => [],
            //    'additional_options' => [],
            //    'flush_timeout' => 5000, // Override global option, remove if not need
            //    'flush_retries' => 5, // Override global option, remove if not need
            //],

            // create new route without options
            //App\Kafka\Messages\ProductMessage::class => 'products',
        ],

        /*
         | Optional, defaults to 5000.
         | Kafka producer flush timeout in milliseconds.
         */
        'flush_timeout' => 5000,

        /*
         | Optional, defaults to 5.
         | Kafka producer flush retries
         */
        'flush_retries' => 5,

        /*
         | Options for Kafka Producer
         */
        'additional_options' => [
            /*
             | Kafka supports 4 compression codecs: none , gzip , lz4 and snappy
             */
            'compression.codec' => env('KAFKA_PRODUCER_COMPRESSION_CODEC', 'snappy'),
        ],
    ],
];
