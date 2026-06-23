# Kafka Bus for Laravel

[![Latest Version on Packagist](https://img.shields.io/packagist/v/kafka-bus/laravel-bridge.svg?style=flat-square)](https://packagist.org/packages/kafka-bus/laravel-bridge)
[![GitHub Tests Action Status](https://img.shields.io/github/actions/workflow/status/kafka-bus/laravel-bridge/run-tests.yml?branch=1.x&label=tests&style=flat-square)](https://github.com/kafka-bus/laravel-bridge/actions?query=workflow%3Arun-tests+branch%3A1.x)
[![GitHub Code Style](https://img.shields.io/github/actions/workflow/status/kafka-bus/laravel-bridge/php-code-style.yml?branch=1.x&label=code-style&style=flat-square)](https://github.com/kafka-bus/laravel-bridge/actions?query=workflow%3Acode-style+branch%3A1.x)
[![GitHub PHPStan](https://img.shields.io/github/actions/workflow/status/kafka-bus/laravel-bridge/phpstan.yml?branch=1.x&label=phpstan&style=flat-square)](https://github.com/kafka-bus/laravel-bridge/actions?query=workflow%3Aphpstan+branch%3A1.x)
[![Total Downloads](https://img.shields.io/packagist/dt/kafka-bus/laravel-bridge.svg?style=flat-square)](https://packagist.org/packages/kafka-bus/laravel-bridge)

Laravel integration for [`kafka-bus`](https://github.com/kafka-bus/kafka-bus) — a configuration-driven Apache Kafka client built on top of `ext-rdkafka`. The package wires producers, consumer workers, topic routing, and middleware into the framework, and ships an optional **Commiter** component for idempotent message handling backed by the database.

## Requirements

- PHP `^8.2`
- Laravel
- `ext-rdkafka`

## Installation

Install the package via Composer:

```bash
composer require kafka-bus/laravel-bridge
```

Publish the main configuration file:

```bash
php artisan vendor:publish --tag=kafka-bus
```

To use the Commiter component (idempotency and commit tracking), additionally publish its configuration and migrations:

```bash
php artisan vendor:publish --tag=kafka-bus-commiter
php artisan migrate
```

## Configuration

The main configuration lives in `config/kafka-bus.php` and is split into four sections:

- `connections` — Kafka broker connections and driver-specific options.
- `topics` — logical topic keys mapped to physical Kafka topic names.
- `consumers` — workers, topic-to-handler bindings, middleware, and consumer options.
- `producers` — message-to-topic routes, middleware, and producer options.

### Connections

Each connection is selected by a `driver` and a set of `options` passed straight to `librdkafka`. The `default` key picks the active connection by name.

```php
'default' => env('KAFKA_CONNECTION', 'kafka'),

'connections' => [
    'kafka' => [
        'driver' => 'kafka',
        'options' => [
            'metadata.broker.list' => env('KAFKA_BROKER_LIST', 'localhost:9092'),
            'security.protocol'    => env('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
            'sasl.mechanisms'      => env('KAFKA_SASL_MECHANISMS', 'PLAIN'),
            'sasl.username'        => env('KAFKA_SASL_USERNAME'),
            'sasl.password'        => env('KAFKA_SASL_PASSWORD'),
            'debug'                => env('KAFKA_DEBUG', false),
        ],
    ],

    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

The `null` driver is useful for tests — calls to the bus succeed without touching a real broker.

### Topics

Topic names usually depend on the environment. The bus prepends `topic_prefix` to every physical topic name, and the `topics` map binds a short logical key to that physical name.

```php
'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local').'.'),

'topics' => [
    'products' => 'fact.products.1',
    'orders'   => 'fact.orders.1',
],
```

With `APP_ENV=production`, `products` resolves to `production.fact.products.1`.

### Producers

A producer route binds a message class to a logical topic key. The shortest form maps the class directly to a topic key:

```php
'producers' => [
    'middleware' => [
        // KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],

    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
    ],

    'flush_timeout' => 5000,
    'flush_retries' => 5,

    'additional_options' => [
        'compression.codec' => env('KAFKA_PRODUCER_COMPRESSION_CODEC', 'snappy'),
    ],
],
```

The verbose form lets you override timeouts, append per-route middleware, and pass driver options:

```php
'routes' => [
    App\Kafka\Messages\ProductMessage::class => [
        'topic_key'          => 'products',
        'middleware'         => [App\Kafka\Middleware\AuditTrailMiddleware::class],
        'additional_options' => [],
        'flush_timeout'      => 5000,
        'flush_retries'      => 5,
    ],
],
```

Publish a message through the bus:

```php
use KafkaBus\Core\Interfaces\Bus\BusInterface;

public function execute(BusInterface $bus): void
{
    $bus->publish(new \App\Kafka\Messages\ProductMessage(/* ... */));
}
```

Or via the `KafkaBus` facade:

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::publish(new \App\Kafka\Messages\ProductMessage(/* ... */));
```

### Consumers

Workers are the units consumed by the artisan command. Each worker subscribes to one or more topics and dispatches incoming messages to handler classes.

```php
'consumers' => [
    'middleware' => [
        // KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        // Multi-topic worker with per-worker overrides
        'default' => [
            'middleware'   => [],
            'auto_commit'  => false,
            'consume_timeout' => 20000,
            'topics' => [
                'products' => App\Kafka\Consumers\ProductsTopicConsumer::class,
                'orders'   => [
                    'handler'    => App\Kafka\Consumers\OrdersTopicConsumer::class,
                    'middleware' => [App\Kafka\Middleware\TenantContextMiddleware::class],
                ],
            ],
        ],

        // Single-topic worker, worker name == topic key
        'products' => App\Kafka\Consumers\ProductsTopicConsumer::class,

        // Single-topic worker with overrides, worker name == topic key
        'orders' => [
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\OrdersTopicConsumer::class,
        ],

        // Single-topic worker where worker name != topic key
        'products-secondary' => [
            'topic_key'  => 'products',
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\ProductsTopicConsumer::class,
        ],
    ],

    'auto_commit'     => env('KAFKA_CONSUMER_AUTO_COMMIT', false),
    'consume_timeout' => 5_000,

    'additional_options' => [
        'group.id'              => env('KAFKA_CONSUMER_GROUP_ID', env('APP_NAME')),
        'max.poll.interval.ms'  => env('KAFKA_MAX_POLL_INTERVAL_MS', 300_000),
        'session.timeout.ms'    => env('KAFKA_SESSION_TIMEOUT_MS', 45_000),
        'heartbeat.interval.ms' => env('KAFKA_HEARTBEAT_INTERVAL_MS', 3_000),
        'auto.offset.reset'     => 'beginning',
    ],
],
```

Each worker resolves options in this order: `additional_options`, `auto_commit`, `consume_timeout`, and middleware are taken from the worker entry, then merged with the global `consumers.*` defaults.

Run a worker:

```bash
php artisan kafka:consume default
```

## Artisan commands

| Command                                                            | Description                                                                                                                                                  |
|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka:consume {workerName}`                                       | Start a long-running consumer for the given worker.                                                                                                          |
| `kafka:worker:list`                                                | Show registered workers, their topic keys, resolved topic names, handlers, consumer middleware, and route middleware.                                        |
| `kafka:route:list`                                                 | Show registered producer routes (message class → topic) and middleware.                                                                                      |
| `kafka:offset:show {workerName}`                                   | Show current / min / max offsets for every partition of every topic the worker subscribes to.                                                                |
| `kafka:offset:set {workerName} {topicKey} {offset} {--partition=}` | Set the committed offset for a topic. `offset` accepts `earliest`, `latest`, or a numeric value. Omit `--partition` to apply to all partitions of the topic. |

### Inspecting workers and routes

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+------------------------------------------+---------------------+----------------+
| Worker   | Topic key | Topic name                 | Handler                                  | Consumer Middleware | Route Middleware|
+----------+-----------+----------------------------+------------------------------------------+---------------------+----------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsTopicConsumer|                     |                |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersTopicConsumer  | App\...\AuditMiddleware | App\...\TenantMiddleware |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsTopicConsumer|                     |                |
+----------+-----------+----------------------------+------------------------------------------+---------------------+----------------+
```

```bash
php artisan kafka:route:list
```

```
+----------------------------------------+-----------+----------------------------+------------+
| Message                                | Topic key | Topic name                 | Middleware |
+----------------------------------------+-----------+----------------------------+------------+
| App\Kafka\Messages\ProductMessage      | products  | production.fact.products.1 | 0          |
| App\Kafka\Messages\OrderMessage        | orders    | production.fact.orders.1   | 1          |
+----------------------------------------+-----------+----------------------------+------------+
```

### Inspecting and resetting offsets

```bash
php artisan kafka:offset:show default
```

```
+-----------+----------------------------+-----------+---------+-----+-----+
| Topic key | Topic name                 | Partition | Current | Min | Max |
+-----------+----------------------------+-----------+---------+-----+-----+
| products  | production.fact.products.1 | 0         | 142     | 0   | 200 |
| products  | production.fact.products.1 | 1         | 90      | 0   | 150 |
+-----------+----------------------------+-----------+---------+-----+-----+
```

Reset all partitions of a topic to the earliest available offset:

```bash
php artisan kafka:offset:set default products earliest
```

Move a single partition to an explicit numeric offset:

```bash
php artisan kafka:offset:set default products 150 --partition=0
```

Jump every partition to the high-water mark (skip backlog):

```bash
php artisan kafka:offset:set default products latest
```

The command prints the resulting offsets:

```
+-----------+----------------------------+-----------+-----+-----+
| Topic key | Topic name                 | Partition | Old | New |
+-----------+----------------------------+-----------+-----+-----+
| products  | production.fact.products.1 | 0         | 142 | 0   |
| products  | production.fact.products.1 | 1         | 90  | 0   |
+-----------+----------------------------+-----------+-----+-----+
```

> The worker must not be running while you reset its offsets — otherwise the active consumer group will overwrite the new position on its next commit.

## Commiter

The Commiter component (powered by `micromus/kafka-bus-commiter`) provides:

- **Consumer idempotency** — every incoming message is tracked in the `kafka_bus_commits` table; duplicates are skipped, retries are counted, and a configurable max-attempt threshold can stop poison messages.
- **Producer idempotency keys** — outgoing messages implementing `HasIdempotency` automatically receive an `x-idempotency-key` header, which the consumer side uses as the dedup key.

It is registered automatically by `CommiterServiceProvider` (loaded via package auto-discovery).

### Configuration

```php
// config/kafka-bus-commiter.php
return [
    'connection' => env('KAFKA_COMMITER_CONNECTION'),
    'table'      => 'kafka_bus_commits',
    'repository' => env('KAFKA_COMMITER_REPOSITORY', 'idempotency'),

    'repositories' => [
        'idempotency' => \KafkaBus\Commiter\Repositories\IdempotencyMessageRepository::class,
        'native'      => \KafkaBus\Commiter\Repositories\NativeMessageRepository::class,
    ],
];
```

- `connection` — Laravel database connection name; `null` uses the default connection.
- `table` — name of the commits table created by the published migration.
- `repository` — strategy for deriving the dedup key:
  - `idempotency` — reads the `x-idempotency-key` header combined with the topic name, falling back to the raw Kafka message id if the header is missing.
  - `native` — uses the raw Kafka message id only.
- `repositories` — registry of repository implementations; add your own class here and reference it via `KAFKA_COMMITER_REPOSITORY`.

### Enabling the consumer middleware

Add `ConsumerCommiterMiddleware` to the consumer middleware stack — either globally for every worker, or only for specific workers/topics:

```php
'consumers' => [
    'middleware' => [
        \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        'orders' => [
            'middleware' => [
                \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
            ],
            'handler' => App\Kafka\Consumers\OrdersTopicConsumer::class,
        ],
    ],
],
```

For each message the middleware:

1. Resolves a dedup key via the configured repository.
2. If the key was already committed — the message is skipped and a warning is logged.
3. If the per-key attempt count exceeds `maxAttempt` (when configured) — the message is skipped and an error is logged.
4. Otherwise the pipeline is executed; on success the key is committed, on failure the attempt counter is incremented and the exception is re-thrown.

### Producing idempotent messages

Implement `HasIdempotency` on the producer message and enable `ProducerIdempotencyMiddleware`:

```php
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Core\Messages\ProducerMessage;

final class ProductMessage extends ProducerMessage implements HasIdempotency
{
    public function __construct(private string $productId) {}

    public function getIdempotencyKey(): string
    {
        return $this->productId;
    }
}
```

```php
'producers' => [
    'middleware' => [
        \KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

The middleware adds the `x-idempotency-key` header to every outgoing message; consumers running `ConsumerCommiterMiddleware` with the `idempotency` repository will use it as the dedup key.

## Testing

```bash
composer test
```

### KafkaBus::fake()

The `KafkaBus` facade ships a first-class fake that works exactly like `Event::fake()` or `Mail::fake()`. Call `KafkaBus::fake()` at the start of a test to replace the real `BusInterface` binding with an in-memory `FakeBus`. From that point on every call to the facade is forwarded to the fake — `publish()` calls are intercepted and stored, consumer pipelines can be triggered directly, and the full set of assertion methods becomes available.

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

#### Asserting producer messages

```php
use App\Kafka\Messages\ProductMessage;
use KafkaBus\Laravel\Facades\KafkaBus;

it('publishes a product message', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(productId: 1);

    KafkaBus::assertPublished(ProductMessage::class);
});
```

Assert with a callback to inspect the serialised `ProducerMessage` (after the full producer pipeline, including middleware). The callback receives a `Micromus\KafkaBus\Producers\Messages\ProducerMessage` instance:

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn($msg) => str_contains($msg->payload, '"id":1')
        && isset($msg->headers['x-idempotency-key'])
);
```

Other available assertions:

```php
// Assert published exactly N times
KafkaBus::assertPublishedTimes(ProductMessage::class, 2);

// Assert a specific message was NOT published
KafkaBus::assertNotPublished(ProductMessage::class);

// Assert no messages were published at all
KafkaBus::assertNothingPublished();
```

Retrieve the published messages directly for custom assertions:

```php
// list<ProducerMessage> — serialised messages including payload, headers, topic
$messages = KafkaBus::getPublished(ProductMessage::class);
$all      = KafkaBus::allPublished();
```

#### Dispatching and asserting consumer messages

`addMessage()` queues an `RdKafka\Message` into the fake connection. Once queued, call `listen()` to run the full consumer path — `ConnectionFaker` → `ConsumerFaker` → `ConsumerStream` → consumer middleware → route middleware → handler → commit — without touching a real broker.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Laravel\Facades\KafkaBus;

it('handles a product message', function () {
    KafkaBus::fake();

    $message = MessageFactory::for()
        ->withTopicKey('products')
        ->withHeaders(['x-idempotency-key' => 'abc-123'])
        ->make('{"id":1,"name":"Widget"}');

    KafkaBus::addMessage($message);
    KafkaBus::listen('products');

    // Assert side effects produced by the handler
    expect(Product::find(1))->not->toBeNull();
});
```

Queue multiple messages before triggering `listen()`:

```php
$factory = MessageFactory::for()->withTopicKey('products');

KafkaBus::addMessage($factory->make('{"id":1}'));
KafkaBus::addMessage($factory->make('{"id":2}'));

KafkaBus::listen('products');
```

#### Asserting committed messages

After `listen()` each successfully processed message is committed into the fake connection. Use the commit assertions to verify that your handler ran and the offset was acknowledged:

```php
KafkaBus::addMessage(
    MessageFactory::for()->withTopicKey('products')->make('{"id":1}')
);

KafkaBus::listen('products');

// Assert at least one message was committed on the topic
KafkaBus::assertCommitted('products');

// Assert with a condition on the ConsumerMessageInterface
KafkaBus::assertCommitted(
    'products',
    fn($msg) => $msg->payload() === '{"id":1}'
        && $msg->headers()['x-idempotency-key'] === 'abc-123'
);

// Assert exact count
KafkaBus::assertCommittedTimes('products', 2);

// Assert nothing was committed (e.g. before listen() is called)
KafkaBus::assertNothingCommitted();
```

Retrieve committed messages directly for custom assertions:

```php
$committed = KafkaBus::getCommitted('products'); // list<ConsumerMessageInterface>

expect($committed[0]->payload())->toBe('{"id":1}');
expect($committed[0]->headers())->toHaveKey('x-idempotency-key');
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## Security Vulnerabilities

Please review [our security policy](../../security/policy) on how to report security vulnerabilities.

## Credits

- [Kirill Popkov](https://github.com/popkovkirill)
- [All Contributors](../../contributors)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
