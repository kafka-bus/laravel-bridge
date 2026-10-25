# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
composer test                    # run all tests
vendor/bin/pest tests/Path/File  # run a single test file
composer analyse                 # PHPStan static analysis
composer format                  # Laravel Pint code style fixer
composer lint                    # Pint + PHPStan together
```

## Architecture

This is a Laravel package (`kafka-bus/laravel-bridge`) that wires [`kafka-bus/kafka-bus`](vendor/kafka-bus/kafka-bus) 2.x into a Laravel application. The monorepo package replaces `core`, `commiter`, `messages`, `worker` and `metadata`, and contains all Kafka logic. This repo only provides:

- Service providers, facade, artisan commands, and factories that adapt core to Laravel's container and config system.
- A `FakeBus` / testing layer (`src/Testing/`) that wraps core's `BusFaker`, mirroring `Event::fake()`.
- An optional **Commiter** component (`src/Components/Commiter/`) for idempotent message tracking via the database/redis.

### How core 2.x works (what the bridge relies on)

- `Bus` is bound to **one** connection (`kafka-bus.default`); it publishes via `PublisherFactory` (routes: message class → topic) and receives via `ReceiverInterface::dispatch()` (routes: **topic name** → handler + middleware). There is no per-worker routing in the bus.
- A `Worker` (`kafka-bus/worker`) only knows its topics and consumer options. `WorkerRunnerFactory::create($bus, $names)` reads raw messages from `$bus->connection()` and feeds them to `$bus->dispatch()`.
- Offsets (`kafka:offset:*`) go through `kafka-bus/metadata` (`Metadata::fromConnection()->partitions($topics, $consumerConfig)`).

### Service providers

| Provider | Responsibility |
|---|---|
| `KafkaBusServiceProvider` | Binds `BusInterface`, `ConnectionRegistryInterface`, `ConnectionInterface` (default connection), `ReceiverInterface` (`LazyReceiver`), `WorkerRegistryInterface`, `WorkerRunnerFactory`, `TopicRegistry`, `PublisherFactory` into the container; registers all artisan commands. |
| `CommiterServiceProvider` | Binds `RepositorySourceInterface` and `ConsumerMessageRepositoryInterface` for idempotency tracking; publishes config + migration. |

Both are auto-discovered via `composer.json` `"extra.laravel.providers"`.

### Config-to-container flow

`config/kafka-bus.php` drives the entire object graph:

- `TopicRegistryFactory` → `TopicRegistry` (logical key → prefixed physical topic name)
- `WorkerFactory` → resolves a worker (`consumers.workers.*`: `topics` key list or the topic named like the worker; auto-created from `consumers.routes` when not configured) into a `WorkerDefinition` (topic keys + merged options) and builds `KafkaBus\Worker\Worker`; `WorkerRegistry` caches them (returns `null` for unknown names).
- `ReceiverRoutesFactory` + `Receivers\RouteFactory` → `RouteCollection<Receiver\Route>` from `consumers.routes`, one lazy route per topic. Middleware order: global `consumers.middleware` + route middleware.
- `PublisherRoutesFactory` / `PublisherRouteFactory` → `RouteCollection<Publisher\Route>` (message class → topic)
- `ConnectionRegistryFactory` → `ConnectionRegistryInterface` using `DriverConnectionFactory` / `NullDriverConnectionFactory`
- `Offsets\PartitionsFactory` → `Partitions` for consumed topics (`consumers.routes`) in the global consumer group; offset commands do not depend on workers

### Commands

| Command | Class |
|---|---|
| `kafka:consume {workerName*}` | `KafkaConsumeCommand` |
| `kafka:consume:routes {--expect=*}` | `KafkaConsumeRoutesCommand` |
| `kafka:commit` | `KafkaCommitCommand` |
| `kafka:commit:purge` | `KafkaCommitPurgeCommand` |
| `kafka:offset:show {topicKey?}` | `KafkaConsumerOffsetShowCommand` |
| `kafka:offset:set {topicKey} {offset}` | `KafkaConsumerOffsetSetCommand` |
| `kafka:consume:route:list` | `KafkaConsumerRouteListCommand` |
| `kafka:worker:list` | `KafkaWorkerListCommand` |
| `kafka:publisher:route:list` | `KafkaPublisherRouteListCommand` |

`KafkaConsumeCommand` and `KafkaConsumeRoutesCommand` share `AbstractKafkaConsumeCommand`, which calls `$runnerFactory->create($bus, $workerNames)->run()` and catches `ConsumerException` for `EXIT_FAILURE`.

### Testing layer

`KafkaBus::fake()` binds `FakeBus` (wrapping core's `BusFaker`, PHPUnit assertion driver) as `BusInterface` and its `ConnectionFaker` as `ConnectionInterface`. Everything not defined on `FakeBus` is forwarded to `BusFaker` via `__call`. After that:

- `KafkaBus::addMessage(Message $msg)` — queues an `RdKafka\Message` into `ConnectionFaker`.
- `KafkaBus::listen(string|array $workerName)` — runs the full consumer pipeline (stream → receiver middleware → handler → commit) without a real broker via `WorkerRunnerFactory`.
- `KafkaBus::assertPublished/assertCommitted/assertDispatched/assertNothingPublished/…` — assertions on the in-memory state.

`MessageFactory::for()->withTopicKey('products')->make('payload')` creates `RdKafka\Message` instances for tests.

Artisan commands (`kafka:consume`, `kafka:consume:routes`) under `KafkaBus::fake()` resolve the `FakeBus`, so the `WorkerRunnerFactory` reads from `ConnectionFaker`. The `ConsumerStream` exits cleanly once all queued messages are consumed (`KafkaMessagesEndedException` is caught internally).

For testing `ConsumerException` failure paths in commands, mock `WorkerRunnerFactory` directly with Mockery:

```php
$factory = Mockery::mock(WorkerRunnerFactory::class);
$factory->shouldReceive('create')->andThrow(new ConsumerException('error'));
$this->app->instance(WorkerRunnerFactory::class, $factory);
```

### Test setup

All tests extend `KafkaBus\Laravel\Tests\TestCase` (Orchestra Testbench). The `Pest.php` file applies `TestCase` globally. Topic names in tests are prefixed with `testing.` (because `APP_ENV=testing`).