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

This is a Laravel package (`kafka-bus/laravel-bridge`) that wires [`kafka-bus/core`](vendor/kafka-bus/core) into a Laravel application. It does **not** contain core Kafka logic — that lives in the vendored `core` package. This repo only provides:

- Service providers, facade, artisan commands, and factories that adapt core to Laravel's container and config system.
- A `FakeBus` / testing layer (`src/Testing/`) that mirrors `Event::fake()`.
- An optional **Commiter** component (`src/Components/Commiter/`) for idempotent message tracking via the database.

### Service providers

| Provider | Responsibility |
|---|---|
| `KafkaBusServiceProvider` | Binds `BusInterface`, `ConnectionRegistryInterface`, `ThreadRegistry`, `TopicRegistry`, `PublisherFactory`, `ListenerFactory` into the container; registers all artisan commands. |
| `CommiterServiceProvider` | Binds `RepositorySourceInterface` and `ConsumerMessageRepositoryInterface` for idempotency tracking; publishes config + migration. |

Both are auto-discovered via `composer.json` `"extra.laravel.providers"`.

### Config-to-container flow

`config/kafka-bus.php` drives the entire object graph. The factories in `src/Factories/` and `src/Listeners/` and `src/Publishers/` translate config arrays into core objects:

- `TopicRegistryFactory` → `TopicRegistry` (logical key → prefixed physical topic name)
- `WorkerFactory` + `LaravelWorkerRegistry` → builds `Worker` objects (topics, handlers, middleware, options)
- `LaravelPublisherRoutesFactory` → builds producer `PublisherRoutes` (message class → topic key)
- `ConnectionRegistryFactory` → `ConnectionRegistryInterface` using `DriverConnectionFactory` / `NullDriverConnectionFactory`

### Commands

| Command | Class |
|---|---|
| `kafka:consume {workerName*}` | `KafkaConsumeCommand` |
| `kafka:consume:all {--expect=*}` | `KafkaConsumeAllCommand` |
| `kafka:commit` | `KafkaCommitCommand` |
| `kafka:offset:show` | `KafkaOffsetShowCommand` |
| `kafka:offset:set` | `KafkaOffsetSetCommand` |
| `kafka:worker:list` | `KafkaWorkerListCommand` |
| `kafka:route:list` | `KafkaRouteListCommand` |

`KafkaConsumeCommand` and `KafkaConsumeAllCommand` share `AbstractKafkaConsumeCommand`, which calls `$bus->listener($workerNames)->listen()` and catches `ConsumerException` for `EXIT_FAILURE`.

### Testing layer

`KafkaBus::fake()` replaces `BusInterface` in the container with `FakeBus` and replaces `ConnectionRegistryInterface` with `ConnectionRegistryFaker`. After that:

- `KafkaBus::addMessage(Message $msg)` — queues an `RdKafka\Message` into `ConnectionFaker`.
- `KafkaBus::listen(string $workerName)` — runs the full consumer pipeline (middleware → handler → commit) without a real broker. Takes a **single string** worker name.
- `KafkaBus::assertPublished/assertCommitted/assertNothingPublished/…` — PHPUnit assertions on the in-memory state.

`MessageFactory::for()->withTopicKey('products')->make('payload')` creates `RdKafka\Message` instances for tests.

When testing artisan commands (`kafka:consume`, `kafka:consum:all`) with `KafkaBus::fake()`, the command calls `$bus->listener(array $workerNames)`. `FakeBus::listener()` accepts `string|array` and delegates to the underlying real bus (which uses `ConnectionRegistryFaker`). The `ConsumerStream` exits cleanly once all queued messages are consumed (`KafkaMessagesEndedException` is caught internally).

For testing `ConsumerException` failure paths in commands, mock `BusInterface` directly with Mockery:

```php
$bus = Mockery::mock(BusInterface::class);
$bus->shouldReceive('listener')->andThrow(new ConsumerException('error'));
$this->app->instance(BusInterface::class, $bus);
```

### Test setup

All tests extend `KafkaBus\Laravel\Tests\TestCase` (Orchestra Testbench). The `Pest.php` file applies `TestCase` globally. Topic names in tests are prefixed with `testing.` (because `APP_ENV=testing`).