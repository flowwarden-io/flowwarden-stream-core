# Changelog

All notable changes to FlowWarden Stream Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `CheckpointStore.saveSeen(streamName, token, ts)` and `CheckpointStore.saveProcessed(streamName, token, ts)` SPI methods for targeted token writes. The default implementation delegates to `findByStreamName + save` so external store implementations continue to work unchanged. `MongoCheckpointStore` and `ReactiveMongoCheckpointStore` override with a native `upsert` that targets only the relevant pair of fields, avoiding a hot-path read. Foundation for upcoming strict separation of `lastSeenToken` and `lastProcessedToken` writes.

## [1.0.0-rc.1] — 2026-05-31

First release candidate of FlowWarden Stream Core.

### Added

#### Programming model
- `@ChangeStream` annotation to declare a Change Stream handler on a MongoDB collection
- Typed event handlers: `@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnReplace`, `@OnChange`
- `ChangeStreamContext<T>` providing access to the document, update description, resume token, and operation type
- Fail-fast validation at startup via `BeanPostProcessor` — invalid configurations fail at boot, not at runtime

#### Dual-mode execution
- Imperative mode using `MongoTemplate` (Spring MVC)
- Reactive mode using `ReactiveMongoTemplate` (Spring WebFlux)
- Single annotation API works transparently across both modes
- Auto-configuration selects the appropriate manager based on classpath

#### Filtering
- `@Pipeline` — server-side MongoDB aggregation filtering
- `@Filter` — application-side Java predicate filtering
- Composable: both can coexist on the same stream (server-side then application-side)

#### Resilience
- `@Checkpoint` — resume token persistence with configurable save interval
- `CheckpointStore` SPI with default MongoDB-backed implementation
- Dual checkpoint system (`lastSeenToken` / `lastProcessedToken`) to avoid oplog rescanning at restart
- `@RetryPolicy` — exponential backoff retry with configurable attempts, initial delay, multiplier
- `@DeadLetterQueue` — failed event routing with TTL retention
- `DlqStore` SPI with default MongoDB-backed implementation
- `@OnError` — custom exception handling with `ErrorAction` (`SKIP`, `RETRY`, `DLQ`, `RETHROW`)
- Multi-instance deployment via `DeploymentMode` (`ALL_INSTANCES`, `SINGLE_LEADER` with MongoDB-backed leader election via `_fw_locks`)

#### Observability
- Spring Boot Actuator endpoint at `/actuator/flowwarden`
- `StreamMetricsProvider` SPI for custom metrics integration

#### Build & quality
- Apache 2.0 license headers on all source files (enforced by `license-maven-plugin`)
- CI matrix: Java 17 and Java 21
- Integration tests using Testcontainers with MongoDB Replica Set

### Known limitations

- `DeploymentMode.PARTITIONED` — defined in the enum but not yet implemented, reserved for a future release
- Watchdog / zombie stream detection — planned for a future release

[Unreleased]: https://github.com/flowwarden-io/flowwarden-stream-core/compare/v1.0.0-rc.1...HEAD
[1.0.0-rc.1]: https://github.com/flowwarden-io/flowwarden-stream-core/releases/tag/v1.0.0-rc.1
