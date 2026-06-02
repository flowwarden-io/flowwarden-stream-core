# Changelog

All notable changes to FlowWarden Stream Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `CheckpointStore.saveSeen(streamName, token, ts)` and `CheckpointStore.saveProcessed(streamName, token, ts)` SPI methods for targeted token writes. The default implementation delegates to `findByStreamName + save` so external store implementations continue to work unchanged. `MongoCheckpointStore` and `ReactiveMongoCheckpointStore` override with a native `upsert` that targets only the relevant pair of fields, avoiding a hot-path read.
- 3-level resume cascade for `@Checkpoint`: `lastProcessedToken` → `lastSeenToken` → `onHistoryLost`. When the saved `lastProcessedToken` has aged out of the MongoDB oplog, the stream now falls back to `lastSeenToken` (advanced by the `saveIntervalSeconds` timer) before escalating to the `onHistoryLost` strategy. Emits a `WARN` log and a `flowwarden.stream.resume.fallback_to_seen` counter on level-2 fallback.
- Startup validation: `@Checkpoint(saveEveryN < 1)` is now rejected with a clear error.
- Startup warning: `@Checkpoint(saveIntervalSeconds = 0, saveEveryN > 1)` logs a warning (the combination disables the resume cascade level-2 safety net).
- `StreamMetricsProvider` SPI: new default methods `onResumeFallbackToSeen(streamName)`, `onResumeHistoryLost(streamName)`, and `onCheckpointLag(streamName, lagSeconds, lagEvents)` for monitoring the dual-token model.
- `@Checkpoint` and `@Filter` Javadoc updated to describe the dual-token model explicitly.
- `LockService` SPI in `io.flowwarden.stream.spi` for distributed lock backends supporting `DeploymentMode.SINGLE_LEADER`. Five methods — `tryAcquire`, `renew`, `release`, `getLockState`, `getCurrentLeader` — passing `instanceId` and `ttl` per call so the SPI is fully stateless and impl-agnostic. New `LockState` record exposed for Reporter / Console introspection. Default `MongoLockService` (backed by the `_fw_locks` collection) is now registered via `@ConditionalOnMissingBean LockService`, allowing user replacement with a Redis / Consul / JDBC implementation.

### Removed
- **Breaking:** `@OnChange.operationTypes` attribute. `@OnChange` is now attribute-less and acts as a pure catch-all — it fires on every operation not covered by a typed handler (`@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnReplace`) in the same class, including `DROP` and `INVALIDATE`. To handle a specific subset of operation types, use two typed handlers delegating to a shared private method.
- Boot-time validation rejecting `@Filter` combined with an unrestricted `@OnChange`. With the catch-all semantics, users are expected to handle `Optional.empty()` from `ChangeStreamContext.getFullDocument()` in their filter predicate, or use a server-side `@Pipeline` to scope the events.

### Fixed
- The `saveIntervalSeconds` heartbeat timer was writing both `lastSeenToken` AND `lastProcessedToken` with the same value, breaking the documented at-least-once delivery guarantee: a crash mid-handler could lose the event being processed because `lastProcessedToken` had already advanced past it on the previous timer tick. The timer now advances only `lastSeenToken`; `lastProcessedToken` advances only after confirmed handler success.

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
