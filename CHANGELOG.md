# Changelog

All notable changes to FlowWarden Stream Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New artifact **`flowwarden-stream-core-testkit`** (`io.flowwarden:flowwarden-stream-core-testkit`) shipping abstract behavior contracts for the SPI surface: `LockServiceContractTest`, `CheckpointStoreContractTest`, `DlqStoreContractTest`. Backend implementors (e.g. a Redis-backed `LockService`) depend on this artifact in test scope and extend the contract class to validate their implementation against the public SPI semantics.
- New artifact **`flowwarden-bom`** (`io.flowwarden:flowwarden-bom`, packaging `pom`) coordinating compatible versions of the FlowWarden ecosystem (currently `flowwarden-stream-core` and `flowwarden-stream-core-testkit`; satellite backends will be added as they ship). Import in your `dependencyManagement` to drop per-dependency `<version>` tags.
- `StreamMetricsProvider.onCheckpointFailed(streamName, cause)` — new SPI callback (default no-op) emitted when a `CheckpointStore` write throws. Use it to wire alerting on checkpoint store outages or stale-checkpoint detection in custom metrics providers.
- New `StopReason` enum in `io.flowwarden.stream.spi` (`GRACEFUL`, `CRASHED`) carried by the updated `onStreamStopped` SPI signal to distinguish operator-initiated stops from silent crashes.
- `StreamMetricsProvider.onEventDlqFailed(streamName, cause)` — new SPI callback (default no-op) emitted when a `DlqStore` write throws. Symmetric to `onCheckpointFailed`. Use it to alert on DLQ store outages where failed events are at risk of being lost without trace.

### Changed
- The repo is now a Maven multi-module reactor (parent pom + `flowwarden-stream-core` + `flowwarden-stream-core-testkit` + `flowwarden-bom`). User-facing coordinates and behavior of `flowwarden-stream-core` are unchanged — no consumer migration required. The build commands at the repo root (`./mvnw clean verify`, `./mvnw -P release deploy`) keep working unchanged thanks to the reactor.
- `StreamMetricsProvider.onCheckpoint(streamName, resumeToken)` is now emitted **after** the checkpoint write succeeds, not before. The previous timing meant the metric reported success even on failed writes, which made it unsafe for monitoring. Custom providers relying on this signal will now see accurate state — failed writes route through `onCheckpointFailed` instead.
- `StreamMetricsProvider.onEventSentToDlq(streamName)` is now emitted **only after** the `DlqStore.save(...)` write succeeds in all four code paths (imperative auto/manual and reactive auto/manual). Previously, the imperative paths and the reactive manual path emitted the metric immediately before the store call, so a store throw left the metric reporting "sent" while the event was lost. The reactive auto path was already correct via `doOnSuccess`. Failed writes now route through the new `onEventDlqFailed` signal.
- **Breaking:** `StreamMetricsProvider.onStreamStopped(String streamName)` becomes `onStreamStopped(String streamName, StopReason reason, Throwable cause)`. Custom metrics providers overriding the old signature must update. The signal is now emitted on silent container deaths (imperative) and unexpected reactive pipeline terminations, in addition to explicit `stopStream()` calls — fixing the case where a dead stream kept showing as `RUNNING` on the console.
- `LockService.tryAcquire(...)` and `LockService.renew(...)` Javadoc now explicitly documents the error contract for hot-path methods: implementations must convert transient backend errors (network timeout, primary stepdown, command timeout, etc.) into a `false` return rather than propagating exceptions. The leader election coordinator handles propagated exceptions defensively, but this is a safety net for non-conformant implementations, not the contract. The two shipped backends (`MongoLockService` in stream-core and `RedisLockService` in `flowwarden-redis`) already follow this convention — no implementation changes required.

### Removed

### Fixed
- Checkpoint write failures (e.g. `MongoWriteConcernException` on `wtimeout`, Redis command timeout, JDBC transient errors) no longer kill the imperative `MessageListenerContainer` thread nor escape the reactive pipeline. Previously, an exception thrown from the checkpoint store either silently terminated the imperative listener thread (leaving the console reporting the stream as `RUNNING` indefinitely) or escaped the reactive `doOnSuccess` side-effect and drifted the persisted token. The stream now keeps processing; the failure is logged at `WARN` and reported via the new `StreamMetricsProvider.onCheckpointFailed` signal. All three checkpoint paths are covered: post-handler `saveProcessed`, manual `ctx.saveCheckpointNow()`, and the periodic `saveSeen` timer.
- The imperative `MessageListenerContainer` thread dying on an uncaught `Throwable` (driver-level Mongo exception, internal bug, metrics provider crash, etc.) no longer leaves the console reporting the stream as `RUNNING` indefinitely. Stream-core wraps the listener and emits `onStreamStopped(streamName, CRASHED, cause)` before the exception propagates to Spring.
- The reactive pipeline terminating on an unexpected `onError` (when something downstream of `onErrorResume` itself fails) or `onComplete` (which should never happen on an infinite change stream) is now reported via the same `onStreamStopped(streamName, CRASHED, cause)` signal. Explicit `stopStream()` calls remain reported as `GRACEFUL`.
- DLQ store write failures (e.g. `MongoWriteConcernException`, Redis/JDBC backend errors) are now reported via the new `onEventDlqFailed` SPI signal instead of being silently logged. Previously, an `onEventSentToDlq` was emitted prematurely on the imperative auto/manual paths and the reactive manual path, so a DLQ store outage looked like success in monitoring while events were actually lost.
- Per-stream state (`streams`, `lastActivityTimes`, `eventCounters`) is now evicted from both stream managers when the stream dies on an uncaught throwable, not only on explicit `stopStream()`. Public APIs `isRunning(streamName)` and `getLastEventTime(streamName)` previously kept reporting `true` / a stale timestamp after a silent crash even though `onStreamStopped(CRASHED, cause)` had been emitted &mdash; finishing the work started by the `onStreamStopped(CRASHED)` signal so the observable state matches reality.
- `LeaderElectionCoordinator` heartbeat now treats a `LockService.renew(...)` thrown exception the same as `renew(...)` returning `false` &mdash; both paths trigger leadership loss (cancel heartbeat, transition to `STANDBY`, invoke `onLostLeadership`, re-enter standby polling). Previously, propagated exceptions were only logged at `WARN` while the heartbeat kept running, leaving the instance convinced it was still leader even after the underlying lock store had expired its lease &mdash; a latent **double-leader** risk for any `LockService` implementation that propagated exceptions instead of converting them to `false`. The two shipped backends (`MongoLockService` in stream-core, `RedisLockService` in `flowwarden-redis`) already convert transient errors to `false`, so no observable behavior change for them; this fix protects against future or third-party backends that may not follow the convention.

### Deprecated

### Security

## [1.0.0-rc.2] — 2026-06-03

### Added
- `CheckpointStore.saveSeen(streamName, token, ts)` and `CheckpointStore.saveProcessed(streamName, token, ts)` SPI methods for targeted token writes. The default implementation delegates to `findByStreamName + save` so external store implementations continue to work unchanged. `MongoCheckpointStore` and `ReactiveMongoCheckpointStore` override with a native `upsert` that targets only the relevant pair of fields, avoiding a hot-path read.
- 3-level resume cascade for `@Checkpoint`: `lastProcessedToken` → `lastSeenToken` → `onHistoryLost`. When the saved `lastProcessedToken` has aged out of the MongoDB oplog, the stream now falls back to `lastSeenToken` (advanced by the `saveIntervalSeconds` timer) before escalating to the `onHistoryLost` strategy. Emits a `WARN` log and a `flowwarden.stream.resume.fallback_to_seen` counter on level-2 fallback.
- Startup validation: `@Checkpoint(saveEveryN < 1)` is now rejected with a clear error.
- Startup warning: `@Checkpoint(saveIntervalSeconds = 0, saveEveryN > 1)` logs a warning (the combination disables the resume cascade level-2 safety net).
- `StreamMetricsProvider` SPI: new default methods `onResumeFallbackToSeen(streamName)`, `onResumeFallbackToProcessed(streamName)`, `onResumeHistoryLost(streamName)`, and `onCheckpointLag(streamName, lagSeconds, lagEvents)` for monitoring the dual-token model.
- `@Checkpoint(resumeStrategy = …)` and new `ResumeStrategy` enum (`PROCESSED_FIRST` default, `SEEN_FIRST`). `PROCESSED_FIRST` preserves the existing strict at-least-once behavior. `SEEN_FIRST` makes the resume cascade start from the heartbeat-fresh `lastSeenToken` instead of `lastProcessedToken`, trading re-delivery of in-flight events for fast restart on low-volume or filter-heavy streams. `lastProcessedToken` remains the cascade fallback before `onHistoryLost`. Emits `flowwarden.stream.resume.fallback_to_processed` on level-2 fallback.
- `@Checkpoint` and `@Filter` Javadoc updated to describe the dual-token model explicitly.
- `LockService` SPI in `io.flowwarden.stream.spi` for distributed lock backends supporting `DeploymentMode.SINGLE_LEADER`. Five methods — `tryAcquire`, `renew`, `release`, `getLockState`, `getCurrentLeader` — passing `instanceId` and `ttl` per call so the SPI is fully stateless and impl-agnostic. New `LockState` record exposed for Reporter / Console introspection. Default `MongoLockService` (backed by the `_fw_locks` collection) is now registered via `@ConditionalOnMissingBean LockService`, allowing user replacement with a Redis / Consul / JDBC implementation.
- `@MongoDlqOptions` annotation in `io.flowwarden.stream.annotation` for MongoDB-specific per-stream DLQ tuning (currently `collection`). Used together with `@DeadLetterQueue` when the user wants to route failed events for a given stream to a non-default Mongo collection.
- `flowwarden.dlq.mongo.collection` configuration property (default `_fw_dlq`) for the backend-level default collection used by streams without `@MongoDlqOptions`. Bound via `MongoDlqProperties`.
- `DlqPolicy` SPI record (`retentionDays`, `includeOriginalDocument`, `includeStackTrace`) in `io.flowwarden.stream.spi`. Built from `@DeadLetterQueue` and passed to `DlqStore.save` so custom impls can honour cross-cutting policy without parsing annotations themselves.
- `TransactionInfo` record in `io.flowwarden.stream` and `ChangeStreamContext.getTransactionInfo()` accessor. Exposes MongoDB transaction metadata (`lsid`, `txnNumber`) for handlers that need to group events of the same transaction — audit logs, aggregation, atomicity-preserving downstream propagation. Returns `Optional.empty()` for non-transactional operations. Additive SPI surface, backward compatible.

### Changed
- **Breaking:** `@DeadLetterQueue` is now backend-agnostic. The `collection` attribute is removed (moved to `@MongoDlqOptions`) and `ttlDays` is renamed `retentionDays` to reflect that each backend translates retention to its native mechanism (Mongo TTL index, Kafka `retention.ms`, Rabbit `x-message-ttl`, JDBC scheduled cleanup).
- **Breaking:** `DlqStore.save(FailedEvent)` is now `DlqStore.save(FailedEvent, DlqPolicy)`. Custom implementations must accept the policy argument; the cross-cutting policy is no longer hidden behind an internal annotation parser.
- **Breaking:** `StreamConfiguration.DlqConfig` exposes `retentionDays` instead of `ttlDays`, and no longer exposes `collection` (backend-specific tuning is out of scope for the SPI snapshot).
- Internal `DeadLetterQueueConfig` removed in favour of the public `DlqPolicy` record. `MongoDlqStore` and `ReactiveMongoDlqStore` now take `MongoDlqProperties` and expose `registerStream(streamName, collection)` so the runtime can bind per-stream collections at startup; unregistered streams fall through to the configured default.

### Removed
- **Breaking:** `@OnChange.operationTypes` attribute. `@OnChange` is now attribute-less and acts as a pure catch-all — it fires on every operation not covered by a typed handler (`@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnReplace`) in the same class, including `DROP` and `INVALIDATE`. To handle a specific subset of operation types, use two typed handlers delegating to a shared private method.
- Boot-time validation rejecting `@Filter` combined with an unrestricted `@OnChange`. With the catch-all semantics, users are expected to handle `Optional.empty()` from `ChangeStreamContext.getFullDocument()` in their filter predicate, or use a server-side `@Pipeline` to scope the events.
- **Breaking:** `DeploymentMode.PARTITIONED` enum value. The mode was defined but rejected at boot with a `BeanCreationException` ("not yet implemented") — no production application could have used it. Atomicity of MongoDB transactions, same-document event causality, watermark coordination and rebalancing are all open problems whose proper solution is out of scope for 1.0. The remaining values `ALL_INSTANCES` (default) and `SINGLE_LEADER` cover the supported deployment topologies. Users who need horizontal scaling should rely on MongoDB sharding upstream, multiple `@ChangeStream` classes on different collections, or future sink modules (Kafka, RabbitMQ) emitting to brokers with consumer-group parallelism.

### Fixed
- The `saveIntervalSeconds` heartbeat timer was writing both `lastSeenToken` AND `lastProcessedToken` with the same value, breaking the documented at-least-once delivery guarantee: a crash mid-handler could lose the event being processed because `lastProcessedToken` had already advanced past it on the previous timer tick. The timer now advances only `lastSeenToken`; `lastProcessedToken` advances only after confirmed handler success.
- MongoDB TTL index on `expiresAt` is now created at startup for every collection bound to a `@DeadLetterQueue`-annotated stream. Previously the DLQ collection had no TTL index, so `retentionDays` had no effect — entries accumulated indefinitely until manual cleanup.
- `saveCheckpointIfNeeded` in both the imperative and reactive stream managers was still using the legacy `CheckpointStore.save(Checkpoint)` SPI method with the same token in `lastSeenToken` and `lastProcessedToken`, overwriting the heartbeat timer's `saveSeen(...)` work on every successful handler invocation. This masked the dual-token divergence that the resume cascade relies on — both tokens collapsed to the same value after any handler success, so the cascade level-2 fallback to `lastSeenToken` never reflected the actual heartbeat advance. The managers now call `CheckpointStore.saveProcessed(streamName, token, timestamp)` — the targeted method introduced for exactly this case.

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

[Unreleased]: https://github.com/flowwarden-io/flowwarden-stream-core/compare/v1.0.0-rc.2...HEAD
[1.0.0-rc.2]: https://github.com/flowwarden-io/flowwarden-stream-core/releases/tag/v1.0.0-rc.2
[1.0.0-rc.1]: https://github.com/flowwarden-io/flowwarden-stream-core/releases/tag/v1.0.0-rc.1
