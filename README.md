<p align="center">
  <strong>FlowWarden Stream Core</strong><br/>
  Declarative MongoDB Change Streams for Spring Boot — resilient, annotation-driven, zero-config.
</p>

<p align="center">
  <a href="https://github.com/flowwarden-io/flowwarden-stream-core/actions/workflows/ci.yml"><img src="https://github.com/flowwarden-io/flowwarden-stream-core/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html"><img src="https://img.shields.io/badge/Java-17%2B-orange.svg" alt="Java 17+"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring_Boot-3.x-brightgreen.svg" alt="Spring Boot 3.x"></a>
  <a href="https://central.sonatype.com/artifact/io.flowwarden/flowwarden-stream-core"><img src="https://img.shields.io/maven-central/v/io.flowwarden/flowwarden-stream-core.svg" alt="Maven Central"></a>
</p>

---

## What is FlowWarden?

FlowWarden Stream Core is an **open source** (Apache 2.0) Java library that brings **declarative, annotation-driven MongoDB Change Streams** to Spring Boot applications. Inspired by frameworks like Mongock, it lets you define Change Stream handlers with simple annotations instead of boilerplate infrastructure code.

The library automatically handles **checkpoint/resume**, **retry with exponential backoff**, **dead letter queues**, and **leader election** — so you can focus on your event-processing logic. It supports both **Spring MVC** (imperative) and **Spring WebFlux** (reactive) programming models out of the box.

## Features

- **`@ChangeStream` declarative handlers** — annotate a class, watch a collection
- **Typed event handlers** — `@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnChange`
- **Automatic checkpoint & resume** — MongoDB-backed resume tokens survive restarts
- **Dead Letter Queue** — failed events are routed to a configurable DLQ with retention policies
- **Exponential backoff retry** — `@RetryPolicy` with configurable max attempts and delays
- **Server-side filtering** — `@Pipeline` pushes aggregation pipelines down to MongoDB
- **Application-side filtering** — `@Filter` runs Java predicates on each event before dispatching to handlers
- **Dual execution mode** — `IMPERATIVE` (`MongoTemplate`) or `REACTIVE` (`ReactiveMongoTemplate`)
- **Multi-instance deployment** — `SINGLE_LEADER`, `ALL_INSTANCES` strategies
- **Zero additional dependencies** — uses only Spring Data MongoDB (already in your classpath)
- **Spring Boot auto-configuration** — just add the dependency and annotate

## Quick Start

### 1. Add the dependency

**Maven**
```xml
<dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-stream-core</artifactId>
    <version>1.0.0-rc.4</version>
</dependency>
```

**Gradle**
```groovy
implementation 'io.flowwarden:flowwarden-stream-core:1.0.0-rc.4'
```

**Optional — using the FlowWarden BOM** *(available from `1.0.0-rc.3` onward)*. Handy if you also pull `flowwarden-stream-core-testkit` or a satellite backend (`flowwarden-javers`, `flowwarden-redis`, `flowwarden-amqp`), so versions stay aligned:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.flowwarden</groupId>
      <artifactId>flowwarden-bom</artifactId>
      <version>1.0.0-rc.4</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-stream-core</artifactId>
  </dependency>
</dependencies>
```

### 2. Configure your application

```yaml
# application.yml
flowwarden:
  default-mode: IMPERATIVE   # optional — defaults to IMPERATIVE if omitted

spring:
  data:
    mongodb:
      uri: mongodb://localhost:27017/mydb   # Must be a Replica Set
```

### 3. Enable FlowWarden and create a handler

```java
@SpringBootApplication
@EnableFlowWarden
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}


@ChangeStream(name = "order-watcher", collection = "orders")
public class OrderHandler {

    @OnChange
    void handle(ChangeStreamContext<?> ctx) {
        System.out.printf("Event %s on %s%n",
            ctx.getOperationType(), ctx.getCollectionName());
    }
}
```

That's it. Start your app and every insert, update, replace, or delete on the `orders` collection triggers your handler.

## Comprehensive Example

```java
@ChangeStream(
    name              = "order-stream",
    collection        = "orders",
    documentType      = Order.class,
    fullDocument      = FullDocumentMode.UPDATE_LOOKUP,
    deploymentMode    = DeploymentMode.SINGLE_LEADER
)
@Checkpoint(saveEveryN = 10, saveIntervalSeconds = 10)
@RetryPolicy(maxAttempts = 5, initialDelay = "500ms", multiplier = 2.0)
@DeadLetterQueue(retentionDays = 30)
@MongoDlqOptions(collection = "orders_dlq")
public class OrderStreamHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderStreamHandler.class);

    /** Server-side: only matching events reach the application. */
    @Pipeline
    List<Bson> pipeline() {
        return List.of(
            Aggregates.match(Filters.in("fullDocument.status", "PAID", "SHIPPED", "CANCELLED"))
        );
    }

    /** Application-side: refine with Java logic (e.g., service calls). */
    @Filter
    boolean filter(ChangeStreamContext<Order> ctx) {
        return ctx.getFullDocument(Order.class).getTotal() > 0;
    }

    @OnInsert
    void onNewOrder(ChangeStreamContext<Order> ctx) {
        Order order = ctx.getFullDocument(Order.class);
        log.info("New order created: {} — total: {}", order.getId(), order.getTotal());
    }

    @OnUpdate
    void onOrderUpdated(ChangeStreamContext<Order> ctx) {
        UpdateDescription update = ctx.getUpdateDescription();
        if (update.hasFieldChanged("status")) {
            String newStatus = update.getUpdatedFieldValue("status", String.class);
            log.info("Order {} status changed to {}", ctx.getDocumentKey(), newStatus);
        }
    }

    @OnDelete
    void onOrderDeleted(ChangeStreamContext<Order> ctx) {
        log.info("Order deleted: {}", ctx.getDocumentKey());
    }

    @OnError
    ErrorAction onProcessingError(Throwable error, ChangeStreamContext<?> ctx) {
        log.error("Failed to process order event {}: {}",
            ctx.getEventId(), error.getMessage());
        return ErrorAction.DLQ;
    }
}
```

## Handler Signatures

Handler methods annotated with `@OnChange`, `@OnInsert`, `@OnUpdate`, `@OnReplace`, or `@OnDelete` support the following signatures:

| Signature | Mode | Description |
|-----------|------|-------------|
| `void handle(ChangeStreamContext<T> ctx)` | Imperative | Blocking handler, full event context |
| `void handle(T document)` | Imperative | Typed-document only, when you don't need event metadata |
| `void handle(T document, ChangeStreamContext<T> ctx)` | Imperative | Both typed document and event context |
| `Mono<Void> handle(ChangeStreamContext<T> ctx)` | Reactive | Non-blocking reactive handler |

For other Spring beans (e.g., a `MongoTemplate`), inject them via the handler class constructor or `@Autowired` field — they are then available from within any handler method.

## Deployment Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `ALL_INSTANCES` | Every instance receives every event independently. No coordination. | **Default.** Cache invalidation, local state refresh, read-model projections. |
| `SINGLE_LEADER` | One active instance processes events; others are on standby. Automatic leader election via MongoDB distributed locks (`_fw_locks`). | Order processing, billing, notifications — anything requiring exactly-once semantics. |

## Compatibility

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| Java | 17 | 21 |
| Spring Boot | 3.2.x | 3.2.x+ |
| Spring Data MongoDB | 4.2.x (via Boot BOM) | 4.2.x+ |
| MongoDB Server | 6.0 | 7.0+ |
| MongoDB Java Driver | 4.11.x (via Boot BOM) | 4.11.x+ |

> **Important:** MongoDB **must** be configured as a [Replica Set](https://www.mongodb.com/docs/manual/replication/) for Change Streams to work. This applies to both production and development environments. Testcontainers automatically provisions a single-node Replica Set for testing.

## Configuration

The only `application.yml` property is the execution mode:

```yaml
flowwarden:
  default-mode: IMPERATIVE   # IMPERATIVE or REACTIVE (defaults to IMPERATIVE)
```

All stream-level settings are configured via annotations on your `@ChangeStream` classes:

| Annotation | Purpose | Key defaults |
|------------|---------|--------------|
| `@Checkpoint` | Durable anchor persistence (at-least-once resume) | `saveEveryN = 1` (persist after N settled events), `saveIntervalSeconds = 5` (max age of an unpersisted anchor — whichever threshold is hit first), `idleHeartbeatIntervalSeconds = 300` (idle-stream oplog-rollover protection, `0` opts out), `startPosition = RESUME` |
| `@RetryPolicy` | Exponential backoff on failure | `maxAttempts = 3`, `initialDelay = "500ms"`, `multiplier = 2.0`, `maxDelay = "30s"`, `jitter = true` |
| `@DeadLetterQueue` | Route failed events to a DLQ (backend-agnostic) | `enabled = true`, `retentionDays = 30` |
| `@MongoDlqOptions` | MongoDB-specific DLQ tuning (collection override) | `collection = "_fw_dlq"` (overrides `flowwarden.dlq.mongo.collection`) |

See the [Comprehensive Example](#comprehensive-example) above for usage, or the [documentation](https://docs.flowwarden.io) for the full reference.

### When history is lost

If a stream stays down (or its resume point cannot be maintained) longer than the MongoDB oplog window, its saved position expires and the `@Checkpoint(onHistoryLost = …)` strategy applies:

- **`FAIL`** (default) — a terminal stop: the stream refuses to start until an operator either deletes the stream's document from the checkpoint collection (`_fw_checkpoints` with the built-in Mongo stores) to restart from a fresh position, or switches the strategy. Choose it when losing events must never go unnoticed.
- **`RESUME_FROM_NOW`** — the recovery abandons the lost history explicitly: a fresh server-certified position is persisted (clearing the expired tokens) and the stream resumes from it. Recommended for rebuildable projections (search models, caches) — reindex the gap after recovery.
- **`RESUME_FROM_OPLOG_START`** — replays whatever history is still readable from the oldest oplog entry. Inherently racy on a tight oplog; falls back to the `RESUME_FROM_NOW` behavior if the oplog boundary cannot be read.


## FlowWarden Ecosystem

| Component | Description | License |
|-----------|-------------|---------|
| **[flowwarden-stream-core](https://github.com/flowwarden-io/flowwarden-stream-core)** | Declarative MongoDB Change Streams library for Spring Boot | Apache 2.0 |
| **[flowwarden-javers](https://github.com/flowwarden-io/flowwarden-javers)** | Native Javers audit stream integration | Apache 2.0 |
| **[flowwarden-redis](https://github.com/flowwarden-io/flowwarden-redis)** | Redis-backed `LockService` and `CheckpointStore` backends | Apache 2.0 |
| **[flowwarden-amqp](https://github.com/flowwarden-io/flowwarden-amqp)** | AMQP (RabbitMQ) publish-only dead-letter queue store | Apache 2.0 |
| **flowwarden-rabbit-streams** | RabbitMQ Streams-backed dead-letter queue store | Apache 2.0 |
| **flowwarden-reporter** | Connects your streams to FlowWarden Console for monitoring | Apache 2.0 |
| **FlowWarden Console** | Dashboard for monitoring, alerting, and managing Change Streams | Commercial |

## Documentation

Full documentation is available at **[docs.flowwarden.io](https://docs.flowwarden.io)** — including guides, API reference, and architecture deep dives.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get involved.

## License

FlowWarden Stream Core is licensed under the [Apache License, Version 2.0](LICENSE).

```
Copyright 2026 FlowWarden

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
