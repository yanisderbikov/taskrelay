# TaskRelay: notes for Claude

This repository is the TaskRelay library itself: a transactional outbox and durable background tasks for Spring Boot on PostgreSQL. Instructions for **using** the library in an application live in `claude-plugin/skills/taskrelay/SKILL.md`. Keep that file in sync whenever the public API or the defaults change.

## Build and test

- `mvn verify` runs all tests. The integration tests start `postgres:16-alpine` through Testcontainers, so Docker must be running.
- The source and target level is Java 17, built against Spring Boot 3.2.0. Do not use newer Java language features or Spring APIs added after Boot 3.2.
- JitPack builds releases from git tags using `jitpack.yml` (JDK 17, tests skipped). The version in `pom.xml` must match the tag.

## Layout

Everything is in `io.github.yanisderbikov.taskrelay`. Public API:

| Class | Role |
|---|---|
| `TaskQueue` | Enqueue, find, retry. The only bean applications inject. |
| `TaskHandler<P>` | Base class of application handlers: type, payload class, `handle`, retry policy hooks, `delay`, `dedicatedThread`. |
| `TaskContext` | Task id, type, attempt, created-at and `idempotencyKey()`. |
| `EnqueueOptions` | Explicit type, `runAt` or delay, dedupe key. |
| `TaskRecord`, `TaskStatus` | Read model of a row. |
| `TaskPayloadCodec`, `JacksonTaskPayloadCodec`, `GsonTaskPayloadCodec` | Payload serialization. |
| `TaskRelayProperties` | `taskrelay.*` configuration. |
| `TaskRelayAutoConfiguration` | Spring Boot auto-configuration, registered in `META-INF/spring/...AutoConfiguration.imports`. |

Package-private internals:

| Class | Role |
|---|---|
| `TaskStore` | All SQL. |
| `TaskRelaySchema` | Idempotent DDL. Runs after Flyway or Liquibase via `@DependsOnDatabaseInitialization`. |
| `TaskRegistry` | Handlers by type and by payload class. Resolved lazily to avoid circular dependencies with services that enqueue. |
| `JdbcTaskQueue` | The `TaskQueue` implementation. |
| `TaskWorker` | Claims and runs one type. |
| `TaskRelayWorkers` | `SmartLifecycle` that owns the executors and graceful shutdown. |
| `TaskTable` | Table name validation. |

## Invariants

- **Stored strings never change.** Status strings `NEW`, `RUNNING`, `DONE`, `FAILED` and the existing column names are read by tables that already hold production data. Never rename them.
- **Schema changes are additive and idempotent.** `ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` only, because the same DDL runs against old hand-rolled tables and fresh databases alike.
- **`attempts` is incremented at claim time.** A row whose `attempts` exceeds `maxAttempts()` after a lock expiry is failed, not re-run. That is what keeps `maxAttempts() == 1` at-most-once.
- **Enqueue goes through `spring-jdbc` on the application's `DataSource`**, so it joins JPA and JDBC transactions. Do not open connections manually there.
- **No `TaskScheduler` or `@EnableScheduling` dependency.** Workers use their own daemon executors, so applications' `@Scheduled` setup is untouched.
- **Do not register library internals as public API.** Add public types only when an application genuinely needs them.

## Conventions

- No comments in code; documentation lives in the README files and the skill.
- Every behaviour change comes with a test in `TaskWorkerIntegrationTest` (store and worker level) or `TaskRelayAutoConfigurationTest` (wiring).
- Update `README.md`, `README.ru.md` and the skill when a property, default or API changes.
