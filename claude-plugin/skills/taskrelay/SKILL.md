---
name: taskrelay
description: Use in Spring Boot projects that depend on TaskRelay (com.github.yanisderbikov:taskrelay) or that need a side effect after a database change, such as sending an email, calling a CRM or another microservice, or firing a webhook once an order or payment is saved. Also use when the user asks for a background task, an outbox, retries with backoff, a delayed job, deduplicated jobs, or about tasks stuck in RUNNING or FAILED.
---

# TaskRelay

TaskRelay is a transactional outbox for Spring Boot on PostgreSQL.
- Code enqueues a payload inside the business transaction.
- A background worker later calls the matching `TaskHandler`, with retries.
- The queue is one table, `taskrelay_task` by default (`taskrelay.table-name`).

Package: `io.github.yanisderbikov.taskrelay`. Public API:
- `TaskQueue`, injected as a bean;
- `TaskHandler<P>`;
- `TaskContext`;
- `EnqueueOptions`;
- `TaskRecord`;
- `TaskStatus`;
- `TaskPayloadCodec`.

## When to reach for it

Use a task when code must do something outside the database **after** changing the database: an email, an SMS, a CRM call, a webhook, a call to another service, or regenerating a file.

Never call such things directly inside `@Transactional`. They are not rolled back, they hold DB locks while waiting on the network, and they are lost on restart.

Do not use TaskRelay for:
- recurring cron jobs: keep `@Scheduled` and let it enqueue tasks;
- in-process work that needs no durability.

## Adding a new task type

1. **Payload.** Create a small serializable class, preferably a record, holding ids and the few values needed. Do not put entities, secrets or large blobs in it.
   ```java
   public record InvoiceEmail(UUID invoiceId) {
   }
   ```
2. **Handler.** One `@Component` extends `TaskHandler<P>`. The type string is stored in the database, so it must be unique and stable; use UPPER_SNAKE_CASE.
   ```java
   @Component
   class InvoiceEmailHandler extends TaskHandler<InvoiceEmail> {

       private final Invoices invoices;
       private final Mailer mailer;

       InvoiceEmailHandler(Invoices invoices, Mailer mailer) {
           super("INVOICE_EMAIL", InvoiceEmail.class);
           this.invoices = invoices;
           this.mailer = mailer;
       }

       @Override
       protected void handle(InvoiceEmail payload, TaskContext context) {
           Invoice invoice = invoices.findById(payload.invoiceId())
                   .orElseThrow(() -> new IllegalStateException("Invoice not found: " + payload.invoiceId()));
           mailer.send(invoice.email(), render(invoice), context.idempotencyKey());
       }

       @Override
       protected int maxAttempts() {
           return 6;
       }
   }
   ```
3. **Enqueue.** Do it in the same transaction as the business change:
   ```java
   @Transactional
   public void issue(Invoice invoice) {
       invoices.save(invoice);
       taskQueue.enqueue(new InvoiceEmail(invoice.id()));
   }
   ```
   The type is resolved from the payload class through the registered handler. For a producer without a handler, pass `EnqueueOptions.defaults().withType("TYPE")`.

## Choosing the retry policy

- The default `maxAttempts() == 1` means **at most once**:
  - a failure is final;
  - a task whose worker crashed is marked `FAILED`, not re-run.

  Keep it for actions that must never repeat and cannot be made idempotent.
- `maxAttempts() > 1` means **at least once**. The handler may run again for the same task, so make it idempotent:
  - pass `context.idempotencyKey()` (`task:<uuid>`, stable across attempts) to downstream APIs;
  - or check state before acting ("already sent?").
- Override `retryable(Exception)` to return `false` for permanent errors (validation, 4xx, missing data that will never appear). Those fail immediately.
- `retryDelay(attempt)` defaults to 1m, 2m, 4m … capped at 1h.
- Throwing any exception marks the attempt as failed, and the message lands in the `comment` column. Do not swallow errors you want retried.

## Delays, scheduling, deduplication

- Run later: `taskQueue.enqueue(p, EnqueueOptions.defaults().withDelay(Duration.ofMinutes(30)))` or `.withRunAt(instant)`.
- Every task of a type waits a grace period: override `delay()` in the handler.
- Deduplicate repeated triggers (double webhooks, retries upstream) with `EnqueueOptions.defaults().withDedupeKey(naturalKey)`.
  - It returns `Optional.empty()` when a task of that type with that key already exists, in any status.
  - Prefer this over searching payloads.
- Slow handlers that should not delay other types can override `dedicatedThread()` to return `true`.

## Configuration (application.properties)

```properties
taskrelay.poll-interval=5s
taskrelay.batch-size=50
taskrelay.lock-timeout=15m
taskrelay.types[NEWSLETTER].batch-size=5
taskrelay.types[NEWSLETTER].poll-interval=1m
```

- Per-type keys must use brackets, otherwise Spring drops `_` from the key.
- `taskrelay.lock-timeout` must exceed the time to process a whole batch.
- `taskrelay.payload-format=gson` is only for projects whose stored payloads were written by Gson.
- `taskrelay.worker.enabled=false` disables background processing; enqueueing still works.
- `@EnableScheduling` is not required.

## Testing

- Unit-test a handler by calling `handle(payload, new TaskContext(UUID.randomUUID(), "TYPE", 1, Instant.now()))` from a test in the handler's package. Also assert `retryable(...)` and `maxAttempts()` for important error cases.
- In `@SpringBootTest`, set `taskrelay.worker.enabled=false`. Assert enqueued work with `taskQueue.findByType("TYPE")` and `taskQueue.payload(record, Payload.class)`.
- Do not test TaskRelay's own claiming, retries or statuses in the application. The library covers that.

## Operations

- **Inspect.** `taskQueue.find(id)`, `taskQueue.findByType(type, limit)`, or SQL on the table (`status`, `attempts`, `comment`, `next_attempt_at`, `locked_until`).
- **Retry a failed task.** `taskQueue.retry(id)`, or `UPDATE … SET status='NEW', attempts=0, next_attempt_at=NULL WHERE status='FAILED' AND …`.
- **Renaming a type** strands existing rows. Migrate them with `UPDATE … SET type='NEW_NAME' WHERE type='OLD_NAME'` in the same release.
- **Old DONE rows.** Removed only when `taskrelay.retention` is set.

## Do not

- Do not create your own task tables, `@Scheduled` pollers or `AbstractRunnableTask` copies next to TaskRelay.
- Do not call external systems inside `@Transactional` "because it is simpler". Enqueue a task.
- Do not raise `maxAttempts()` on a handler that is not idempotent.
- Do not change a payload class incompatibly while old tasks of that type may still be queued. Add fields; do not rename or remove them in the same release.
- TaskRelay is PostgreSQL-only (9.6+). Do not suggest it for MySQL or other databases.
