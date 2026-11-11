package io.github.yanisderbikov.taskrelay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskWorkerIntegrationTest {

    private final DataSource dataSource = PostgresSupport.dataSource();
    private final JdbcTemplate jdbc = new JdbcTemplate(dataSource);
    private final TaskPayloadCodec codec = new JacksonTaskPayloadCodec(new com.fasterxml.jackson.databind.ObjectMapper());
    private final MutableClock clock = new MutableClock(Instant.parse("2026-11-11T10:00:00Z"));
    private String table;
    private TaskStore store;

    @BeforeEach
    void setUp() {
        table = PostgresSupport.uniqueTable();
        new TaskRelaySchema(dataSource, new TaskTable(table), true).ensure();
        store = new TaskStore(dataSource, new TaskTable(table));
    }

    private JdbcTaskQueue queue(TaskHandler<?>... handlers) {
        return new JdbcTaskQueue(store, StaticHandlers.registry(handlers), codec, clock);
    }

    private TaskWorker worker(TaskHandler<?> handler) {
        return new TaskWorker(handler, store, codec, clock, 10, Duration.ofMinutes(5));
    }

    private TaskRecord task(UUID id) {
        return store.find(id).orElseThrow();
    }

    @Test
    void enqueuedTaskIsHandledOnceAndMarkedDone() {
        RecordingHandler handler = new RecordingHandler("EMAIL");
        UUID id = queue(handler).enqueue(new RecordingHandler.Payload("hello"));

        assertThat(worker(handler).pollOnce()).isEqualTo(1);
        assertThat(worker(handler).pollOnce()).isZero();

        assertThat(handler.handled).containsExactly("hello");
        TaskContext context = handler.contexts.get(0);
        assertThat(context.taskId()).isEqualTo(id);
        assertThat(context.attempt()).isEqualTo(1);
        assertThat(context.idempotencyKey()).isEqualTo("task:" + id);
        TaskRecord done = task(id);
        assertThat(done.status()).isEqualTo(TaskStatus.DONE);
        assertThat(done.attempts()).isEqualTo(1);
    }

    @Test
    void enqueueJoinsTheCallersTransaction() {
        RecordingHandler handler = new RecordingHandler("EMAIL");
        JdbcTaskQueue queue = queue(handler);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            queue.enqueue(new RecordingHandler.Payload("rolled back"));
            throw new IllegalStateException("business failure");
        })).isInstanceOf(IllegalStateException.class);
        tx.executeWithoutResult(status -> queue.enqueue(new RecordingHandler.Payload("committed")));

        worker(handler).pollOnce();
        assertThat(handler.handled).containsExactly("committed");
    }

    @Test
    void retryableFailureIsRescheduledWithBackoffAndFailsAfterLastAttempt() {
        RecordingHandler handler = new RecordingHandler("EMAIL", 3, Duration.ZERO)
                .failWith(new IllegalStateException("smtp down"));
        UUID id = queue(handler).enqueue(new RecordingHandler.Payload("x"));
        TaskWorker worker = worker(handler);

        worker.pollOnce();
        TaskRecord first = task(id);
        assertThat(first.status()).isEqualTo(TaskStatus.NEW);
        assertThat(first.attempts()).isEqualTo(1);
        assertThat(first.comment()).isEqualTo("smtp down");
        assertThat(first.nextAttemptAt()).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));

        assertThat(worker.pollOnce()).isZero();
        clock.advance(Duration.ofMinutes(1));
        worker.pollOnce();
        assertThat(task(id).nextAttemptAt()).isEqualTo(clock.instant().plus(Duration.ofMinutes(2)));

        clock.advance(Duration.ofMinutes(2));
        worker.pollOnce();
        TaskRecord last = task(id);
        assertThat(last.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(last.attempts()).isEqualTo(3);
    }

    @Test
    void nonRetryableFailureFailsImmediately() {
        RecordingHandler handler = new RecordingHandler("EMAIL", 5, Duration.ZERO)
                .failWith(new IllegalArgumentException("bad address"));
        UUID id = queue(handler).enqueue(new RecordingHandler.Payload("x"));

        worker(handler).pollOnce();

        assertThat(task(id).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task(id).comment()).isEqualTo("bad address");
    }

    @Test
    void unreadablePayloadFailsWithoutCallingTheHandler() {
        RecordingHandler handler = new RecordingHandler("EMAIL", 5, Duration.ZERO);
        UUID id = UUID.randomUUID();
        store.insert(id, "EMAIL", "not json", null, null, clock.instant());

        worker(handler).pollOnce();

        assertThat(handler.handled).isEmpty();
        assertThat(task(id).status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    void scheduledTaskWaitsForItsRunAt() {
        RecordingHandler handler = new RecordingHandler("EMAIL");
        queue(handler).enqueue(new RecordingHandler.Payload("later"),
                EnqueueOptions.defaults().withDelay(Duration.ofMinutes(10)));

        assertThat(worker(handler).pollOnce()).isZero();
        clock.advance(Duration.ofMinutes(10));
        assertThat(worker(handler).pollOnce()).isEqualTo(1);
    }

    @Test
    void handlerDelayKeepsFreshTasksWaiting() {
        RecordingHandler handler = new RecordingHandler("REPLY_CHECK", 1, Duration.ofMinutes(30));
        queue(handler).enqueue(new RecordingHandler.Payload("check"));

        clock.advance(Duration.ofMinutes(29));
        assertThat(worker(handler).pollOnce()).isZero();
        clock.advance(Duration.ofMinutes(1));
        assertThat(worker(handler).pollOnce()).isEqualTo(1);
    }

    @Test
    void dedupeKeySkipsSecondEnqueue() {
        RecordingHandler handler = new RecordingHandler("AMO_FAILED_PAYMENT");
        JdbcTaskQueue queue = queue(handler);
        EnqueueOptions options = EnqueueOptions.defaults().withDedupeKey("transaction-1");

        Optional<UUID> first = queue.enqueue(new RecordingHandler.Payload("a"), options);
        Optional<UUID> second = queue.enqueue(new RecordingHandler.Payload("b"), options);

        assertThat(first).isPresent();
        assertThat(second).isEmpty();
        assertThat(queue.findByType("AMO_FAILED_PAYMENT")).hasSize(1);
    }

    @Test
    void lockedRowsAreSkippedByConcurrentClaims() throws Exception {
        RecordingHandler handler = new RecordingHandler("EMAIL");
        JdbcTaskQueue queue = queue(handler);
        UUID locked = queue.enqueue(new RecordingHandler.Payload("locked"));
        UUID free = queue.enqueue(new RecordingHandler.Payload("free"));

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement lock = connection.prepareStatement("SELECT id FROM " + table + " WHERE id = ? FOR UPDATE")) {
                lock.setObject(1, locked);
                lock.executeQuery();
            }
            List<TaskRecord> claimed = store.claim("EMAIL", 10, clock.instant(), clock.instant().plusSeconds(60), null);
            assertThat(claimed).extracting(TaskRecord::id).containsExactly(free);
            connection.rollback();
        }
    }

    @Test
    void expiredLockIsReclaimedWhileAttemptsRemain() {
        RecordingHandler retrying = new RecordingHandler("EMAIL", 3, Duration.ZERO);
        UUID id = queue(retrying).enqueue(new RecordingHandler.Payload("crashed"));
        store.claim("EMAIL", 10, clock.instant(), clock.instant().plus(Duration.ofMinutes(5)), null);

        assertThat(worker(retrying).pollOnce()).isZero();
        clock.advance(Duration.ofMinutes(6));
        worker(retrying).pollOnce();

        assertThat(retrying.handled).containsExactly("crashed");
        assertThat(task(id).status()).isEqualTo(TaskStatus.DONE);
        assertThat(task(id).attempts()).isEqualTo(2);
    }

    @Test
    void expiredLockOfSingleAttemptTaskIsFailedNotRepeated() {
        RecordingHandler once = new RecordingHandler("EMAIL");
        UUID id = queue(once).enqueue(new RecordingHandler.Payload("crashed"));
        store.claim("EMAIL", 10, clock.instant(), clock.instant().plus(Duration.ofMinutes(5)), null);

        clock.advance(Duration.ofMinutes(6));
        worker(once).pollOnce();

        assertThat(once.handled).isEmpty();
        assertThat(task(id).status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    void stoppedWorkerReleasesUnstartedTasks() {
        AtomicReference<TaskWorker> self = new AtomicReference<>();
        TaskHandler<RecordingHandler.Payload> stopsAfterFirst = new TaskHandler<>("EMAIL", RecordingHandler.Payload.class) {
            @Override
            protected void handle(RecordingHandler.Payload payload, TaskContext context) {
                self.get().stop();
            }
        };
        JdbcTaskQueue queue = queue(stopsAfterFirst);
        UUID first = queue.enqueue(new RecordingHandler.Payload("1"));
        clock.advance(Duration.ofSeconds(1));
        UUID second = queue.enqueue(new RecordingHandler.Payload("2"));
        TaskWorker worker = worker(stopsAfterFirst);
        self.set(worker);

        worker.pollOnce();

        assertThat(task(first).status()).isEqualTo(TaskStatus.DONE);
        TaskRecord released = task(second);
        assertThat(released.status()).isEqualTo(TaskStatus.NEW);
        assertThat(released.attempts()).isZero();
    }

    @Test
    void failedTaskCanBeRetriedManually() {
        RecordingHandler handler = new RecordingHandler("EMAIL").failWith(new IllegalStateException("down"));
        JdbcTaskQueue queue = queue(handler);
        UUID id = queue.enqueue(new RecordingHandler.Payload("x"));
        worker(handler).pollOnce();

        assertThat(queue.retry(id)).isTrue();
        assertThat(queue.retry(id)).isFalse();
        assertThat(task(id).status()).isEqualTo(TaskStatus.NEW);
        assertThat(task(id).attempts()).isZero();
    }

    @Test
    void legacyTableIsUpgradedInPlace() {
        String legacy = PostgresSupport.uniqueTable();
        jdbc.execute("CREATE TABLE " + legacy + " (id UUID PRIMARY KEY, type VARCHAR(255) NOT NULL, payload TEXT,"
                + " status VARCHAR(255) NOT NULL, comment TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        UUID oldId = UUID.randomUUID();
        jdbc.update("INSERT INTO " + legacy + " (id, type, payload, status, created_at) VALUES (?, 'EMAIL', ?, 'NEW', ?)",
                oldId, "{\"value\":\"from old code\"}", java.sql.Timestamp.from(clock.instant().minusSeconds(60)));

        new TaskRelaySchema(dataSource, new TaskTable(legacy), true).ensure();
        new TaskRelaySchema(dataSource, new TaskTable(legacy), true).ensure();
        TaskStore legacyStore = new TaskStore(dataSource, new TaskTable(legacy));
        RecordingHandler handler = new RecordingHandler("EMAIL");
        new TaskWorker(handler, legacyStore, codec, clock, 10, Duration.ofMinutes(5)).pollOnce();

        assertThat(handler.handled).containsExactly("from old code");
        assertThat(legacyStore.find(oldId).orElseThrow().status()).isEqualTo(TaskStatus.DONE);
    }

    @Test
    void gsonCodecReadsPayloadsWrittenByGson() {
        TaskPayloadCodec gson = new GsonTaskPayloadCodec();
        GsonPayload decoded = gson.decode(gson.encode(new GsonPayload("a@b.c", 3)), GsonPayload.class);
        assertThat(decoded.to).isEqualTo("a@b.c");
        assertThat(decoded.count).isEqualTo(3);
    }

    @Test
    void cleanupRemovesOnlyOldDoneTasks() {
        RecordingHandler handler = new RecordingHandler("EMAIL").failWith(new IllegalStateException("down"));
        JdbcTaskQueue queue = queue(handler);
        UUID failed = queue.enqueue(new RecordingHandler.Payload("failed"));
        worker(handler).pollOnce();
        RecordingHandler ok = new RecordingHandler("OK");
        UUID done = queue(ok).enqueue(new RecordingHandler.Payload("done"));
        worker(ok).pollOnce();

        clock.advance(Duration.ofDays(31));
        assertThat(store.deleteDoneBefore(clock.instant().minus(Duration.ofDays(30)))).isEqualTo(1);

        assertThat(store.find(done)).isEmpty();
        assertThat(store.find(failed)).isPresent();
    }

    static final class GsonPayload {
        private final String to;
        private final int count;

        GsonPayload(String to, int count) {
            this.to = to;
            this.count = count;
        }
    }

    static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
