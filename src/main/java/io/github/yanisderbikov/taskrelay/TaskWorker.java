package io.github.yanisderbikov.taskrelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

final class TaskWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);
    private static final int MAX_COMMENT_LENGTH = 1000;

    private final TaskHandler<Object> handler;
    private final TaskStore store;
    private final TaskPayloadCodec codec;
    private final Clock clock;
    private final int batchSize;
    private final Duration lockTimeout;
    private volatile boolean stopping;

    @SuppressWarnings("unchecked")
    TaskWorker(TaskHandler<?> handler, TaskStore store, TaskPayloadCodec codec, Clock clock,
               int batchSize, Duration lockTimeout) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size of task type " + handler.type() + " must be positive");
        }
        this.handler = (TaskHandler<Object>) handler;
        this.store = store;
        this.codec = codec;
        this.clock = clock;
        this.batchSize = batchSize;
        this.lockTimeout = lockTimeout;
    }

    String type() {
        return handler.type();
    }

    void stop() {
        stopping = true;
    }

    @Override
    public void run() {
        if (stopping) {
            return;
        }
        try {
            pollOnce();
        } catch (Exception e) {
            log.error("TaskRelay worker for type {} failed to poll tasks", handler.type(), e);
        }
    }

    int pollOnce() {
        Instant now = clock.instant();
        Duration delay = handler.delay();
        Instant createdBefore = delay == null || delay.isZero() || delay.isNegative() ? null : now.minus(delay);
        List<TaskRecord> batch = store.claim(handler.type(), batchSize, now, now.plus(lockTimeout), createdBefore);
        for (int i = 0; i < batch.size(); i++) {
            if (stopping) {
                releaseRest(batch.subList(i, batch.size()));
                break;
            }
            execute(batch.get(i));
        }
        return batch.size();
    }

    private void releaseRest(List<TaskRecord> rest) {
        List<UUID> ids = new ArrayList<>(rest.size());
        rest.forEach(task -> ids.add(task.id()));
        try {
            store.release(ids, clock.instant());
        } catch (Exception e) {
            log.warn("TaskRelay could not release {} claimed tasks of type {} on shutdown", ids.size(), handler.type(), e);
        }
    }

    private void execute(TaskRecord task) {
        int maxAttempts = Math.max(handler.maxAttempts(), 1);
        if (task.attempts() > maxAttempts) {
            store.markFailed(task.id(), "Lock expired after " + maxAttempts + " attempt(s); the task was not finished in time",
                    clock.instant());
            log.error("Task {} of type {} lost its lock and has no attempts left", task.id(), handler.type());
            return;
        }
        Object payload;
        try {
            payload = codec.decode(task.payload(), handler.payloadType());
        } catch (Exception e) {
            store.markFailed(task.id(), comment(e), clock.instant());
            log.error("Task {} of type {} has an unreadable payload", task.id(), handler.type(), e);
            return;
        }
        try {
            handler.handle(payload, new TaskContext(task.id(), task.type(), task.attempts(), task.createdAt()));
        } catch (Exception e) {
            onFailure(task, e, maxAttempts);
            return;
        }
        try {
            store.markDone(task.id(), clock.instant());
        } catch (Exception e) {
            log.error("Task {} of type {} succeeded but could not be marked DONE", task.id(), handler.type(), e);
        }
    }

    private void onFailure(TaskRecord task, Exception error, int maxAttempts) {
        int attempt = task.attempts();
        Instant now = clock.instant();
        try {
            if (attempt < maxAttempts && handler.retryable(error)) {
                Instant next = now.plus(handler.retryDelay(attempt));
                store.reschedule(task.id(), next, comment(error), now);
                log.warn("Task {} of type {} failed on attempt {}/{} and will be retried at {}: {}",
                        task.id(), handler.type(), attempt, maxAttempts, next, error.toString());
            } else {
                store.markFailed(task.id(), comment(error), now);
                log.error("Task {} of type {} failed on attempt {}/{}", task.id(), handler.type(), attempt, maxAttempts, error);
            }
        } catch (Exception e) {
            log.error("Task {} of type {} failed and its status could not be saved", task.id(), handler.type(), e);
        }
    }

    private static String comment(Exception error) {
        String message = error.getMessage() != null ? error.getMessage() : error.getClass().getName();
        return message.length() > MAX_COMMENT_LENGTH ? message.substring(0, MAX_COMMENT_LENGTH) : message;
    }
}
