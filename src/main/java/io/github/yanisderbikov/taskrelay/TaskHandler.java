package io.github.yanisderbikov.taskrelay;

import java.time.Duration;
import java.util.Objects;

public abstract class TaskHandler<P> {

    private static final Duration FIRST_RETRY_DELAY = Duration.ofMinutes(1);
    private static final Duration MAX_RETRY_DELAY = Duration.ofHours(1);

    private final String type;
    private final Class<P> payloadType;

    protected TaskHandler(String type, Class<P> payloadType) {
        this.type = TaskTypes.requireValid(type);
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
    }

    public final String type() {
        return type;
    }

    public final Class<P> payloadType() {
        return payloadType;
    }

    protected abstract void handle(P payload, TaskContext context) throws Exception;

    protected int maxAttempts() {
        return 1;
    }

    protected boolean retryable(Exception error) {
        return true;
    }

    protected Duration retryDelay(int attempt) {
        int shift = Math.min(Math.max(attempt - 1, 0), 20);
        Duration delay = FIRST_RETRY_DELAY.multipliedBy(1L << shift);
        return delay.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : delay;
    }

    protected Duration delay() {
        return Duration.ZERO;
    }

    protected boolean dedicatedThread() {
        return false;
    }
}
