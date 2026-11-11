package io.github.yanisderbikov.taskrelay;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public final class EnqueueOptions {

    private static final EnqueueOptions DEFAULTS = new EnqueueOptions(null, null, null, null);
    private static final int MAX_DEDUPE_KEY_LENGTH = 255;

    private final String type;
    private final Instant runAt;
    private final Duration delay;
    private final String dedupeKey;

    private EnqueueOptions(String type, Instant runAt, Duration delay, String dedupeKey) {
        this.type = type;
        this.runAt = runAt;
        this.delay = delay;
        this.dedupeKey = dedupeKey;
    }

    public static EnqueueOptions defaults() {
        return DEFAULTS;
    }

    public EnqueueOptions withType(String type) {
        return new EnqueueOptions(TaskTypes.requireValid(type), runAt, delay, dedupeKey);
    }

    public EnqueueOptions withRunAt(Instant runAt) {
        return new EnqueueOptions(type, Objects.requireNonNull(runAt, "runAt"), null, dedupeKey);
    }

    public EnqueueOptions withDelay(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("Delay must not be negative: " + delay);
        }
        return new EnqueueOptions(type, null, delay, dedupeKey);
    }

    public EnqueueOptions withDedupeKey(String dedupeKey) {
        if (dedupeKey == null || dedupeKey.isBlank()) {
            throw new IllegalArgumentException("Dedupe key must not be blank");
        }
        if (dedupeKey.length() > MAX_DEDUPE_KEY_LENGTH) {
            throw new IllegalArgumentException("Dedupe key is longer than " + MAX_DEDUPE_KEY_LENGTH + " characters");
        }
        return new EnqueueOptions(type, runAt, delay, dedupeKey);
    }

    public String type() {
        return type;
    }

    public String dedupeKey() {
        return dedupeKey;
    }

    Instant resolveRunAt(Instant now) {
        if (runAt != null) {
            return runAt;
        }
        if (delay != null && !delay.isZero()) {
            return now.plus(delay);
        }
        return null;
    }
}
