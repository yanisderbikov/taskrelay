package io.github.yanisderbikov.taskrelay;

import java.time.Instant;
import java.util.UUID;

public record TaskContext(UUID taskId, String type, int attempt, Instant createdAt) {

    public String idempotencyKey() {
        return "task:" + taskId;
    }
}
