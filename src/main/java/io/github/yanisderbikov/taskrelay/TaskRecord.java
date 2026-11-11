package io.github.yanisderbikov.taskrelay;

import java.time.Instant;
import java.util.UUID;

public record TaskRecord(
        UUID id,
        String type,
        TaskStatus status,
        String payload,
        String dedupeKey,
        int attempts,
        String comment,
        Instant createdAt,
        Instant nextAttemptAt,
        Instant updatedAt) {
}
