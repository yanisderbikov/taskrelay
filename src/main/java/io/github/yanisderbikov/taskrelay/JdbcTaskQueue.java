package io.github.yanisderbikov.taskrelay;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

final class JdbcTaskQueue implements TaskQueue {

    private final TaskStore store;
    private final TaskRegistry registry;
    private final TaskPayloadCodec codec;
    private final Clock clock;

    JdbcTaskQueue(TaskStore store, TaskRegistry registry, TaskPayloadCodec codec, Clock clock) {
        this.store = store;
        this.registry = registry;
        this.codec = codec;
        this.clock = clock;
    }

    @Override
    public UUID enqueue(Object payload) {
        return enqueue(payload, EnqueueOptions.defaults())
                .orElseThrow(() -> new IllegalStateException("Task was not inserted"));
    }

    @Override
    public Optional<UUID> enqueue(Object payload, EnqueueOptions options) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(options, "options");
        String type = options.type() != null ? options.type() : registry.typeFor(payload.getClass());
        Instant now = clock.instant();
        UUID id = UUID.randomUUID();
        boolean inserted = store.insert(id, type, codec.encode(payload), options.dedupeKey(),
                options.resolveRunAt(now), now);
        return inserted ? Optional.of(id) : Optional.empty();
    }

    @Override
    public Optional<TaskRecord> find(UUID id) {
        return store.find(Objects.requireNonNull(id, "id"));
    }

    @Override
    public List<TaskRecord> findByType(String type, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive: " + limit);
        }
        return store.findByType(TaskTypes.requireValid(type), limit);
    }

    @Override
    public List<TaskRecord> findByType(String type) {
        return store.findByType(TaskTypes.requireValid(type), null);
    }

    @Override
    public <P> P payload(TaskRecord task, Class<P> payloadType) {
        return codec.decode(task.payload(), payloadType);
    }

    @Override
    public boolean retry(UUID id) {
        return store.retry(Objects.requireNonNull(id, "id"), clock.instant());
    }
}
