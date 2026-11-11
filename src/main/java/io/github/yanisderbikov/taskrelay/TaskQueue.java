package io.github.yanisderbikov.taskrelay;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TaskQueue {

    UUID enqueue(Object payload);

    Optional<UUID> enqueue(Object payload, EnqueueOptions options);

    Optional<TaskRecord> find(UUID id);

    List<TaskRecord> findByType(String type, int limit);

    List<TaskRecord> findByType(String type);

    <P> P payload(TaskRecord task, Class<P> payloadType);

    boolean retry(UUID id);
}
