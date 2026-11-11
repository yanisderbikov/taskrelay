package io.github.yanisderbikov.taskrelay;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

class RecordingHandler extends TaskHandler<RecordingHandler.Payload> {

    record Payload(String value) {
    }

    final List<String> handled = new CopyOnWriteArrayList<>();
    final List<TaskContext> contexts = new CopyOnWriteArrayList<>();
    private final int maxAttempts;
    private final Duration delay;
    private volatile Consumer<Payload> behaviour = p -> { };

    RecordingHandler(String type) {
        this(type, 1, Duration.ZERO);
    }

    RecordingHandler(String type, int maxAttempts, Duration delay) {
        super(type, Payload.class);
        this.maxAttempts = maxAttempts;
        this.delay = delay;
    }

    RecordingHandler failWith(RuntimeException error) {
        behaviour = p -> {
            throw error;
        };
        return this;
    }

    @Override
    protected void handle(Payload payload, TaskContext context) {
        behaviour.accept(payload);
        handled.add(payload.value());
        contexts.add(context);
    }

    @Override
    protected int maxAttempts() {
        return maxAttempts;
    }

    @Override
    protected boolean retryable(Exception error) {
        return !(error instanceof IllegalArgumentException);
    }

    @Override
    protected Duration delay() {
        return delay;
    }
}
