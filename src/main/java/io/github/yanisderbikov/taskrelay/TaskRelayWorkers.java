package io.github.yanisderbikov.taskrelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class TaskRelayWorkers implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TaskRelayWorkers.class);
    private static final Duration CLEANUP_INTERVAL = Duration.ofHours(1);

    private final TaskRegistry registry;
    private final TaskStore store;
    private final TaskPayloadCodec codec;
    private final TaskRelayProperties properties;
    private final Clock clock;
    private final List<TaskWorker> workers = new ArrayList<>();
    private final List<ScheduledExecutorService> executors = new ArrayList<>();
    private volatile boolean running;

    TaskRelayWorkers(TaskRegistry registry, TaskStore store, TaskPayloadCodec codec,
                     TaskRelayProperties properties, Clock clock) {
        this.registry = registry;
        this.store = store;
        this.codec = codec;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ScheduledExecutorService shared = executor("taskrelay-", Math.max(properties.getThreads(), 1));
        long initialDelay = properties.getInitialDelay().toMillis();
        for (TaskHandler<?> handler : registry.handlers()) {
            TaskRelayProperties.TypeSettings settings = properties.settingsFor(handler.type());
            if (Boolean.FALSE.equals(settings.getEnabled())) {
                log.info("TaskRelay worker for type {} is disabled", handler.type());
                continue;
            }
            int batchSize = settings.getBatchSize() != null ? settings.getBatchSize() : properties.getBatchSize();
            Duration pollInterval = settings.getPollInterval() != null ? settings.getPollInterval() : properties.getPollInterval();
            Duration lockTimeout = settings.getLockTimeout() != null ? settings.getLockTimeout() : properties.getLockTimeout();
            TaskWorker worker = new TaskWorker(handler, store, codec, clock, batchSize, lockTimeout);
            ScheduledExecutorService target = handler.dedicatedThread()
                    ? executor("taskrelay-" + handler.type() + "-", 1)
                    : shared;
            target.scheduleWithFixedDelay(worker, initialDelay, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
            workers.add(worker);
            log.info("TaskRelay worker started for type {}: batch {}, every {}", handler.type(), batchSize, pollInterval);
        }
        Duration retention = properties.getRetention();
        if (retention != null) {
            shared.scheduleWithFixedDelay(this::cleanup, initialDelay, CLEANUP_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        }
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        workers.forEach(TaskWorker::stop);
        executors.forEach(ScheduledExecutorService::shutdown);
        long deadline = System.nanoTime() + properties.getShutdownTimeout().toNanos();
        for (ScheduledExecutorService executor : executors) {
            try {
                long left = deadline - System.nanoTime();
                if (left <= 0 || !executor.awaitTermination(left, TimeUnit.NANOSECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        workers.clear();
        executors.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void cleanup() {
        try {
            int deleted = store.deleteDoneBefore(clock.instant().minus(properties.getRetention()));
            if (deleted > 0) {
                log.info("TaskRelay deleted {} finished tasks older than {}", deleted, properties.getRetention());
            }
        } catch (Exception e) {
            log.error("TaskRelay cleanup failed", e);
        }
    }

    private ScheduledExecutorService executor(String prefix, int threads) {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threads, factory);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executors.add(executor);
        return executor;
    }
}
