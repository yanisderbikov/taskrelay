package io.github.yanisderbikov.taskrelay;

import org.springframework.beans.factory.ObjectProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class TaskRegistry {

    private final ObjectProvider<TaskHandler<?>> provider;
    private final Map<Class<?>, String> typeByPayloadClass = new ConcurrentHashMap<>();
    private volatile Map<String, TaskHandler<?>> handlers;

    TaskRegistry(ObjectProvider<TaskHandler<?>> provider) {
        this.provider = provider;
    }

    Collection<TaskHandler<?>> handlers() {
        return byType().values();
    }

    String typeFor(Class<?> payloadClass) {
        return typeByPayloadClass.computeIfAbsent(payloadClass, this::resolveType);
    }

    private String resolveType(Class<?> payloadClass) {
        List<TaskHandler<?>> exact = new ArrayList<>();
        List<TaskHandler<?>> assignable = new ArrayList<>();
        for (TaskHandler<?> handler : byType().values()) {
            if (handler.payloadType().equals(payloadClass)) {
                exact.add(handler);
            } else if (handler.payloadType().isAssignableFrom(payloadClass)) {
                assignable.add(handler);
            }
        }
        List<TaskHandler<?>> candidates = exact.isEmpty() ? assignable : exact;
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("No TaskHandler accepts payload " + payloadClass.getName()
                    + ". Register a TaskHandler for it or pass EnqueueOptions.defaults().withType(...)");
        }
        if (candidates.size() > 1) {
            throw new IllegalArgumentException("Several TaskHandlers accept payload " + payloadClass.getName() + ": "
                    + candidates.stream().map(TaskHandler::type).toList()
                    + ". Pass EnqueueOptions.defaults().withType(...) to choose one");
        }
        return candidates.get(0).type();
    }

    private Map<String, TaskHandler<?>> byType() {
        Map<String, TaskHandler<?>> current = handlers;
        if (current == null) {
            synchronized (this) {
                current = handlers;
                if (current == null) {
                    current = load();
                    handlers = current;
                }
            }
        }
        return current;
    }

    private Map<String, TaskHandler<?>> load() {
        Map<String, TaskHandler<?>> loaded = new LinkedHashMap<>();
        provider.orderedStream().forEach(handler -> {
            TaskHandler<?> previous = loaded.putIfAbsent(handler.type(), handler);
            if (previous != null) {
                throw new IllegalStateException("Task type " + handler.type() + " is handled by both "
                        + previous.getClass().getName() + " and " + handler.getClass().getName());
            }
        });
        return Map.copyOf(loaded);
    }
}
