package org.dong.scheduler.core.service;

import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.beans.factory.ObjectProvider;

import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskHandlerRegistry {
    private final ObjectProvider<TaskHandler> handlerProvider;
    private volatile Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> handlers) {
        this.handlerProvider = null;
        this.handlers = buildIndex(handlers);
    }

    public TaskHandlerRegistry(ObjectProvider<TaskHandler> handlerProvider) {
        this.handlerProvider = handlerProvider;
        this.handlers = null;
    }

    public TaskHandler find(String bizType) {
        return handlers().get(bizType);
    }

    private Map<String, TaskHandler> handlers() {
        Map<String, TaskHandler> current = handlers;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = handlers;
            if (current == null) {
                if (handlerProvider == null) {
                    current = Map.of();
                } else {
                    current = buildIndex(handlerProvider.orderedStream().toList());
                }
                handlers = current;
            }
        }
        return current;
    }

    private Map<String, TaskHandler> buildIndex(List<TaskHandler> handlers) {
        Map<String, TaskHandler> index = new HashMap<>();
        for (TaskHandler handler : handlers) {
            for (String bizType : resolveBizTypes(handler)) {
                TaskHandler existed = index.putIfAbsent(bizType, handler);
                if (existed != null) {
                    throw new IllegalStateException("duplicate TaskHandler bizType=" + bizType
                            + ", existed=" + existed.getClass().getName()
                            + ", incoming=" + handler.getClass().getName());
                }
            }
        }
        return Map.copyOf(index);
    }

    private List<String> resolveBizTypes(TaskHandler handler) {
        Set<String> resolved = new LinkedHashSet<>();
        List<String> multi = handler.bizTypes();
        if (multi != null) {
            for (String bizType : multi) {
                if (bizType == null || bizType.isBlank()) {
                    continue;
                }
                resolved.add(bizType);
            }
        }

        if (resolved.isEmpty()) {
            throw new IllegalStateException("TaskHandler bizTypes is empty: " + handler.getClass().getName());
        }
        return List.copyOf(resolved);
    }
}
