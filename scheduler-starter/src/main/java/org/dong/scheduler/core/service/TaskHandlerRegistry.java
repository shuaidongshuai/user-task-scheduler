package org.dong.scheduler.core.service;

import org.dong.scheduler.core.spi.TaskHandler;

import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> handlers) {
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
        this.handlers = Map.copyOf(index);
    }

    public TaskHandler find(String bizType) {
        return handlers.get(bizType);
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
