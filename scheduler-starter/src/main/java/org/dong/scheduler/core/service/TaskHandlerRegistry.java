package org.dong.scheduler.core.service;

import org.dong.scheduler.core.spi.TaskHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> handlers) {
        Map<String, TaskHandler> index = new HashMap<>();
        for (TaskHandler handler : handlers) {
            TaskHandler existed = index.putIfAbsent(handler.bizType(), handler);
            if (existed != null) {
                throw new IllegalStateException("duplicate TaskHandler bizType=" + handler.bizType()
                        + ", existed=" + existed.getClass().getName()
                        + ", incoming=" + handler.getClass().getName());
            }
        }
        this.handlers = Map.copyOf(index);
    }

    public TaskHandler find(String bizType) {
        return handlers.get(bizType);
    }
}
