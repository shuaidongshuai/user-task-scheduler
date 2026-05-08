package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.SchedulerClient;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class DefaultSchedulerClient implements SchedulerClient {
    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 99_999;

    private final TaskRepository taskRepository;
    private final QueueRedisService queueRedisService;
    private final SchedulerProperties properties;
    private final TaskStateService taskStateService;

    public DefaultSchedulerClient(TaskRepository taskRepository,
                                  QueueRedisService queueRedisService,
                                  SchedulerProperties properties,
                                  TaskStateService taskStateService) {
        this.taskRepository = taskRepository;
        this.queueRedisService = queueRedisService;
        this.properties = properties;
        this.taskStateService = taskStateService;
    }

    @Override
    public long submit(TaskSubmitRequest request) {
        TaskSubmitRequest normalized = normalize(request);
        String taskNo = "t-" + UUID.randomUUID().toString().replace("-", "");
        log.info("submit task request accepted, taskNo={}, group={}, user={}, bizType={}, priority={}, executeAt={}, maxRetry={}",
                taskNo, normalized.getGroupCode(), normalized.getUserId(), normalized.getBizType(),
                normalized.getPriority(), normalized.getExecuteAt(), normalized.getMaxRetryCount());
        long id = taskStateService.submit(taskNo, normalized);
        SchedulerTask task = taskRepository.findById(id)
                .orElseThrow(() -> new IllegalStateException("task not found after submit: " + id));
        log.info("task submitted, taskId={}, taskNo={}, status={}, executeAt={}",
                id, task.getTaskNo(), task.getStatus(), task.getExecuteAt());
        return id;
    }

    @Override
    public boolean cancel(String taskNo) {
        boolean cancelled = taskStateService.cancel(taskNo);
        log.info("cancel task by taskNo={}, cancelled={}", taskNo, cancelled);
        return cancelled;
    }

    private TaskSubmitRequest normalize(TaskSubmitRequest request) {
        Objects.requireNonNull(request, "request is null");
        if (request.getGroupCode() == null || request.getGroupCode().isBlank()) {
            request.setGroupCode(properties.getDefaultGroupCode());
        }
        requireText(request.getGroupCode(), "groupCode is required");
        requireText(request.getUserId(), "userId is required");
        requireText(request.getBizType(), "bizType is required");
        requireText(request.getBizKey(), "bizKey is required");
        if (request.getExecuteAt() == null) {
            request.setExecuteAt(LocalDateTime.now());
        }
        if (request.getMaxRetryCount() == null) {
            request.setMaxRetryCount(3);
        }
        if (request.getMaxRetryCount() < 0) {
            request.setMaxRetryCount(0);
        }
        if (request.getRetryDelaySec() != null && request.getRetryDelaySec() < 0) {
            request.setRetryDelaySec(0);
        }
        normalizeDependencies(request);
        if (request.getPriority() == null) {
            request.setPriority(0);
        } else if (request.getPriority() < MIN_PRIORITY) {
            request.setPriority(MIN_PRIORITY);
        } else if (request.getPriority() > MAX_PRIORITY) {
            request.setPriority(MAX_PRIORITY);
        }
        return request;
    }

    private void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private void normalizeDependencies(TaskSubmitRequest request) {
        if (request.getDependencies() == null) {
            return;
        }
        List<TaskDependencyRequest> dependencies = request.getDependencies().stream()
                .filter(Objects::nonNull)
                .toList();
        Set<Long> taskIds = new HashSet<>();
        for (TaskDependencyRequest dependency : dependencies) {
            if (dependency.getTaskId() == null || dependency.getTaskId() <= 0) {
                throw new IllegalArgumentException("dependency taskId is required");
            }
            if (dependency.getTargetState() == null) {
                throw new IllegalArgumentException("dependency targetState is required");
            }
            if (!taskIds.add(dependency.getTaskId())) {
                throw new IllegalArgumentException("duplicate dependency taskId: " + dependency.getTaskId());
            }
        }
        request.setDependencies(dependencies);
    }
}
