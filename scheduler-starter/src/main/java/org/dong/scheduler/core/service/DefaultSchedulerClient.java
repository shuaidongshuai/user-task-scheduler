package org.dong.scheduler.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class DefaultSchedulerClient implements SchedulerClient {
    private static final Logger log = LoggerFactory.getLogger(DefaultSchedulerClient.class);
    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 99_999;

    private final TaskRepository taskRepository;
    private final QueueRedisService queueRedisService;
    private final ObjectMapper objectMapper;

    public DefaultSchedulerClient(TaskRepository taskRepository,
                                  QueueRedisService queueRedisService,
                                  ObjectMapper objectMapper) {
        this.taskRepository = taskRepository;
        this.queueRedisService = queueRedisService;
        this.objectMapper = objectMapper;
    }

    @Override
    public long submit(TaskSubmitRequest request) {
        TaskSubmitRequest normalized = normalize(request);
        LocalDateTime now = LocalDateTime.now();
        TaskStatus status = normalized.getExecuteAt().isAfter(now) ? TaskStatus.PENDING : TaskStatus.RUNNABLE;
        log.info("submit task request accepted, taskNo={}, group={}, user={}, bizType={}, priority={}, executeAt={}, maxRetry={}",
                normalized.getTaskNo(), normalized.getGroupCode(), normalized.getUserId(), normalized.getBizType(),
                normalized.getPriority(), normalized.getExecuteAt(), normalized.getMaxRetryCount());
        long id = taskRepository.insert(normalized, toJson(normalized.getExt()), status);

        SchedulerTask task = taskRepository.findById(id)
                .orElseThrow(() -> new IllegalStateException("task not found after insert: " + id));
        queueRedisService.enqueue(task);
        log.info("task submitted, taskId={}, taskNo={}, status={}, executeAt={}",
                id, task.getTaskNo(), status, task.getExecuteAt());
        return id;
    }

    @Override
    public boolean cancel(String taskNo) {
        boolean cancelled = taskRepository.markCancelledByTaskNo(taskNo, LocalDateTime.now());
        log.info("cancel task by taskNo={}, cancelled={}", taskNo, cancelled);
        return cancelled;
    }

    private TaskSubmitRequest normalize(TaskSubmitRequest request) {
        Objects.requireNonNull(request, "request is null");
        requireText(request.getGroupCode(), "groupCode is required");
        requireText(request.getUserId(), "userId is required");
        requireText(request.getBizType(), "bizType is required");

        if (request.getTaskNo() == null || request.getTaskNo().isBlank()) {
            request.setTaskNo("task-" + UUID.randomUUID().toString().replace("-", ""));
        }
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
        if (request.getPriority() == null) {
            request.setPriority(0);
        } else if (request.getPriority() < MIN_PRIORITY) {
            request.setPriority(MIN_PRIORITY);
        } else if (request.getPriority() > MAX_PRIORITY) {
            request.setPriority(MAX_PRIORITY);
        }
        if (request.getExt() == null) {
            request.setExt(Map.of());
        }
        return request;
    }

    private void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialize ext", e);
        }
    }
}
