package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class DefaultSchedulerClient implements SchedulerClient {
    private static final Logger log = LoggerFactory.getLogger(DefaultSchedulerClient.class);
    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 99_999;

    private final TaskRepository taskRepository;
    private final QueueRedisService queueRedisService;

    public DefaultSchedulerClient(TaskRepository taskRepository,
                                  QueueRedisService queueRedisService) {
        this.taskRepository = taskRepository;
        this.queueRedisService = queueRedisService;
    }

    @Override
    public long submit(TaskSubmitRequest request) {
        TaskSubmitRequest normalized = normalize(request);
        SchedulerTask existing = taskRepository.findByBizTypeAndBizKey(normalized.getBizType(), normalized.getBizKey())
                .orElse(null);
        if (existing != null) {
            log.info("submit task request deduplicated, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}",
                    existing.getId(), existing.getTaskNo(), existing.getGroupCode(), existing.getUserId(),
                    existing.getBizType(), existing.getBizKey());
            return existing.getId();
        }
        String taskNo = "task-" + UUID.randomUUID().toString().replace("-", "");
        LocalDateTime now = LocalDateTime.now();
        TaskStatus status = normalized.getExecuteAt().isAfter(now) ? TaskStatus.PENDING : TaskStatus.RUNNABLE;
        log.info("submit task request accepted, taskNo={}, group={}, user={}, bizType={}, priority={}, executeAt={}, maxRetry={}",
                taskNo, normalized.getGroupCode(), normalized.getUserId(), normalized.getBizType(),
                normalized.getPriority(), normalized.getExecuteAt(), normalized.getMaxRetryCount());
        long id = taskRepository.insert(taskNo, normalized, normalized.getExtInfo(), status);

        SchedulerTask task = taskRepository.findById(id)
                .orElseThrow(() -> new IllegalStateException("task not found after insert: " + id));
        // DuplicateKey race fallback may return an existing task id; existing tasks should not be re-enqueued.
        if (taskNo.equals(task.getTaskNo())) {
            queueRedisService.enqueue(task);
        }
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
}
