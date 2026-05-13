package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.model.batch.BatchSubmitTaskRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.SchedulerClient;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        TaskSubmitRequest normalized = normalizeSingle(request);
        String taskNo = "t-" + UUID.randomUUID().toString().replace("-", "");
        log.info("submit task request accepted, taskNo={}, group={}, user={}, bizType={}, priority={}, executeAt={}," +
                " maxRetry={}, dependencyTaskIds={}",
                taskNo, normalized.getGroupCode(), normalized.getUserId(), normalized.getBizType(),
                normalized.getPriority(), normalized.getExecuteAt(), normalized.getMaxRetryCount(),
                dependencyTaskIds(normalized));
        long id = taskStateService.submit(taskNo, normalized);
        SchedulerTask task = taskRepository.findById(id)
                .orElseThrow(() -> new IllegalStateException("task not found after submit: " + id));
        log.info("task submitted, taskId={}, taskNo={}, status={}, executeAt={}",
                id, task.getTaskNo(), task.getStatus(), task.getExecuteAt());
        return id;
    }

    @Override
    public List<BatchSubmitResultItem> submitBatch(BatchSubmitRequest request) {
        List<NormalizedBatchTask> normalizedTasks = normalizeBatch(request);
        log.info("submit batch request accepted, size={}", normalizedTasks.size());
        List<TaskStateService.BatchSubmitCommand> commands = normalizedTasks.stream().map(item ->
                new TaskStateService.BatchSubmitCommand(
                        item.clientTaskRef,
                        "t-" + UUID.randomUUID().toString().replace("-", ""),
                        item.taskRequest,
                        item.dependencies
                )).toList();
        List<BatchSubmitResultItem> results = taskStateService.submitBatch(commands);
        log.info("batch submitted, size={}", results.size());
        return results;
    }

    @Override
    public boolean cancel(String taskNo) {
        boolean cancelled = taskStateService.cancel(taskNo);
        log.info("cancel task by taskNo={}, cancelled={}", taskNo, cancelled);
        return cancelled;
    }

    private TaskSubmitRequest normalizeSingle(TaskSubmitRequest request) {
        TaskSubmitRequest normalized = normalizeBase(request);
        normalizeTaskIdDependencies(normalized);
        return normalized;
    }

    private TaskSubmitRequest normalizeBase(TaskSubmitRequest request) {
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

    private void normalizeTaskIdDependencies(TaskSubmitRequest request) {
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
        ensureTaskIdsExist(new ArrayList<>(taskIds));
        request.setDependencies(dependencies);
    }

    private List<NormalizedBatchTask> normalizeBatch(BatchSubmitRequest request) {
        Objects.requireNonNull(request, "request is null");
        if (request.getTasks() == null || request.getTasks().isEmpty()) {
            throw new IllegalArgumentException("batch tasks is required");
        }
        Map<String, NormalizedBatchTask> byRef = new LinkedHashMap<>();
        for (BatchSubmitTaskRequest task : request.getTasks()) {
            if (task == null) {
                throw new IllegalArgumentException("batch task is null");
            }
            String ref = normalizeRef(task.getClientTaskRef());
            if (byRef.containsKey(ref)) {
                throw new IllegalArgumentException("duplicate clientTaskRef: " + ref);
            }
            TaskSubmitRequest base = normalizeBase(new TaskSubmitRequest()
                    .setGroupCode(task.getGroupCode())
                    .setUserId(task.getUserId())
                    .setBizType(task.getBizType())
                    .setBizKey(task.getBizKey())
                    .setPriority(task.getPriority())
                    .setExecuteAt(task.getExecuteAt())
                    .setMaxRetryCount(task.getMaxRetryCount())
                    .setExecuteTimeoutSec(task.getExecuteTimeoutSec())
                    .setRetryDelaySec(task.getRetryDelaySec())
                    .setExtInfo(task.getExtInfo())
                    .setDependencies(List.of()));
            List<BatchSubmitDependencyRequest> dependencies = normalizeBatchDependencies(ref, task.getDependencies());
            byRef.put(ref, new NormalizedBatchTask(ref, base, dependencies));
        }
        detectInBatchCycle(byRef);
        return new ArrayList<>(byRef.values());
    }

    private List<BatchSubmitDependencyRequest> normalizeBatchDependencies(String taskRef,
                                                                          List<BatchSubmitDependencyRequest> dependencies) {
        if (dependencies == null) {
            return List.of();
        }
        List<BatchSubmitDependencyRequest> normalized = dependencies.stream()
                .filter(Objects::nonNull)
                .toList();
        Set<String> dedup = new HashSet<>();
        Set<Long> taskIds = new HashSet<>();
        for (BatchSubmitDependencyRequest dependency : normalized) {
            boolean hasTaskId = dependency.getDependsOnTaskId() != null;
            boolean hasRef = dependency.getDependsOnClientTaskRef() != null
                    && !dependency.getDependsOnClientTaskRef().isBlank();
            if (hasTaskId == hasRef) {
                throw new IllegalArgumentException("dependency requires exactly one of dependsOnTaskId/dependsOnClientTaskRef");
            }
            if (hasTaskId && dependency.getDependsOnTaskId() <= 0) {
                throw new IllegalArgumentException("dependency dependsOnTaskId must be positive");
            }
            if (hasTaskId) {
                taskIds.add(dependency.getDependsOnTaskId());
            }
            if (hasRef) {
                dependency.setDependsOnClientTaskRef(normalizeRef(dependency.getDependsOnClientTaskRef()));
                if (Objects.equals(taskRef, dependency.getDependsOnClientTaskRef())) {
                    throw new IllegalArgumentException("self dependency is not allowed: " + taskRef);
                }
            }
            if (dependency.getTargetState() == null) {
                throw new IllegalArgumentException("dependency targetState is required");
            }
            String depKey = hasTaskId
                    ? "id:" + dependency.getDependsOnTaskId() + ":" + dependency.getTargetState()
                    : "ref:" + dependency.getDependsOnClientTaskRef() + ":" + dependency.getTargetState();
            if (!dedup.add(depKey)) {
                throw new IllegalArgumentException("duplicate dependency: " + depKey);
            }
        }
        ensureTaskIdsExist(new ArrayList<>(taskIds));
        return normalized;
    }

    private void ensureTaskIdsExist(List<Long> taskIds) {
        if (taskIds == null || taskIds.isEmpty()) {
            return;
        }
        List<Long> existing = taskRepository.findExistingTaskIds(taskIds);
        Set<Long> existingIds = new HashSet<>(existing);
        for (Long taskId : taskIds) {
            if (!existingIds.contains(taskId)) {
                throw new IllegalArgumentException("dependency taskId not found: " + taskId);
            }
        }
    }

    private String normalizeRef(String ref) {
        if (ref == null || ref.isBlank()) {
            throw new IllegalArgumentException("clientTaskRef is required");
        }
        return ref.trim();
    }

    private void detectInBatchCycle(Map<String, NormalizedBatchTask> byRef) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> outgoing = new LinkedHashMap<>();
        for (String ref : byRef.keySet()) {
            indegree.put(ref, 0);
            outgoing.put(ref, new ArrayList<>());
        }
        for (NormalizedBatchTask task : byRef.values()) {
            for (BatchSubmitDependencyRequest dep : task.dependencies) {
                if (dep.getDependsOnClientTaskRef() == null) {
                    continue;
                }
                if (!byRef.containsKey(dep.getDependsOnClientTaskRef())) {
                    throw new IllegalArgumentException(
                            "dependency clientTaskRef not found in batch: " + dep.getDependsOnClientTaskRef());
                }
                outgoing.get(dep.getDependsOnClientTaskRef()).add(task.clientTaskRef);
                indegree.put(task.clientTaskRef, indegree.get(task.clientTaskRef) + 1);
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.addLast(entry.getKey());
            }
        }
        int visited = 0;
        while (!queue.isEmpty()) {
            String node = queue.removeFirst();
            visited++;
            for (String next : outgoing.get(node)) {
                int left = indegree.get(next) - 1;
                indegree.put(next, left);
                if (left == 0) {
                    queue.addLast(next);
                }
            }
        }
        if (visited != byRef.size()) {
            throw new IllegalArgumentException("in-batch dependency cycle detected");
        }
    }

    private record NormalizedBatchTask(
            String clientTaskRef,
            TaskSubmitRequest taskRequest,
            List<BatchSubmitDependencyRequest> dependencies
    ) {
    }

    private List<Long> dependencyTaskIds(TaskSubmitRequest request) {
        if (request.getDependencies() == null || request.getDependencies().isEmpty()) {
            return List.of();
        }
        return request.getDependencies().stream()
                .map(TaskDependencyRequest::getTaskId)
                .filter(Objects::nonNull)
                .toList();
    }
}
