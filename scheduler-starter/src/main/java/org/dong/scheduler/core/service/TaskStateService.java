package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TaskStateService {
    private static final String WAIT_TIMEOUT_ERROR_CODE = "SCHEDULE_WAIT_TIMEOUT";
    private static final String WAIT_TIMEOUT_ERROR_MSG = "task exceeded max wait before running";

    private final TaskRepository taskRepository;
    private final TaskDependencyService taskDependencyService;
    private final ConcurrencyGuard concurrencyGuard;
    private final QueueRedisService queueRedisService;
    private final TransactionTemplate transactionTemplate;

    public TaskStateService(TaskRepository taskRepository,
                            TaskDependencyService taskDependencyService,
                            ConcurrencyGuard concurrencyGuard,
                            QueueRedisService queueRedisService,
                            TransactionTemplate transactionTemplate) {
        this.taskRepository = taskRepository;
        this.taskDependencyService = taskDependencyService;
        this.concurrencyGuard = concurrencyGuard;
        this.queueRedisService = queueRedisService;
        this.transactionTemplate = transactionTemplate;
    }

    public long submit(String taskNo, TaskSubmitRequest request) {
        SubmissionResult result = transactionTemplate.execute(status -> {
            LocalDateTime now = LocalDateTime.now();
            boolean hasDependencies = request.getDependencies() != null && !request.getDependencies().isEmpty();
            TaskStatus initialStatus = !hasDependencies && !request.getExecuteAt().isAfter(now)
                    ? TaskStatus.RUNNABLE
                    : TaskStatus.PENDING;
            long taskId = taskRepository.insert(taskNo, request, request.getExtInfo(), initialStatus);
            taskDependencyService.createDependencies(taskId, request.getDependencies(), now);
            SchedulerTask queueTask = hasDependencies ? taskDependencyService.refreshTaskAfterSubmit(taskId, now) : null;
            SchedulerTask task = taskRepository.findById(taskId)
                    .orElseThrow(() -> new IllegalStateException("task not found after insert: " + taskId));
            return new SubmissionResult(taskId, task, queueTask);
        });
        if (result == null) {
            throw new IllegalStateException("submit transaction returned null");
        }
        if (result.queueTask != null) {
            routeTaskToQueue(result.queueTask);
        } else if (result.task.getStatus() == TaskStatus.RUNNABLE) {
            routeTaskToQueue(result.task);
        }
        return result.taskId;
    }

    public long submitDirect(String taskNo,
                             TaskSubmitRequest request,
                             GroupConfig groupConfig,
                             int userLimit,
                             String instanceId,
                             String threadName,
                             String executeNo) {
        AtomicReference<DirectAcquireContext> acquiredRef = new AtomicReference<>();
        try {
            Long taskId = transactionTemplate.execute(status -> {
                LocalDateTime now = LocalDateTime.now();
                if (request.getDependencies() != null && !request.getDependencies().isEmpty()) {
                    throw new IllegalArgumentException("sync submit does not support dependencies");
                }
                if (request.getExecuteAt().isAfter(now)) {
                    throw new IllegalArgumentException("sync submit requires executeAt <= now");
                }
                long createdTaskId = taskRepository.insert(taskNo, request, request.getExtInfo(), TaskStatus.RUNNABLE);
                boolean acquired = concurrencyGuard.tryAcquire(
                        groupConfig.getGroupCode(),
                        request.getUserId(),
                        createdTaskId,
                        groupConfig.getMaxConcurrency(),
                        userLimit,
                        groupConfig.getLockExpireSec(),
                        executeNo
                );
                if (!acquired) {
                    throw new IllegalStateException("sync task is throttled by concurrency limit");
                }
                acquiredRef.set(new DirectAcquireContext(groupConfig.getGroupCode(), request.getUserId(), createdTaskId, executeNo));
                boolean running = taskRepository.casToRunning(createdTaskId, instanceId, threadName, now);
                if (!running) {
                    boolean released = concurrencyGuard.release(groupConfig.getGroupCode(), request.getUserId(), createdTaskId, executeNo);
                    acquiredRef.set(null);
                    if (!released) {
                        throw new IllegalStateException("sync submit failed to release concurrency after CAS miss, taskId=" + createdTaskId);
                    }
                    throw new IllegalStateException("sync submit failed to enter RUNNING state, taskId=" + createdTaskId);
                }
                return createdTaskId;
            });
            if (taskId == null) {
                throw new IllegalStateException("submitDirect transaction returned null");
            }
            acquiredRef.set(null);
            return taskId;
        } catch (RuntimeException ex) {
            DirectAcquireContext acquired = acquiredRef.getAndSet(null);
            if (acquired != null) {
                concurrencyGuard.release(acquired.groupCode(), acquired.userId(), acquired.taskId(), acquired.executeNo());
            }
            throw ex;
        }
    }

    public List<BatchSubmitResultItem> submitBatch(List<BatchSubmitCommand> commands) {
        BatchSubmissionResult result = transactionTemplate.execute(status -> {
            if (commands == null || commands.isEmpty()) {
                throw new IllegalArgumentException("batch commands is required");
            }
            LocalDateTime now = LocalDateTime.now();
            Map<String, Long> refToTaskId = new LinkedHashMap<>();
            List<CreatedTask> createdTasks = new ArrayList<>();
            List<BatchSubmitCommand> dependencyCommands = new ArrayList<>();
            for (BatchSubmitCommand command : commands) {
                boolean hasDependencies = command.dependencies() != null && !command.dependencies().isEmpty();
                TaskStatus initialStatus = !hasDependencies && !command.request().getExecuteAt().isAfter(now)
                        ? TaskStatus.RUNNABLE
                        : TaskStatus.PENDING;
                long taskId = taskRepository.insert(command.taskNo(), command.request(), command.request().getExtInfo(), initialStatus);
                refToTaskId.put(command.clientTaskRef(), taskId);
                SchedulerTask task = taskRepository.findById(taskId)
                        .orElseThrow(() -> new IllegalStateException("task not found after insert: " + taskId));
                createdTasks.add(new CreatedTask(command.clientTaskRef(), taskId, command.taskNo(), task, hasDependencies));
                if (hasDependencies) {
                    dependencyCommands.add(command);
                }
            }
            for (BatchSubmitCommand command : dependencyCommands) {
                Long taskId = refToTaskId.get(command.clientTaskRef());
                if (taskId == null) {
                    throw new IllegalStateException("clientTaskRef not found: " + command.clientTaskRef());
                }
                List<TaskDependencyRequest> resolved = resolveDependencies(command.dependencies(), refToTaskId);
                taskDependencyService.createDependencies(taskId, resolved, now);
            }
            List<SchedulerTask> queueTasks = new ArrayList<>();
            for (CreatedTask createdTask : createdTasks) {
                if (createdTask.hasDependencies) {
                    SchedulerTask queueTask = taskDependencyService.refreshTaskAfterSubmit(createdTask.taskId, now);
                    if (queueTask != null) {
                        queueTasks.add(queueTask);
                    }
                } else if (createdTask.task.getStatus() == TaskStatus.RUNNABLE) {
                    queueTasks.add(createdTask.task);
                }
            }
            List<BatchSubmitResultItem> items = createdTasks.stream()
                    .map(t -> new BatchSubmitResultItem(t.clientTaskRef, t.taskId, t.taskNo))
                    .toList();
            return new BatchSubmissionResult(items, queueTasks);
        });
        if (result == null) {
            throw new IllegalStateException("submitBatch transaction returned null");
        }
        enqueueTasks(result.queueTasks);
        return result.items;
    }

    public boolean cancel(String taskNo) {
        TerminalTransitionResult result = transactionTemplate.execute(status -> {
            SchedulerTask task = taskRepository.findByTaskNo(taskNo).orElse(null);
            if (task == null) {
                return new TerminalTransitionResult(false, List.of());
            }
            boolean cancelled = taskRepository.markCancelledByTaskNo(taskNo, LocalDateTime.now());
            if (!cancelled) {
                return new TerminalTransitionResult(false, List.of());
            }
            List<SchedulerTask> queueTasks = taskDependencyService.onUpstreamTaskTerminal(
                    task.getId(),
                    TaskStatus.CANCELLED,
                    LocalDateTime.now()
            );
            return new TerminalTransitionResult(true, queueTasks);
        });
        if (result == null || !result.changed) {
            return false;
        }
        enqueueTasks(result.queueTasks);
        return true;
    }

    public boolean markSuccess(Long taskId, LocalDateTime now) {
        return transitionTerminal(taskId, now, () -> taskRepository.markSuccess(taskId, now), TaskStatus.SUCCESS);
    }

    public boolean markFailed(Long taskId, String errorCode, String errorMsg, LocalDateTime now) {
        return transitionTerminal(taskId, now, () -> taskRepository.markFailed(taskId, errorCode, errorMsg, now), TaskStatus.FAILED);
    }

    public boolean markFailedByWaitDeadline(Long taskId, LocalDateTime now) {
        return transitionTerminal(
                taskId,
                now,
                () -> taskRepository.markFailedByWaitDeadline(taskId, WAIT_TIMEOUT_ERROR_CODE, WAIT_TIMEOUT_ERROR_MSG, now),
                TaskStatus.FAILED
        );
    }

    public boolean markTerminalByBusinessState(Long taskId, TaskStatus status, LocalDateTime now) {
        return transitionTerminal(taskId, now, () -> taskRepository.markTerminalByBusinessState(taskId, status, now), status);
    }

    public int expireWaitingTasks(int limit, LocalDateTime now) {
        List<Long> taskIds = taskRepository.findWaitingTimeoutTaskIds(now, limit);
        int expired = 0;
        for (Long taskId : taskIds) {
            if (markFailedByWaitDeadline(taskId, now)) {
                expired++;
            }
        }
        return expired;
    }

    private boolean transitionTerminal(Long taskId,
                                       LocalDateTime now,
                                       StatusUpdater updater,
                                       TaskStatus terminalStatus) {
        TerminalTransitionResult result = transactionTemplate.execute(tx -> {
            boolean changed = updater.update();
            if (!changed) {
                return new TerminalTransitionResult(false, List.of());
            }
            List<SchedulerTask> queueTasks = taskDependencyService.onUpstreamTaskTerminal(taskId, terminalStatus, now);
            return new TerminalTransitionResult(true, queueTasks);
        });
        if (result == null || !result.changed) {
            return false;
        }
        enqueueTasks(result.queueTasks);
        return true;
    }

    private void enqueueTasks(List<SchedulerTask> tasks) {
        for (SchedulerTask task : tasks) {
            if (task != null) {
                routeTaskToQueue(task);
            }
        }
    }

    private void routeTaskToQueue(SchedulerTask task) {
        LocalDateTime now = LocalDateTime.now();
        if (task.runnableStatus() && task.due(now)) {
            queueRedisService.enqueueReady(task);
            return;
        }
        queueRedisService.enqueue(task);
    }

    private List<TaskDependencyRequest> resolveDependencies(List<BatchSubmitDependencyRequest> dependencies,
                                                            Map<String, Long> refToTaskId) {
        List<TaskDependencyRequest> resolved = new ArrayList<>();
        for (BatchSubmitDependencyRequest dependency : dependencies) {
            Long dependsOnTaskId = dependency.getDependsOnTaskId();
            if (dependsOnTaskId == null) {
                dependsOnTaskId = refToTaskId.get(dependency.getDependsOnClientTaskRef());
                if (dependsOnTaskId == null) {
                    throw new IllegalArgumentException(
                            "dependency clientTaskRef not found: " + dependency.getDependsOnClientTaskRef());
                }
            }
            resolved.add(new TaskDependencyRequest(dependsOnTaskId, dependency.getTargetState()));
        }
        return resolved;
    }

    @FunctionalInterface
    private interface StatusUpdater {
        boolean update();
    }

    private record SubmissionResult(
            long taskId,
            SchedulerTask task,
            SchedulerTask queueTask
    ) {
    }

    private record TerminalTransitionResult(
            boolean changed,
            List<SchedulerTask> queueTasks
    ) {
    }

    public record BatchSubmitCommand(
            String clientTaskRef,
            String taskNo,
            TaskSubmitRequest request,
            List<BatchSubmitDependencyRequest> dependencies
    ) {
    }

    private record CreatedTask(
            String clientTaskRef,
            long taskId,
            String taskNo,
            SchedulerTask task,
            boolean hasDependencies
    ) {
    }

    private record BatchSubmissionResult(
            List<BatchSubmitResultItem> items,
            List<SchedulerTask> queueTasks
    ) {
    }

    private record DirectAcquireContext(
            String groupCode,
            String userId,
            long taskId,
            String executeNo
    ) {
    }
}
