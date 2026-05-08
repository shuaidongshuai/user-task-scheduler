package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;

public class TaskStateService {
    private final TaskRepository taskRepository;
    private final TaskDependencyService taskDependencyService;
    private final QueueRedisService queueRedisService;
    private final TransactionTemplate transactionTemplate;

    public TaskStateService(TaskRepository taskRepository,
                            TaskDependencyService taskDependencyService,
                            QueueRedisService queueRedisService,
                            TransactionTemplate transactionTemplate) {
        this.taskRepository = taskRepository;
        this.taskDependencyService = taskDependencyService;
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
            queueRedisService.enqueue(result.queueTask);
        } else if (result.task.getStatus() == TaskStatus.RUNNABLE) {
            queueRedisService.enqueue(result.task);
        }
        return result.taskId;
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

    public boolean markTerminalByBusinessState(Long taskId, TaskStatus status, LocalDateTime now) {
        return transitionTerminal(taskId, now, () -> taskRepository.markTerminalByBusinessState(taskId, status, now), status);
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
                queueRedisService.enqueue(task);
            }
        }
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
}
