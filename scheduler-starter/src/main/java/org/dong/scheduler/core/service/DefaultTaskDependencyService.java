package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskDependencySummary;
import org.dong.scheduler.core.repo.TaskDependencyRepository;
import org.dong.scheduler.core.repo.TaskRepository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DefaultTaskDependencyService implements TaskDependencyService {
    private static final String ERROR_CODE = "DEPENDENCY_NOT_SATISFIED";

    private final TaskRepository taskRepository;
    private final TaskDependencyRepository taskDependencyRepository;

    public DefaultTaskDependencyService(TaskRepository taskRepository,
                                        TaskDependencyRepository taskDependencyRepository) {
        this.taskRepository = taskRepository;
        this.taskDependencyRepository = taskDependencyRepository;
    }

    @Override
    public void createDependencies(Long taskId, List<TaskDependencyRequest> dependencies, LocalDateTime now) {
        taskDependencyRepository.batchInsert(taskId, dependencies, now);
    }

    @Override
    public SchedulerTask refreshTaskAfterSubmit(Long taskId, LocalDateTime now) {
        taskDependencyRepository.refreshByTaskId(taskId, now);
        return evaluateTask(taskId, now);
    }

    @Override
    public List<SchedulerTask> onUpstreamTaskTerminal(Long upstreamTaskId, TaskStatus actualStatus, LocalDateTime now) {
        taskDependencyRepository.updateByUpstreamTerminal(upstreamTaskId, actualStatus, now);
        List<Long> taskIds = taskDependencyRepository.findDependentTaskIds(upstreamTaskId);
        List<SchedulerTask> tasks = new ArrayList<>();
        for (Long taskId : taskIds) {
            SchedulerTask task = evaluateTask(taskId, now);
            if (task != null) {
                tasks.add(task);
            }
        }
        return tasks;
    }

    private SchedulerTask evaluateTask(Long taskId, LocalDateTime now) {
        SchedulerTask task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus().isTerminal() || task.getStatus() == TaskStatus.RUNNABLE
                || task.getStatus() == TaskStatus.RUNNING || task.getStatus() == TaskStatus.WAIT_RETRY) {
            return null;
        }

        TaskDependencySummary summary = taskDependencyRepository.summarize(taskId);
        if (summary.getTotalCount() == 0) {
            return null;
        }

        if (summary.hasImpossible()) {
            taskRepository.markFailedPendingByDependency(
                    taskId,
                    ERROR_CODE,
                    buildDependencyErrorMessage(taskId),
                    now
            );
            return null;
        }
        if (!summary.allSatisfied()) {
            return null;
        }

        if (task.due(now)) {
            boolean updated = taskRepository.markRunnableIfPending(taskId, now);
            if (!updated) {
                return null;
            }
            task.setStatus(TaskStatus.RUNNABLE);
        }
        return task;
    }

    private String buildDependencyErrorMessage(Long taskId) {
        return "dependency task status not satisfied: taskId=" + taskId;
    }
}
