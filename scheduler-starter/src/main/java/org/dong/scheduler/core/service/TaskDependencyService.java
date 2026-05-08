package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;

import java.time.LocalDateTime;
import java.util.List;

public interface TaskDependencyService {
    void createDependencies(Long taskId, List<TaskDependencyRequest> dependencies, LocalDateTime now);

    SchedulerTask refreshTaskAfterSubmit(Long taskId, LocalDateTime now);

    List<SchedulerTask> onUpstreamTaskTerminal(Long upstreamTaskId, TaskStatus actualStatus, LocalDateTime now);
}
