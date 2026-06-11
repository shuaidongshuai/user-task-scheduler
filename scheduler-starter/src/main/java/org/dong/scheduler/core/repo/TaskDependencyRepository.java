package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskDependencySummary;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface TaskDependencyRepository {
    void batchInsert(Long taskId, List<TaskDependencyRequest> dependencies, LocalDateTime now);

    void refreshByTaskId(Long taskId, LocalDateTime now);

    List<Long> findDependentTaskIds(Long dependsOnTaskId);

    void updateByUpstreamTerminal(Long dependsOnTaskId, TaskStatus actualStatus, LocalDateTime now);

    TaskDependencySummary summarize(Long taskId);

    Optional<Long> findFirstImpossibleDependsOnTaskId(Long taskId);
}
