package org.dong.scheduler.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TaskDependencyDispatchAction {
    public enum ActionType {
        ENQUEUE_TIME,
        ADD_READY
    }

    private ActionType actionType;
    private SchedulerTask task;
}
