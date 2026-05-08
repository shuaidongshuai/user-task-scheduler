package org.dong.scheduler.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TaskDependencySummary {
    private int totalCount;
    private int satisfiedCount;
    private int impossibleCount;

    public int waitingCount() {
        return Math.max(0, totalCount - satisfiedCount - impossibleCount);
    }

    public boolean allSatisfied() {
        return totalCount > 0 && satisfiedCount == totalCount && impossibleCount == 0;
    }

    public boolean hasImpossible() {
        return impossibleCount > 0;
    }
}
