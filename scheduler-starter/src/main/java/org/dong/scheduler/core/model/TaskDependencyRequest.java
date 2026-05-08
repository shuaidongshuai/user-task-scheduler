package org.dong.scheduler.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.dong.scheduler.core.enums.DependencyTargetState;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class TaskDependencyRequest {
    private Long taskId;
    private DependencyTargetState targetState;
}
