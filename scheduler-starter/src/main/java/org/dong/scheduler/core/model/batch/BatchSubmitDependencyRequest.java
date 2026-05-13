package org.dong.scheduler.core.model.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.dong.scheduler.core.enums.DependencyTargetState;

/**
 * 批量提交中的依赖参数。
 *
 * <p>依赖锚点二选一：
 * <ul>
 *   <li>dependsOnTaskId: 依赖已存在任务</li>
 *   <li>dependsOnClientTaskRef: 依赖当前批次内任务</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class BatchSubmitDependencyRequest {
    /**
     * 依赖已存在任务ID（与 dependsOnClientTaskRef 二选一）。
     * 依赖历史任务时必填。
     */
    private Long dependsOnTaskId;
    /**
     * 依赖批内任务引用（与 dependsOnTaskId 二选一）。
     * 依赖同批次任务时必填。
     */
    private String dependsOnClientTaskRef;
    /**
     * 依赖目标状态（必填）。
     */
    private DependencyTargetState targetState;
}
