package org.dong.scheduler.core.model.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * 批量任务提交参数。
 *
 * <p>事务语义：
 * <ul>
 *   <li>同一批次内任务、依赖关系在一个事务中提交</li>
 *   <li>任一任务或依赖落库失败时，整批回滚</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class BatchSubmitRequest {
    /**
     * 批量任务列表（必填，不能为空）。
     */
    private List<BatchSubmitTaskRequest> tasks;
}
