package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;

import java.util.List;

public interface SchedulerClient {
    /**
     * 提交调度任务。
     *
     * <p>参数支持默认值：
     * <ul>
     *   <li>executeAt 默认当前时间（立即运行）</li>
     *   <li>maxRetryCount 默认 3</li>
     * </ul>
     *
     * <p>必填参数：groupCode、userId、bizType、bizKey。</p>
     */
    long submit(TaskSubmitRequest request);

    /**
     * 批量提交任务。批内任务及依赖在同一事务内落库；任一任务失败将整体回滚。
     */
    List<BatchSubmitResultItem> submitBatch(BatchSubmitRequest request);

    boolean cancel(String taskNo);
}
