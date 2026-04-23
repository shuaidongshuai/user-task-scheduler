package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.model.TaskSubmitRequest;

public interface SchedulerClient {
    /**
     * 提交调度任务。
     *
     * <p>参数支持默认值：
     * <ul>
     *   <li>executeAt 默认当前时间（立即运行）</li>
     *   <li>maxRetryCount 默认 3</li>
     *   <li>taskNo 为空时自动生成</li>
     * </ul>
     *
     * <p>必填参数：groupCode、userId、bizType。</p>
     */
    long submit(TaskSubmitRequest request);

    boolean cancel(String taskNo);
}
