package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;

import java.util.List;

/**
 * 业务任务执行器。
 *
 * <p>约束建议：
 * <ul>
 *   <li>必须保证业务幂等（调度系统是 at-least-once 语义）</li>
 *   <li>应尽量响应线程中断（超时后调度器会发送 interrupt）</li>
 *   <li>对外部IO调用建议设置超时，避免长期不可中断阻塞</li>
 * </ul>
 *
 * <p>超时相关说明：
 * <ul>
 *   <li>若执行超时且线程在中断宽限期内退出，调度器会产生错误码 <code>TASK_TIMEOUT</code>（可重试）</li>
 *   <li>若执行超时且线程在中断宽限期后仍未退出，调度器会产生错误码 <code>TASK_TIMEOUT_UNINTERRUPTIBLE</code></li>
 *   <li><code>TASK_TIMEOUT_UNINTERRUPTIBLE</code> 表示当前执行线程可能仍在运行，后续重试可能与旧执行并发，业务必须保证幂等</li>
 * </ul>
 */
public interface TaskHandler {
    List<String> bizTypes();

    TaskExecuteResult execute(SchedulerTask task) throws Exception;
}
