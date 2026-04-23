package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;

/**
 * 业务任务执行器。
 *
 * <p>约束建议：
 * <ul>
 *   <li>必须保证业务幂等（调度系统是 at-least-once 语义）</li>
 *   <li>应尽量响应线程中断（超时后调度器会发送 interrupt）</li>
 *   <li>对外部IO调用建议设置超时，避免长期不可中断阻塞</li>
 * </ul>
 */
public interface TaskHandler {
    String bizType();

    TaskExecuteResult execute(SchedulerTask task) throws Exception;
}
