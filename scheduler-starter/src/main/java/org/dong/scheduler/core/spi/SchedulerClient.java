package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.enums.SchedulerErrorCode;
import org.dong.scheduler.core.exception.SchedulerException;
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
     * 同步提交并在调用线程直接执行任务首轮。
     *
     * <p>语义说明：
     * <ul>
     *   <li>调用期间任务会完成落库、并发抢占、进入 RUNNING，并在当前线程直接执行一次。</li>
     *   <li>若首轮执行完成后任务状态为 {@code SUCCESS}，方法正常返回。</li>
     *   <li>若首轮执行返回 {@code WAIT_HOLD}，也视为本次同步提交成功，方法正常返回；
     *       但任务整体生命周期尚未结束，后续仍会由框架按 WAIT_HOLD 语义继续调度轮询。</li>
     *   <li>若首轮执行结束后状态为 {@code FAILED}/{@code WAIT_RETRY} 等非成功态，则方法抛异常。</li>
     * </ul>
     *
     * <p>因此，{@code executeSync} 的返回只表示“首轮同步执行已成功提交并完成本轮处理”，
     * 不保证任务整体生命周期已经结束。</p>
     *
     * <p>限流异常说明：</p>
     * <ul>
     *   <li>若提交时 group 或 user 并发已满，会抛出 {@link SchedulerException}。</li>
     *   <li>当前限流错误枚举为 {@link SchedulerErrorCode#CONCURRENCY_LIMIT}。</li>
     *   <li>当前限流错误码为 {@code 429}，可通过
     *       {@link SchedulerErrorCode#CONCURRENCY_LIMIT#getCode()} 获取。</li>
     * </ul>
     *
     * @throws SchedulerException 当同步提交触发 group/user 并发限流时抛出；
     *                            可通过 {@link SchedulerException#getErrorCode()} 判断具体错误
     */
    long executeSync(TaskSubmitRequest request);

    /**
     * 批量提交任务。批内任务及依赖在同一事务内落库；任一任务失败将整体回滚。
     */
    List<BatchSubmitResultItem> submitBatch(BatchSubmitRequest request);

    boolean cancel(String taskNo);
}
