package org.dong.scheduler.core.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class TaskExecuteResult {
    private final boolean success;
    private final boolean retryable;
    private final boolean waitHold;
    private final String errorCode;
    private final String errorMsg;
    private final String nextGroupCode;

    public static TaskExecuteResult success() {
        return new TaskExecuteResult(true, false, false, null, null, null);
    }

    public static TaskExecuteResult waitHold() {
        return new TaskExecuteResult(false, false, true, null, null, null);
    }

    /**
     * 失败结果。
     *
     * <p>说明：<code>TASK_TIMEOUT</code> 和 <code>TASK_TIMEOUT_UNINTERRUPTIBLE</code>
     * 为调度器内部保留错误码，由框架在超时控制逻辑中自动返回，业务侧通常无需主动返回这两个值。</p>
     */
    public static TaskExecuteResult failed(String errorCode, String errorMsg, boolean retryable) {
        return new TaskExecuteResult(false, retryable, false, errorCode, errorMsg, null);
    }

    /**
     * 请求在本次执行失败后切换 group，并在目标 group 中重试。
     *
     * <p>本次执行仍占用并从当前 group 释放并发；只有目标 group 存在且启用时，
     * 调度器才会原子地更新 {@code group_code} 并将任务转为 {@code WAIT_RETRY}。</p>
     */
    public static TaskExecuteResult retryableOnGroup(String errorCode, String errorMsg, String nextGroupCode) {
        return new TaskExecuteResult(false, true, false, errorCode, errorMsg, nextGroupCode);
    }

    /**
     * 请求在目标 group 中重试，不记录错误原因。
     */
    public static TaskExecuteResult retryableOnGroup(String nextGroupCode) {
        return retryableOnGroup(null, null, nextGroupCode);
    }

    public boolean isSuccess() {
        return success;
    }
}
