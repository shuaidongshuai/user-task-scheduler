package org.dong.scheduler.core.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class TaskExecuteResult {
    private final boolean success;
    private final boolean retryable;
    private final String errorCode;
    private final String errorMsg;

    public static TaskExecuteResult success() {
        return new TaskExecuteResult(true, false, null, null);
    }

    /**
     * 失败结果。
     *
     * <p>说明：<code>TASK_TIMEOUT</code> 和 <code>TASK_TIMEOUT_UNINTERRUPTIBLE</code>
     * 为调度器内部保留错误码，由框架在超时控制逻辑中自动返回，业务侧通常无需主动返回这两个值。</p>
     */
    public static TaskExecuteResult failed(String errorCode, String errorMsg, boolean retryable) {
        return new TaskExecuteResult(false, retryable, errorCode, errorMsg);
    }

    public boolean isSuccess() {
        return success;
    }
}
