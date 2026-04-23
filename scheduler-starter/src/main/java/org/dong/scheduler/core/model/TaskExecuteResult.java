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
    private final String extInfo;

    public static TaskExecuteResult success() {
        return new TaskExecuteResult(true, false, null, null, null);
    }

    public static TaskExecuteResult success(String extInfo) {
        return new TaskExecuteResult(true, false, null, null, extInfo);
    }

    public static TaskExecuteResult failed(String errorCode, String errorMsg, boolean retryable) {
        return new TaskExecuteResult(false, retryable, errorCode, errorMsg, null);
    }

    public static TaskExecuteResult failed(String errorCode, String errorMsg, boolean retryable, String extInfo) {
        return new TaskExecuteResult(false, retryable, errorCode, errorMsg, extInfo);
    }

    public boolean isSuccess() {
        return success;
    }
}
