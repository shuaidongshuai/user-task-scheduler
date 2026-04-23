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

    public static TaskExecuteResult failed(String errorCode, String errorMsg, boolean retryable) {
        return new TaskExecuteResult(false, retryable, errorCode, errorMsg);
    }

    public boolean isSuccess() {
        return success;
    }
}
