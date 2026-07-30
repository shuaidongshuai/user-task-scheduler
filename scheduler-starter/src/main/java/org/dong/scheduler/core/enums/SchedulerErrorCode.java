package org.dong.scheduler.core.enums;

public enum SchedulerErrorCode {
    CONCURRENCY_LIMIT(429, "sync task is throttled by concurrency limit"),
    SYNC_FALLBACK_CHECK_UNSUPPORTED(4001, "executeSync does not support fallbackCheckAt");

    private final int code;
    private final String message;

    SchedulerErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
