package org.dong.scheduler.core.exception;

import lombok.Getter;
import org.dong.scheduler.core.enums.SchedulerErrorCode;

@Getter
public class SchedulerException extends IllegalStateException {
    private final int errorCode;

    public SchedulerException(SchedulerErrorCode error) {
        this(error, error.getMessage());
    }

    public SchedulerException(SchedulerErrorCode error, String message) {
        super(message);
        this.errorCode = error.getCode();
    }

    public static SchedulerException concurrencyLimit() {
        return new SchedulerException(SchedulerErrorCode.CONCURRENCY_LIMIT);
    }

    public static SchedulerException concurrencyLimit(String message) {
        return new SchedulerException(SchedulerErrorCode.CONCURRENCY_LIMIT, message);
    }

}
