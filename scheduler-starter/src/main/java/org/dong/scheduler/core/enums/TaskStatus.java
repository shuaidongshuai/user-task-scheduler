package org.dong.scheduler.core.enums;

public enum TaskStatus {
    PENDING,
    RUNNABLE,
    RUNNING,
    WAIT_RETRY,
    SUCCESS,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }
}
