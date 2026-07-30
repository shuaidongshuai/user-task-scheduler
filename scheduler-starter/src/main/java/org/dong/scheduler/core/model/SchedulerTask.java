package org.dong.scheduler.core.model;

import lombok.Data;
import org.dong.scheduler.core.enums.TaskStatus;

import java.time.LocalDateTime;

@Data
public class SchedulerTask {
    private Long id;
    private String taskNo;
    private String groupCode;
    private String dispatchRoute;
    private String userId;
    private String bizType;
    private String bizKey;
    private TaskStatus status;
    private int priority;
    private LocalDateTime executeAt;
    private LocalDateTime nextRetryAt;
    private int retryCount;
    private int maxRetryCount;
    private int holdRoundCount;
    private int holdMaxRounds;
    private int holdRetryDelaySec;
    private Integer executeTimeoutSec;
    private Integer retryDelaySec;
    private Integer maxWaitSec;
    private LocalDateTime waitDeadlineAt;
    private LocalDateTime fallbackCheckAt;
    private int fallbackPolicyCount;
    private int groupFallbackCount;
    private String dispatcherInstance;
    private String workerInstance;
    private String workerThread;
    private LocalDateTime heartbeatTime;
    private LocalDateTime startTime;
    private LocalDateTime finishTime;
    private int version;
    private String errorCode;
    private String errorMsg;
    private String extInfo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public boolean canRetry() {
        return retryCount < maxRetryCount;
    }

    public boolean runnableStatus() {
        return status == TaskStatus.RUNNABLE || status == TaskStatus.WAIT_RETRY || status == TaskStatus.WAIT_HOLD;
    }

    public boolean due(LocalDateTime now) {
        return executeAt != null && !executeAt.isAfter(now);
    }

    public boolean waitingTimedOut(LocalDateTime now) {
        return waitDeadlineAt != null && !waitDeadlineAt.isAfter(now);
    }

    public int retryDelaySec(int defaultRetryDelaySec) {
        return retryDelaySec == null || retryDelaySec < 0 ? defaultRetryDelaySec : retryDelaySec;
    }

    public boolean holdRoundsExhausted() {
        return holdRoundCount >= holdMaxRounds;
    }
}
