package org.dong.scheduler.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {
    private boolean enabled = true;
    private long dispatchIntervalMs = 500;
    private long recoveryIntervalMs = 30_000;
    private long queueRefillIntervalMs = 15_000;
    private int recoveryScanLimit = 200;
    private int queueRefillLimit = 500;
    private int workerThreads = 16;
    private int heartbeatIntervalSec = 10;
    private int defaultRetryDelaySec = 15;
    private int defaultExecuteTimeoutSec = 600;
    private int timeoutInterruptGraceSec = 5;
    private int reconcileLockSec = 30;
    private String instanceId;
}
