package org.dong.scheduler.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "utask.scheduler")
public class SchedulerProperties {
    private boolean enabled = true;
    /**
     * Whether scheduled dispatch/recovery/refill loops should run.
     * Keep true by default; set false to pause scheduling while preserving submit APIs.
     */
    private boolean dispatchEnabled = true;
    private boolean autoInitDefaultGroup = true;
    private String defaultGroupCode = "public-group";
    private int defaultGroupMaxConcurrency = 100;
    private int defaultGroupUserBaseConcurrency = 4;
    private int defaultGroupDispatchBatchSize = 100;
    private int defaultGroupHeartbeatTimeoutSec = 90;
    private int defaultGroupLockExpireSec = 120;
    private String defaultGroupDescription = "auto initialized default public group";
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
    private int immediateReconcileThrottleSec = 3;
    private String instanceId;
}
