package org.dong.scheduler.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.LocalDateTime;

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
    /**
     * Local dispatch route of current service. Blank means legacy mode without route isolation.
     */
    private String dispatchRoute;
    private int defaultGroupMaxConcurrency = 100;
    private int defaultGroupUserBaseConcurrency = 4;
    private int defaultGroupDispatchBatchSize = 20;
    private int defaultGroupHeartbeatTimeoutSec = 90;
    private int defaultGroupLockExpireSec = 120;
    private String defaultGroupDescription = "auto initialized default public group";
    private long dispatchIntervalMs = 500;
    private long waitTimeoutScanIntervalMs = 1000;
    private long recoveryIntervalMs = 30_000;
    private long queueRefillIntervalMs = 15_000;
    /**
     * Max ready-queue pages scanned in one dispatch round for a group.
     */
    private int readyScanPageLimit = 10;
    private int recoveryScanLimit = 200;
    private int queueRefillLimit = 500;
    private int activeUserLockTtlMs = 5_000;
    private LocalDateTime priorityBaseEpoch = LocalDateTime.of(2026, 1, 1, 0, 0);
    private int workerThreads = 16;
    private int maxWorkerThreads = 200;
    /**
     * Heartbeat scheduler threads. 0 means max(2, workerThreads / 4).
     */
    private int heartbeatThreads = 0;
    private int heartbeatIntervalSec = 10;
    private int timeoutMonitorThreads = 2;
    private int defaultRetryDelaySec = 15;
    private int defaultExecuteTimeoutSec = 600;
    private int timeoutInterruptGraceSec = 5;
    private int reconcileLockSec = 30;
    /**
     * Distributed lock ttl for scheduled jobs like expire/recover/refill.
     * Should be larger than the slowest expected single scan duration.
     */
    private int scheduledJobLockSec = 60;
    private int immediateReconcileThrottleSec = 3;
    private String instanceId;
}
