package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class RecoveryService {
    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final SchedulerProperties properties;
    private final TaskRepository taskRepository;
    private final ConcurrencyGuard concurrencyGuard;
    private final QueueRedisService queueRedisService;

    public RecoveryService(SchedulerProperties properties,
                           TaskRepository taskRepository,
                           ConcurrencyGuard concurrencyGuard,
                           QueueRedisService queueRedisService) {
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.concurrencyGuard = concurrencyGuard;
        this.queueRedisService = queueRedisService;
    }

    public int recoverTimeoutRunning(String groupCode, int heartbeatTimeoutSec) {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(heartbeatTimeoutSec);
        List<SchedulerTask> timeoutTasks = taskRepository.findRunningHeartbeatTimeout(groupCode, cutoff, properties.getRecoveryScanLimit());
        int recovered = 0;
        if (!timeoutTasks.isEmpty()) {
            log.info("recovery scan timeout tasks, group={}, heartbeatTimeoutSec={}, matched={}",
                    groupCode, heartbeatTimeoutSec, timeoutTasks.size());
        } else {
            log.debug("recovery scan no timeout tasks, group={}, heartbeatTimeoutSec={}", groupCode, heartbeatTimeoutSec);
        }
        for (SchedulerTask task : timeoutTasks) {
            try {
                String lease = concurrencyGuard.leaseValue(task.getId());
                if (lease != null && !lease.isBlank()) {
                    log.debug("recovery skip task because lease still exists, taskId={}, taskNo={}, lease={}",
                            task.getId(), task.getTaskNo(), lease);
                    continue;
                }

                if (task.canRetry()) {
                    LocalDateTime nextRetryAt = LocalDateTime.now().plusSeconds(task.retryDelaySec(properties.getDefaultRetryDelaySec()));
                    boolean ok = taskRepository.markWaitRetry(task.getId(), nextRetryAt,
                            "HEARTBEAT_TIMEOUT", "heartbeat timeout recovered", LocalDateTime.now());
                    if (ok) {
                        concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                        taskRepository.findById(task.getId()).ifPresent(queueRedisService::enqueue);
                        log.warn("recovery moved task to WAIT_RETRY, taskId={}, taskNo={}, nextRetryAt={}",
                                task.getId(), task.getTaskNo(), nextRetryAt);
                        recovered++;
                    }
                } else {
                    boolean ok = taskRepository.markFailed(task.getId(), "HEARTBEAT_TIMEOUT", "heartbeat timeout recovered", LocalDateTime.now());
                    if (ok) {
                        concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                        log.error("recovery marked task FAILED after timeout, taskId={}, taskNo={}",
                                task.getId(), task.getTaskNo());
                        recovered++;
                    }
                }
            } catch (Exception e) {
                log.error("recover timeout task failed, taskId={}", task.getId(), e);
            }
        }
        return recovered;
    }

    public int refillQueue() {
        LocalDateTime now = LocalDateTime.now();
        taskRepository.promotePendingToRunnable(now, properties.getQueueRefillLimit());
        List<SchedulerTask> list = taskRepository.findRunnableForQueueRefill(now, properties.getQueueRefillLimit());
        int refillTime = 0;
        int refillReady = 0;
        for (SchedulerTask task : list) {
            if (task.getExecuteAt().isAfter(now)) {
                if (!queueRedisService.existsInTime(task.getGroupCode(), task.getId())) {
                    queueRedisService.enqueue(task);
                    refillTime++;
                }
                continue;
            }

            if (!queueRedisService.existsInReady(task.getGroupCode(), task.getId())) {
                queueRedisService.addToReady(task);
                refillReady++;
            }
            queueRedisService.removeFromTime(task.getGroupCode(), task.getId());
        }
        if (refillTime > 0 || refillReady > 0) {
            log.info("queue refill done, scanned={}, refillTime={}, refillReady={}", list.size(), refillTime, refillReady);
        } else {
            log.debug("queue refill no-op, scanned={}", list.size());
        }
        return refillTime + refillReady;
    }
}
