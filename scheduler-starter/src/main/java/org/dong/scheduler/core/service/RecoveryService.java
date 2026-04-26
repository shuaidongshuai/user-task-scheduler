package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class RecoveryService {
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
                log.info("recovery timeout task hit, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}, status={}, executeAt={}, retryCount={}/{}",
                        task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType(),
                        task.getBizKey(), task.getStatus(), task.getExecuteAt(), task.getRetryCount(), task.getMaxRetryCount());
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
                        log.warn("recovery moved task to WAIT_RETRY, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}, nextRetryAt={}, retryCount={}/{}",
                                task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType(),
                                task.getBizKey(), nextRetryAt, task.getRetryCount() + 1, task.getMaxRetryCount());
                        recovered++;
                    }
                } else {
                    boolean ok = taskRepository.markFailed(task.getId(), "HEARTBEAT_TIMEOUT", "heartbeat timeout recovered", LocalDateTime.now());
                    if (ok) {
                        concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                        log.error("recovery marked task FAILED after timeout, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}, retryCount={}/{}",
                                task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType(),
                                task.getBizKey(), task.getRetryCount(), task.getMaxRetryCount());
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
                    log.info("queue refill task enqueued to time queue, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}, executeAt={}, status={}",
                            task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType(),
                            task.getBizKey(), task.getExecuteAt(), task.getStatus());
                }
                continue;
            }

            if (!queueRedisService.existsInReady(task.getGroupCode(), task.getId())) {
                queueRedisService.addToReady(task);
                refillReady++;
                log.info("queue refill task added to ready queue, taskId={}, taskNo={}, group={}, user={}, bizType={}, bizKey={}, executeAt={}, priority={}, status={}",
                        task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType(),
                        task.getBizKey(), task.getExecuteAt(), task.getPriority(), task.getStatus());
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

    public int reconcileRunningCountersIfNeeded(List<String> groupCodes) {
        String owner = properties.getInstanceId() + ":" + UUID.randomUUID();
        boolean locked = concurrencyGuard.tryAcquireReconcileLock(owner, properties.getReconcileLockSec());
        if (!locked) {
            log.debug("skip running counter reconcile, lock held by another instance");
            return 0;
        }
        int reconciledGroups = 0;
        try {
            for (String groupCode : groupCodes) {
                if (reconcileRunningCountersForGroup(groupCode)) {
                    reconciledGroups++;
                }
            }
            return reconciledGroups;
        } finally {
            concurrencyGuard.releaseReconcileLock(owner);
        }
    }

    public boolean reconcileRunningCountersImmediately(String groupCode, String userId, String trigger) {
        if (groupCode == null || groupCode.isBlank() || userId == null || userId.isBlank()) {
            return false;
        }
        boolean throttledIn = concurrencyGuard.tryAcquireGroupReconcileThrottle(
                groupCode, properties.getImmediateReconcileThrottleSec()
        );
        if (!throttledIn) {
            log.debug("skip immediate running counter reconcile by throttle, group={}, user={}, trigger={}",
                    groupCode, userId, trigger);
            return false;
        }

        String owner = properties.getInstanceId() + ":" + UUID.randomUUID();
        boolean locked = concurrencyGuard.tryAcquireReconcileLock(owner, properties.getReconcileLockSec());
        if (!locked) {
            log.debug("skip immediate running counter reconcile, lock held by another instance, group={}, user={}, trigger={}",
                    groupCode, userId, trigger);
            return false;
        }
        try {
            return reconcileRunningCountersForGroupAndUser(groupCode, userId, trigger);
        } finally {
            concurrencyGuard.releaseReconcileLock(owner);
        }
    }

    private boolean reconcileRunningCountersForGroup(String groupCode) {
        long dbGroupRunning = taskRepository.countRunningByGroup(groupCode);
        long redisGroupRunning = concurrencyGuard.groupRunning(groupCode);
        if (redisGroupRunning <= dbGroupRunning) {
            if (redisGroupRunning < dbGroupRunning) {
                log.warn("running counter drift detected but skipped (redis<db), group={}, redisGroupRunning={}, dbGroupRunning={}",
                        groupCode, redisGroupRunning, dbGroupRunning);
            }
            return false;
        }

        // Global reconcile baseline: only fix overflow (redis > db).
        Map<String, Long> dbUserRunning = taskRepository.countRunningByUserInGroup(groupCode);
        long remaining = redisGroupRunning - dbGroupRunning;
        long originalRemaining = remaining;

        // First pass: compare only DB users to avoid scanning all Redis user counters every time.
        for (Map.Entry<String, Long> entry : dbUserRunning.entrySet()) {
            if (remaining <= 0) {
                break;
            }
            String userId = entry.getKey();
            long dbUser = entry.getValue() == null ? 0L : entry.getValue();
            long redisUser = concurrencyGuard.userRunning(groupCode, userId);
            if (redisUser <= dbUser) {
                continue;
            }
            long expectedReduce = Math.min(redisUser - dbUser, remaining);
            long actualReduce = concurrencyGuard.reduceUserAndGroupRunning(groupCode, userId, expectedReduce);
            if (actualReduce <= 0) {
                continue;
            }
            remaining -= actualReduce;
            long redisAfter = concurrencyGuard.userRunning(groupCode, userId);
            log.warn("running counter reconciled by user (db user pass), group={}, user={}, redisBefore={}, "
                            + "dbTarget={}, expectedReduce={}, actualReduce={}, redisAfter={}, remainingOverflow={}",
                    groupCode, userId, redisUser, dbUser, expectedReduce, actualReduce, redisAfter, remaining);
        }

        // Fallback: only when overflow remains, scan Redis users and trim extra stale counters.
        if (remaining > 0) {
            Map<String, Long> redisUserRunning = concurrencyGuard.listUserRunning(groupCode);
            for (Map.Entry<String, Long> entry : redisUserRunning.entrySet()) {
                if (remaining <= 0) {
                    break;
                }
                String userId = entry.getKey();
                long redisUser = entry.getValue() == null ? 0L : entry.getValue();
                long dbUser = dbUserRunning.getOrDefault(userId, 0L);
                if (redisUser <= dbUser) {
                    continue;
                }
                long expectedReduce = Math.min(redisUser - dbUser, remaining);
                long actualReduce = concurrencyGuard.reduceUserAndGroupRunning(groupCode, userId, expectedReduce);
                if (actualReduce <= 0) {
                    continue;
                }
                remaining -= actualReduce;
                long redisAfter = concurrencyGuard.userRunning(groupCode, userId);
                log.warn("running counter reconciled by user (redis fallback pass), group={}, user={}, "
                                + "redisBefore={}, dbTarget={}, expectedReduce={}, actualReduce={}, "
                                + "redisAfter={}, remainingOverflow={}",
                        groupCode, userId, redisUser, dbUser, expectedReduce, actualReduce, redisAfter, remaining);
            }
        }

        // Final hard correction for any orphan overflow not covered by user counters.
        if (remaining > 0) {
            concurrencyGuard.setGroupRunning(groupCode, dbGroupRunning);
        }
        long redisGroupAfter = concurrencyGuard.groupRunning(groupCode);
        log.warn("running counters reconciled (redis>db), group={}, redisGroupBefore={}, dbGroup={}, redisGroupAfter={}, overflowBefore={}, remainingOverflow={}",
                groupCode, redisGroupRunning, dbGroupRunning, redisGroupAfter, originalRemaining, remaining);
        return true;
    }

    private boolean reconcileRunningCountersForGroupAndUser(String groupCode, String userId, String trigger) {
        long dbGroupRunning = taskRepository.countRunningByGroup(groupCode);
        long redisGroupRunning = concurrencyGuard.groupRunning(groupCode);
        if (redisGroupRunning <= dbGroupRunning) {
            return false;
        }

        long overflow = redisGroupRunning - dbGroupRunning;
        long dbUserRunning = taskRepository.countRunningByUserInGroup(groupCode, userId);
        long redisUserRunning = concurrencyGuard.userRunning(groupCode, userId);
        long reduced = 0L;
        if (redisUserRunning > dbUserRunning) {
            long expectedReduce = Math.min(redisUserRunning - dbUserRunning, overflow);
            reduced = concurrencyGuard.reduceUserAndGroupRunning(groupCode, userId, expectedReduce);
        }

        long redisUserAfter = concurrencyGuard.userRunning(groupCode, userId);
        if (redisUserAfter > dbUserRunning) {
            concurrencyGuard.setUserRunning(groupCode, userId, dbUserRunning);
        }

        long redisGroupAfterUserFix = concurrencyGuard.groupRunning(groupCode);
        if (redisGroupAfterUserFix > dbGroupRunning) {
            concurrencyGuard.setGroupRunning(groupCode, dbGroupRunning);
        }
        long redisGroupAfter = concurrencyGuard.groupRunning(groupCode);
        log.warn("running counters immediately reconciled by user hint, group={}, user={}, trigger={}, "
                        + "dbGroupRunning={}, redisGroupBefore={}, redisGroupAfter={}, dbUserRunning={}, "
                        + "redisUserBefore={}, redisUserAfter={}, reducedByScript={}",
                groupCode, userId, trigger, dbGroupRunning, redisGroupRunning, redisGroupAfter,
                dbUserRunning, redisUserRunning, concurrencyGuard.userRunning(groupCode, userId),
                reduced);
        return true;
    }
}
