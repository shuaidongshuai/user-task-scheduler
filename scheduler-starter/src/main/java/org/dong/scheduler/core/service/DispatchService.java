package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.core.task.TaskRejectedException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DispatchService {
    private static final ZoneId SYSTEM_ZONE = ZoneId.systemDefault();
    private static final int SUMMARY_LOG_EVERY_N = 5;

    private final SchedulerProperties properties;
    private final GroupConfigRepository groupConfigRepository;
    private final TaskRepository taskRepository;
    private final QueueRedisService queueRedisService;
    private final ConcurrencyGuard concurrencyGuard;
    private final DynamicUserLimitService dynamicUserLimitService;
    private final WorkerService workerService;
    private final RecoveryService recoveryService;
    private final TaskHandlerRegistry handlerRegistry;
    private final BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry;
    private final TaskStateService taskStateService;
    private final ConcurrentHashMap<String, AtomicInteger> groupSummaryLogCounter = new ConcurrentHashMap<>();

    public DispatchService(SchedulerProperties properties,
                           GroupConfigRepository groupConfigRepository,
                           TaskRepository taskRepository,
                           QueueRedisService queueRedisService,
                           ConcurrencyGuard concurrencyGuard,
                           DynamicUserLimitService dynamicUserLimitService,
                           WorkerService workerService,
                           RecoveryService recoveryService,
                           TaskHandlerRegistry handlerRegistry,
                           BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry,
                           TaskStateService taskStateService) {
        this.properties = properties;
        this.groupConfigRepository = groupConfigRepository;
        this.taskRepository = taskRepository;
        this.queueRedisService = queueRedisService;
        this.concurrencyGuard = concurrencyGuard;
        this.dynamicUserLimitService = dynamicUserLimitService;
        this.workerService = workerService;
        this.recoveryService = recoveryService;
        this.handlerRegistry = handlerRegistry;
        this.businessTaskStateProviderRegistry = businessTaskStateProviderRegistry;
        this.taskStateService = taskStateService;
    }

    public void dispatchOnce() {
        List<GroupConfig> groups = groupConfigRepository.listEnabled();
        long nowMillis = LocalDateTime.now().atZone(SYSTEM_ZONE).toInstant().toEpochMilli();
        log.debug("dispatch tick start, enabledGroups={}", groups.size());

        for (GroupConfig cfg : groups) {
            try {
                dispatchGroup(cfg, nowMillis);
            } catch (Exception e) {
                log.error("dispatch group failed, group={}", cfg.getGroupCode(), e);
            }
        }
        log.debug("dispatch tick end");
    }

    private void dispatchGroup(GroupConfig cfg, long nowMillis) {
        long begin = System.currentTimeMillis();
        String dispatchRoute = properties.getDispatchRoute();
        List<Long> dueTaskIds = queueRedisService.promoteDueTasks(cfg.getGroupCode(), dispatchRoute, nowMillis, cfg.getDispatchBatchSize());
        Map<Long, SchedulerTask> dueTasks = dueTaskIds.isEmpty() ? Map.of() : taskRepository.findByIds(dueTaskIds);
        int promoted = 0;
        for (Long taskId : dueTaskIds) {
            boolean addedToReady = false;
            SchedulerTask task = dueTasks.get(taskId);
            if (task != null) {
                LocalDateTime currentNow = LocalDateTime.now();
                if (task.getStatus() == TaskStatus.PENDING) {
                    boolean promotedNow = taskRepository.markRunnableIfPending(task.getId(), currentNow);
                    if (promotedNow) {
                        task.setStatus(TaskStatus.RUNNABLE);
                    }
                }
                if (task.runnableStatus() && task.due(currentNow)) {
                    queueRedisService.enqueueReady(task);
                    addedToReady = true;
                } else if (task.getExecuteAt() != null && task.getExecuteAt().isAfter(currentNow)) {
                    queueRedisService.enqueue(task);
                }
                if (!addedToReady) {
                    log.warn("promoted due task was not added to ready queue, taskId={}, taskNo={}, group={}, user={}, "
                                    + "status={}, executeAt={}, dueNow={}, removedFromTimeQueue=true",
                            task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(),
                            task.getStatus(), task.getExecuteAt(), task.due(currentNow));
                }
            } else {
                log.warn("promoted due task missing in database, taskId={}, group={}, removedFromTimeQueue=true",
                        taskId, cfg.getGroupCode());
            }
            promoted++;
        }

        long groupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
        if (groupRunning >= cfg.getMaxConcurrency()) {
            log.info("dispatch group skipped by full concurrency, group={}, groupRunning={}, groupMax={}",
                    cfg.getGroupCode(), groupRunning, cfg.getMaxConcurrency());
            return;
        }

        int pageSize = Math.max(1, cfg.getDispatchBatchSize());
        int dispatched = 0;
        int skipped = 0;
        boolean workerPoolSaturated = false;
        while (groupRunning < cfg.getMaxConcurrency()) {
            groupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
            if (groupRunning >= cfg.getMaxConcurrency()) {
                break;
            }

            String userId = queueRedisService.peekNextActiveUser(cfg.getGroupCode(), dispatchRoute);
            if (userId == null || userId.isBlank()) {
                break;
            }
            String lockToken = queueRedisService.tryAcquireActiveUserLock(
                    cfg.getGroupCode(), dispatchRoute, userId, properties.getActiveUserLockTtlMs()
            );
            if (lockToken == null) {
                queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                continue;
            }

            try {
                Integer headPriority = queueRedisService.peekReadyHeadPriority(cfg.getGroupCode(), dispatchRoute, userId);
                if (headPriority == null) {
                    queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                    continue;
                }

                int userLimit = dynamicUserLimitService.calculate(cfg, groupRunning);
                long userRunning = concurrencyGuard.userRunning(cfg.getGroupCode(), userId);
                int groupRemaining = Math.max(0, cfg.getMaxConcurrency() - (int) groupRunning);
                int userRemaining = Math.max(0, userLimit - (int) userRunning);
                int batchLimit = Math.min(pageSize, Math.min(groupRemaining, userRemaining));
                if (batchLimit <= 0) {
                    queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                    continue;
                }

                List<Long> ready = queueRedisService.peekReadyTasksByPriority(
                        cfg.getGroupCode(), dispatchRoute, userId, headPriority, batchLimit
                );
                if (ready.isEmpty()) {
                    queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                    continue;
                }

                Map<Long, SchedulerTask> readyTasks = taskRepository.findByIds(ready);
                boolean stopCurrentUser = false;
                for (Long taskId : ready) {
                    if (groupRunning >= cfg.getMaxConcurrency()) {
                        break;
                    }

                    SchedulerTask task = readyTasks.get(taskId);
                    if (task == null) {
                        skipped++;
                        continue;
                    }
                    LocalDateTime currentNow = LocalDateTime.now();
                    if (!task.runnableStatus() || !task.due(currentNow)) {
                        queueRedisService.removeFromReadyQueue(task);
                        if (task.runnableStatus() && task.getExecuteAt() != null && task.getExecuteAt().isAfter(currentNow)) {
                            queueRedisService.enqueue(task);
                        }
                        skipped++;
                        continue;
                    }

                    if (task.waitingTimedOut(currentNow)) {
                        boolean failed = taskStateService.markFailedByWaitDeadline(task.getId(), currentNow);
                        queueRedisService.removeFromReadyQueue(task);
                        skipped++;
                        if (failed) {
                            log.info("dispatch skipped timed out task before running, taskId={}, taskNo={}, group={}, user={}",
                                    task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId());
                        }
                        continue;
                    }

                    TaskHandler handler = handlerRegistry.find(task.getBizType());
                    if (handler == null) {
                        skipped++;
                        log.warn("dispatch skip task because no TaskHandler found in current service, taskId={}, taskNo={}, group={}, user={}, bizType={}",
                                task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), task.getBizType());
                        continue;
                    }

                    BusinessTaskStateProvider stateProvider = businessTaskStateProviderRegistry.find(task.getBizType());
                    if (stateProvider != null) {
                        BusinessTaskState state = stateProvider.query(task);
                        if (state == BusinessTaskState.SUCCESS) {
                            taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.SUCCESS, currentNow);
                            queueRedisService.removeFromReadyQueue(task);
                            skipped++;
                            continue;
                        }
                        if (state == BusinessTaskState.FAILED) {
                            taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.FAILED, currentNow);
                            queueRedisService.removeFromReadyQueue(task);
                            skipped++;
                            continue;
                        }
                        if (state != BusinessTaskState.NEED_RUNNING && state != BusinessTaskState.RUNNING) {
                            LocalDateTime nextCheckAt = nextRetryTime(task);
                            boolean deferred = taskRepository.rescheduleToRunnable(
                                    task.getId(),
                                    nextCheckAt,
                                    "BIZ_STATE_NOT_READY",
                                    "business state is " + state,
                                    currentNow
                            );
                            queueRedisService.removeFromReadyQueue(task);
                            if (deferred) {
                                task.setExecuteAt(nextCheckAt);
                                queueRedisService.enqueue(task);
                            }
                            skipped++;
                            continue;
                        }
                    }

                    userLimit = dynamicUserLimitService.calculate(cfg, groupRunning);
                    String executeNo = workerService.newExecuteNo();
                    boolean acquired = concurrencyGuard.tryAcquire(
                            cfg.getGroupCode(), task.getUserId(), task.getId(),
                            cfg.getMaxConcurrency(), userLimit, cfg.getLockExpireSec(), executeNo
                    );
                    if (!acquired) {
                        skipped++;
                        long latestGroupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
                        if (latestGroupRunning >= cfg.getMaxConcurrency()) {
                            groupRunning = latestGroupRunning;
                            break;
                        }
                        long latestUserRunning = concurrencyGuard.userRunning(cfg.getGroupCode(), task.getUserId());
                        if (latestUserRunning >= userLimit) {
                            stopCurrentUser = true;
                            break;
                        }
                        continue;
                    }

                    boolean cas = taskRepository.casToRunning(task.getId(), properties.getInstanceId(), Thread.currentThread().getName(), currentNow);
                    if (!cas) {
                        boolean released = concurrencyGuard.release(cfg.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                        if (!released) {
                            String currentLease = concurrencyGuard.leaseValue(task.getId());
                            log.warn("dispatch release mismatch, skip blind repair to avoid decrementing another execution "
                                            + "counters, taskId={}, taskNo={}, executeNo={}, currentLease={}, group={}, user={}",
                                    task.getId(), task.getTaskNo(), executeNo, currentLease, cfg.getGroupCode(), task.getUserId());
                            recoveryService.reconcileRunningCountersImmediately(cfg.getGroupCode(), task.getUserId(), "dispatch-cas-release-mismatch");
                        }
                        skipped++;
                        continue;
                    }

                    try {
                        workerService.submit(task, cfg, executeNo);
                        queueRedisService.removeFromReadyQueue(task);
                        groupRunning++;
                        dispatched++;
                    } catch (RuntimeException ex) {
                        boolean released = concurrencyGuard.release(cfg.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                        if (!released) {
                            String currentLease = concurrencyGuard.leaseValue(task.getId());
                            log.warn("dispatch submit rollback release mismatch, skip blind repair to avoid decrementing "
                                            + "another execution counters, taskId={}, taskNo={}, executeNo={}, "
                                            + "currentLease={}, group={}, user={}",
                                    task.getId(), task.getTaskNo(), executeNo, currentLease, cfg.getGroupCode(), task.getUserId());
                            recoveryService.reconcileRunningCountersImmediately(cfg.getGroupCode(), task.getUserId(), "dispatch-submit-release-mismatch");
                        }
                        LocalDateTime nextCheckAt = nextRetryTime(task);
                        boolean rollback = taskRepository.rescheduleToRunnable(
                                task.getId(),
                                nextCheckAt,
                                "DISPATCH_SUBMIT_REJECTED",
                                ex.getClass().getSimpleName() + ":" + ex.getMessage(),
                                currentNow
                        );
                        queueRedisService.removeFromReadyQueue(task);
                        if (rollback) {
                            task.setExecuteAt(nextCheckAt);
                            queueRedisService.enqueue(task);
                        }
                        skipped++;
                        if (isWorkerPoolRejected(ex)) {
                            workerPoolSaturated = true;
                            break;
                        }
                    }
                }

                queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                if (workerPoolSaturated) {
                    break;
                }
                if (stopCurrentUser) {
                    continue;
                }
            } finally {
                queueRedisService.releaseActiveUserLock(cfg.getGroupCode(), dispatchRoute, userId, lockToken);
            }
        }

        if (dispatched > 0 || groupRunning > 0) {
            AtomicInteger counter = groupSummaryLogCounter.computeIfAbsent(cfg.getGroupCode(), key -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            if (current % SUMMARY_LOG_EVERY_N != 0) {
                return;
            }
            log.info("dispatch group summary, group={}, promoted={}, userBatchSize={}, dispatched={}, skipped={}, "
                            + "groupRunning={}, costMs={}",
                    cfg.getGroupCode(), promoted, pageSize, dispatched, skipped,
                    groupRunning, System.currentTimeMillis() - begin);
        }
    }

    private LocalDateTime nextRetryTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.retryDelaySec(properties.getDefaultRetryDelaySec()));
    }

    private boolean isWorkerPoolRejected(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof RejectedExecutionException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
