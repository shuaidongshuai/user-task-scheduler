package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.UserConcurrencyConfig;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.repo.UserConcurrencyConfigRepository;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.TaskHandler;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DispatchService {
    private static final ZoneId SYSTEM_ZONE = ZoneId.systemDefault();
    private static final int SUMMARY_LOG_EVERY_N = 5;

    private final SchedulerProperties properties;
    private final GroupConfigRepository groupConfigRepository;
    private final UserConcurrencyConfigRepository userConcurrencyConfigRepository;
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
                           UserConcurrencyConfigRepository userConcurrencyConfigRepository,
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
        this.userConcurrencyConfigRepository = userConcurrencyConfigRepository;
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
                if (!Objects.equals(task.getGroupCode(), cfg.getGroupCode())
                        || !sameRoute(task.getDispatchRoute(), dispatchRoute)) {
                    log.debug("remove stale time queue member, taskId={}, queueGroup={}, dbGroup={}",
                            taskId, cfg.getGroupCode(), task.getGroupCode());
                    continue;
                }
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

        int pageSize = Math.max(1, cfg.getDispatchBatchSize());
        int dispatched = 0;
        int skipped = 0;
        boolean workerPoolSaturated = false;
        int scannedUsers = 0;
        while (scannedUsers < Math.max(1, properties.getReadyScanPageLimit())) {
            groupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());

            String userId = queueRedisService.peekNextActiveUser(cfg.getGroupCode(), dispatchRoute);
            if (userId == null || userId.isBlank()) {
                break;
            }
            scannedUsers++;
            String lockToken = queueRedisService.tryAcquireActiveUserLock(
                    cfg.getGroupCode(), dispatchRoute, userId, properties.getActiveUserLockTtlMs()
            );
            if (lockToken == null) {
                log.debug("dispatch skipped active-user lock, group={}, route={}, user={}",
                        cfg.getGroupCode(), dispatchRoute, userId);
                queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                continue;
            }

            try {
                Integer headPriority = queueRedisService.peekReadyHeadPriority(cfg.getGroupCode(), dispatchRoute, userId);
                if (headPriority == null) {
                    queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                    continue;
                }

                UserConcurrencyConfig userConfig = userConcurrencyConfigRepository
                        .findByUserIdAndGroupCode(userId, cfg.getGroupCode())
                        .orElse(null);
                long userRunning = concurrencyGuard.userRunning(cfg.getGroupCode(), userId);
                List<Long> ready = queueRedisService.peekReadyTasksByPriority(
                        cfg.getGroupCode(), dispatchRoute, userId, headPriority, pageSize
                );
                if (ready.isEmpty()) {
                    queueRedisService.rebalanceActiveUser(cfg.getGroupCode(), dispatchRoute, userId);
                    continue;
                }

                Map<Long, SchedulerTask> readyTasks = taskRepository.findByIds(ready);
                for (Long taskId : ready) {
                    SchedulerTask task = readyTasks.get(taskId);
                    if (task == null) {
                        queueRedisService.removeFromReadyQueue(cfg.getGroupCode(), dispatchRoute, userId, taskId);
                        skipped++;
                        continue;
                    }
                    if (!Objects.equals(task.getGroupCode(), cfg.getGroupCode())
                            || !sameRoute(task.getDispatchRoute(), dispatchRoute)
                            || !Objects.equals(task.getUserId(), userId)) {
                        queueRedisService.removeFromReadyQueue(cfg.getGroupCode(), dispatchRoute, userId, taskId);
                        skipped++;
                        log.debug("remove stale ready queue member, taskId={}, queueGroup={}, dbGroup={}",
                                taskId, cfg.getGroupCode(), task.getGroupCode());
                        continue;
                    }
                    boolean waitHoldTask = task.getStatus() == TaskStatus.WAIT_HOLD;
                    if (!waitHoldTask && groupRunning >= cfg.getMaxConcurrency()) {
                        break;
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

                    if (waitHoldTask && task.holdRoundsExhausted()) {
                        boolean failed = taskStateService.markFailed(
                                task.getId(),
                                "WAIT_HOLD_ROUNDS_EXHAUSTED",
                                "wait hold rounds exhausted",
                                currentNow
                        );
                        queueRedisService.removeFromReadyQueue(task);
                        if (failed) {
                            concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                        }
                        skipped++;
                        continue;
                    }

                    if (task.waitingTimedOut(currentNow)) {
                        boolean failed = taskStateService.markFailedByWaitDeadline(task.getId(), currentNow);
                        queueRedisService.removeFromReadyQueue(task);
                        skipped++;
                        if (failed) {
                            if (waitHoldTask) {
                                concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                            }
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
                            boolean changed = taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.SUCCESS, currentNow);
                            queueRedisService.removeFromReadyQueue(task);
                            if (changed && waitHoldTask) {
                                concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                            }
                            skipped++;
                            continue;
                        }
                        if (state == BusinessTaskState.FAILED) {
                            boolean changed = taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.FAILED, currentNow);
                            queueRedisService.removeFromReadyQueue(task);
                            if (changed && waitHoldTask) {
                                concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                            }
                            skipped++;
                            continue;
                        }
                        if (state != BusinessTaskState.NEED_RUNNING && state != BusinessTaskState.RUNNING) {
                            LocalDateTime nextCheckAt = waitHoldTask ? nextHoldTime(task) : nextRetryTime(task);
                            boolean deferred = waitHoldTask
                                    ? taskRepository.rollbackToWaitHold(task.getId(), nextCheckAt, currentNow)
                                    : taskRepository.rescheduleToRunnable(
                                            task.getId(),
                                            nextCheckAt,
                                            "BIZ_STATE_NOT_READY",
                                            "business state is " + state,
                                            currentNow
                                    );
                            queueRedisService.removeFromReadyQueue(task);
                            if (deferred) {
                                task.setStatus(waitHoldTask ? TaskStatus.WAIT_HOLD : TaskStatus.RUNNABLE);
                                task.setExecuteAt(nextCheckAt);
                                queueRedisService.enqueue(task);
                            }
                            skipped++;
                            continue;
                        }
                    }

                    boolean freshTask = !waitHoldTask;
                    String executeNo;
                    if (freshTask) {
                        int userLimit = dynamicUserLimitService.calculate(cfg, userConfig, groupRunning);
                        int groupRemaining = Math.max(0, cfg.getMaxConcurrency() - (int) groupRunning);
                        int userRemaining = Math.max(0, userLimit - (int) userRunning);
                        if (groupRemaining <= 0 || userRemaining <= 0) {
                            break;
                        }
                        executeNo = workerService.newExecuteNo();
                        boolean acquired = concurrencyGuard.tryAcquire(
                                cfg.getGroupCode(), task.getUserId(), task.getId(),
                                cfg.getMaxConcurrency(), userLimit, cfg.getLockExpireSec(), executeNo
                        );
                        if (!acquired) {
                            log.debug("dispatch skipped concurrency, taskId={}, taskNo={}, group={}, user={}, "
                                            + "groupRunning={}, groupLimit={}, userRunning={}, userLimit={}, version={}",
                                    task.getId(), task.getTaskNo(), cfg.getGroupCode(), task.getUserId(),
                                    groupRunning, cfg.getMaxConcurrency(), userRunning, userLimit, task.getVersion());
                            skipped++;
                            long latestGroupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
                            groupRunning = latestGroupRunning;
                            if (latestGroupRunning >= cfg.getMaxConcurrency()) {
                                break;
                            }
                            long latestUserRunning = concurrencyGuard.userRunning(cfg.getGroupCode(), task.getUserId());
                            userRunning = latestUserRunning;
                            if (latestUserRunning >= userLimit) {
                                break;
                            }
                            continue;
                        }
                    } else {
                        executeNo = workerService.newExecuteNo();
                        boolean leaseAcquired = concurrencyGuard.acquireLease(task.getId(), executeNo, cfg.getLockExpireSec());
                        if (!leaseAcquired) {
                            skipped++;
                            continue;
                        }
                    }

                    boolean cas = waitHoldTask
                            ? taskRepository.casWaitHoldToRunning(task.getId(), properties.getInstanceId(), Thread.currentThread().getName(), currentNow)
                            : taskRepository.casToRunning(task.getId(), task.getGroupCode(), task.getVersion(),
                                    properties.getInstanceId(), Thread.currentThread().getName(), currentNow);
                    if (!cas) {
                        if (freshTask) {
                            boolean released = concurrencyGuard.release(cfg.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                            if (!released) {
                                String currentLease = concurrencyGuard.leaseValue(task.getId());
                                log.warn("dispatch release mismatch, skip blind repair to avoid decrementing another execution "
                                                + "counters, taskId={}, taskNo={}, executeNo={}, currentLease={}, group={}, user={}",
                                        task.getId(), task.getTaskNo(), executeNo, currentLease, cfg.getGroupCode(), task.getUserId());
                                recoveryService.reconcileRunningCountersImmediately(cfg.getGroupCode(), task.getUserId(), "dispatch-cas-release-mismatch");
                            }
                        } else {
                            concurrencyGuard.releaseLease(task.getId(), executeNo);
                        }
                        skipped++;
                        continue;
                    }

                    try {
                        workerService.submit(task, cfg, executeNo);
                        queueRedisService.removeFromReadyQueue(task);
                        if (freshTask) {
                            groupRunning++;
                            userRunning++;
                        }
                        dispatched++;
                    } catch (RuntimeException ex) {
                        queueRedisService.removeFromReadyQueue(task);
                        if (freshTask) {
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
                            if (rollback) {
                                task.setExecuteAt(nextCheckAt);
                                queueRedisService.enqueue(task);
                            }
                        } else {
                            LocalDateTime nextHoldAt = nextHoldTime(task);
                            concurrencyGuard.releaseLease(task.getId(), executeNo);
                            boolean rollback = taskRepository.rollbackToWaitHold(task.getId(), nextHoldAt, currentNow);
                            if (rollback) {
                                task.setStatus(TaskStatus.WAIT_HOLD);
                                task.setExecuteAt(nextHoldAt);
                                queueRedisService.enqueue(task);
                            }
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

    private LocalDateTime nextHoldTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.getHoldRetryDelaySec());
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

    private boolean sameRoute(String left, String right) {
        String normalizedLeft = left == null || left.isBlank() ? null : left;
        String normalizedRight = right == null || right.isBlank() ? null : right;
        return Objects.equals(normalizedLeft, normalizedRight);
    }
}
