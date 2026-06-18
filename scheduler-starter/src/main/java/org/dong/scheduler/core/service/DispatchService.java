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
import java.util.HashSet;
import java.util.Set;
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
        LocalDateTime now = LocalDateTime.now();
        String dispatchRoute = properties.getDispatchRoute();
        List<Long> dueTaskIds = queueRedisService.promoteDueTasks(cfg.getGroupCode(), dispatchRoute, nowMillis, cfg.getDispatchBatchSize());
        Map<Long, SchedulerTask> dueTasks = dueTaskIds.isEmpty() ? Map.of() : taskRepository.findByIds(dueTaskIds);
        int promoted = 0;
        for (Long taskId : dueTaskIds) {
            boolean addedToReady = false;
            SchedulerTask task = dueTasks.get(taskId);
            if (task != null) {
                if (task.getStatus() == TaskStatus.PENDING) {
                    boolean promotedNow = taskRepository.markRunnableIfPending(task.getId(), now);
                    if (promotedNow) {
                        task.setStatus(TaskStatus.RUNNABLE);
                    }
                }
                if (task.runnableStatus()) {
                    queueRedisService.addToReady(task);
                    addedToReady = true;
                } else if (task.getExecuteAt() != null && task.getExecuteAt().isAfter(now)
                        && task.getStatus() == TaskStatus.PENDING) {
                    queueRedisService.enqueue(task);
                }
                if (!addedToReady) {
                    log.warn("promoted due task was not added to ready queue, taskId={}, taskNo={}, group={}, user={}, "
                                    + "status={}, executeAt={}, dueNow={}, removedFromTimeQueue=true",
                            task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(),
                            task.getStatus(), task.getExecuteAt(), task.due(now));
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
        int offset = 0;
        Set<String> saturatedUsers = new HashSet<>();
        int dispatched = 0;
        int skipped = 0;
        int readyScanned = 0;
        int readyScanPages = 0;
        int readyScanPageLimit = Math.max(1, properties.getReadyScanPageLimit());
        boolean workerPoolSaturated = false;
        while (groupRunning < cfg.getMaxConcurrency() && readyScanPages < readyScanPageLimit) {
            groupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
            if (groupRunning >= cfg.getMaxConcurrency()) {
                break;
            }

            List<Long> ready = queueRedisService.peekReady(cfg.getGroupCode(), dispatchRoute, offset, pageSize);
            if (ready == null || ready.isEmpty()) {
                break;
            }
            readyScanPages++;
            readyScanned += ready.size();
            Map<Long, SchedulerTask> readyTasks = taskRepository.findByIds(ready);
            boolean progressed = false;
            for (Long taskId : ready) {
                if (groupRunning >= cfg.getMaxConcurrency()) {
                    break;
                }

                SchedulerTask task = readyTasks.get(taskId);
                if (task == null) {
                    queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                    progressed = true;
                    skipped++;
                    log.debug("dispatch skip missing task, group={}, taskId={}", cfg.getGroupCode(), taskId);
                    continue;
                }
                if (!task.runnableStatus() || !task.due(now)) {
                    queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                    progressed = true;
                    skipped++;
                    log.debug("dispatch skip non-runnable/non-due, group={}, taskId={}, status={}, executeAt={}",
                            cfg.getGroupCode(), taskId, task.getStatus(), task.getExecuteAt());
                    continue;
                }

                if (task.waitingTimedOut(now)) {
                    boolean failed = taskStateService.markFailedByWaitDeadline(task.getId(), now);
                    queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                    progressed = true;
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
                        taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.SUCCESS, now);
                        queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                        progressed = true;
                        skipped++;
                        log.info("dispatch short-circuit success by biz state, taskId={}, taskNo={}, group={}",
                                task.getId(), task.getTaskNo(), task.getGroupCode());
                        continue;
                    }
                    if (state == BusinessTaskState.FAILED) {
                        taskStateService.markTerminalByBusinessState(task.getId(), TaskStatus.FAILED, now);
                        queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                        progressed = true;
                        skipped++;
                        log.info("dispatch short-circuit failed by biz state, taskId={}, taskNo={}, group={}",
                                task.getId(), task.getTaskNo(), task.getGroupCode());
                        continue;
                    }
                    if (state != BusinessTaskState.NEED_RUNNING && state != BusinessTaskState.RUNNING) {
                        LocalDateTime nextCheckAt = nextRetryTime(task);
                        boolean deferred = taskRepository.rescheduleToRunnable(
                                task.getId(),
                                nextCheckAt,
                                "BIZ_STATE_NOT_READY",
                                "business state is " + state,
                                now
                        );
                        queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, taskId);
                        progressed = true;
                        if (deferred) {
                            task.setExecuteAt(nextCheckAt);
                            queueRedisService.enqueue(task);
                        }
                        skipped++;
                        log.info("dispatch deferred by biz state, taskId={}, taskNo={}, state={}, nextCheckAt={}",
                                task.getId(), task.getTaskNo(), state, nextCheckAt);
                        continue;
                    }
                }

                if (saturatedUsers.contains(task.getUserId())) {
                    skipped++;
                    log.debug("dispatch skip saturated user in current round, taskId={}, taskNo={}, group={}, user={}",
                            task.getId(), task.getTaskNo(), cfg.getGroupCode(), task.getUserId());
                    continue;
                }

                int userLimit = dynamicUserLimitService.calculate(cfg, groupRunning);
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
                    long userRunning = concurrencyGuard.userRunning(cfg.getGroupCode(), task.getUserId());
                    if (userRunning >= userLimit) {
                        saturatedUsers.add(task.getUserId());
                    }
                    log.info("dispatch acquire false, taskId={}, taskNo={}, group={}, user={}, groupRunning={}, groupMax={}, userLimit={}",
                            task.getId(), task.getTaskNo(), cfg.getGroupCode(), task.getUserId(), groupRunning,
                            cfg.getMaxConcurrency(), userLimit);
                    continue;
                }

                boolean cas = taskRepository.casToRunning(task.getId(), properties.getInstanceId(), Thread.currentThread().getName(), now);
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
                    log.debug("dispatch CAS to RUNNING failed, taskId={}, taskNo={}, group={}",
                            task.getId(), task.getTaskNo(), cfg.getGroupCode());
                    continue;
                }

                try {
                    workerService.submit(task, cfg, executeNo);
                    queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, task.getId());
                    progressed = true;
                    groupRunning++;
                    dispatched++;
                    log.info("task dispatched, taskId={}, taskNo={}, executeNo={}, group={}, user={}, priority={}, groupRunningAfter={}",
                            task.getId(), task.getTaskNo(), executeNo, task.getGroupCode(), task.getUserId(), task.getPriority(), groupRunning);
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
                            now
                    );
                    queueRedisService.removeFromReady(cfg.getGroupCode(), dispatchRoute, task.getId());
                    progressed = true;
                    if (rollback) {
                        task.setExecuteAt(nextCheckAt);
                        queueRedisService.enqueue(task);
                    }
                    skipped++;
                    log.warn("dispatch submit rejected and rolled back by worker pool capacity, taskId={}, taskNo={}, "
                                    + "executeNo={}, rollback={}, nextCheckAt={}",
                            task.getId(), task.getTaskNo(), executeNo, rollback, nextCheckAt, ex);
                    if (isWorkerPoolRejected(ex)) {
                        workerPoolSaturated = true;
                        log.info("dispatch stop current round because worker pool is saturated, group={}, taskId={}, taskNo={}",
                                cfg.getGroupCode(), task.getId(), task.getTaskNo());
                        break;
                    }
                }
            }
            if (workerPoolSaturated) {
                break;
            }
            if (!progressed) {
                offset += ready.size();
            } else {
                offset = Math.max(0, offset - ready.size());
            }
        }

        if (readyScanPages >= readyScanPageLimit && groupRunning < cfg.getMaxConcurrency()) {
            log.warn("dispatch ready scan page limit reached, group={}, readyScanPages={}, pageSize={}, readyScanned={}, "
                            + "dispatched={}, skipped={}, saturatedUsers={}, groupRunning={}, groupMax={}",
                    cfg.getGroupCode(), readyScanPages, pageSize, readyScanned, dispatched, skipped,
                    saturatedUsers.size(), groupRunning, cfg.getMaxConcurrency());
        }

        if (dispatched > 0 || groupRunning > 0) {
            AtomicInteger counter = groupSummaryLogCounter.computeIfAbsent(cfg.getGroupCode(), key -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            if (current % SUMMARY_LOG_EVERY_N != 0) {
                return;
            }
            log.info("dispatch group summary, group={}, promoted={}, readyScanPages={}, readyScanned={}, pageSize={}, "
                            + "dispatched={}, skipped={}, saturatedUsers={}, groupRunning={}, costMs={}",
                    cfg.getGroupCode(), promoted, readyScanPages, readyScanned, pageSize, dispatched, skipped,
                    saturatedUsers.size(), groupRunning, System.currentTimeMillis() - begin);
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
