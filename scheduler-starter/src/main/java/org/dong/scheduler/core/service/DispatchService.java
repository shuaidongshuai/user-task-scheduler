package org.dong.scheduler.core.service;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

public class DispatchService {
    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);

    private final SchedulerProperties properties;
    private final GroupConfigRepository groupConfigRepository;
    private final TaskRepository taskRepository;
    private final QueueRedisService queueRedisService;
    private final ConcurrencyGuard concurrencyGuard;
    private final DynamicUserLimitService dynamicUserLimitService;
    private final WorkerService workerService;
    private final Optional<BusinessTaskStateProvider> businessTaskStateProvider;

    public DispatchService(SchedulerProperties properties,
                           GroupConfigRepository groupConfigRepository,
                           TaskRepository taskRepository,
                           QueueRedisService queueRedisService,
                           ConcurrencyGuard concurrencyGuard,
                           DynamicUserLimitService dynamicUserLimitService,
                           WorkerService workerService,
                           Optional<BusinessTaskStateProvider> businessTaskStateProvider) {
        this.properties = properties;
        this.groupConfigRepository = groupConfigRepository;
        this.taskRepository = taskRepository;
        this.queueRedisService = queueRedisService;
        this.concurrencyGuard = concurrencyGuard;
        this.dynamicUserLimitService = dynamicUserLimitService;
        this.workerService = workerService;
        this.businessTaskStateProvider = businessTaskStateProvider;
    }

    public void dispatchOnce() {
        List<GroupConfig> groups = groupConfigRepository.listEnabled();
        long nowMillis = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
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
        List<Long> dueTaskIds = queueRedisService.promoteDueTasks(cfg.getGroupCode(), nowMillis, cfg.getDispatchBatchSize());
        int promoted = 0;
        for (Long taskId : dueTaskIds) {
            taskRepository.findById(taskId).ifPresent(task -> {
                if (task.getStatus() == TaskStatus.PENDING && task.due(now)) {
                    boolean promotedNow = taskRepository.markRunnableIfPending(task.getId(), now);
                    if (!promotedNow) {
                        return;
                    }
                    task.setStatus(TaskStatus.RUNNABLE);
                }
                if (task.runnableStatus() && task.due(now)) {
                    queueRedisService.addToReady(task);
                }
            });
            promoted++;
        }

        long groupRunning = concurrencyGuard.groupRunning(cfg.getGroupCode());
        if (groupRunning >= cfg.getMaxConcurrency()) {
            log.debug("dispatch group skipped by full concurrency, group={}, groupRunning={}, groupMax={}",
                    cfg.getGroupCode(), groupRunning, cfg.getMaxConcurrency());
            return;
        }

        List<Long> ready = queueRedisService.peekReady(cfg.getGroupCode(), cfg.getDispatchBatchSize());
        int dispatched = 0;
        int skipped = 0;
        for (Long taskId : ready) {
            if (groupRunning >= cfg.getMaxConcurrency()) {
                break;
            }

            Optional<SchedulerTask> taskOpt = taskRepository.findById(taskId);
            if (taskOpt.isEmpty()) {
                queueRedisService.removeFromReady(cfg.getGroupCode(), taskId);
                skipped++;
                log.debug("dispatch skip missing task, group={}, taskId={}", cfg.getGroupCode(), taskId);
                continue;
            }
            SchedulerTask task = taskOpt.get();
            if (!task.runnableStatus() || !task.due(now)) {
                queueRedisService.removeFromReady(cfg.getGroupCode(), taskId);
                skipped++;
                log.debug("dispatch skip non-runnable/non-due, group={}, taskId={}, status={}, executeAt={}",
                        cfg.getGroupCode(), taskId, task.getStatus(), task.getExecuteAt());
                continue;
            }

            if (businessTaskStateProvider.isPresent()) {
                BusinessTaskState state = businessTaskStateProvider.get().query(task);
                if (state == BusinessTaskState.SUCCESS) {
                    taskRepository.markTerminalByBusinessState(task.getId(), TaskStatus.SUCCESS, now);
                    queueRedisService.removeFromReady(cfg.getGroupCode(), taskId);
                    skipped++;
                    log.info("dispatch short-circuit success by biz state, taskId={}, taskNo={}, group={}",
                            task.getId(), task.getTaskNo(), task.getGroupCode());
                    continue;
                }
                if (state == BusinessTaskState.FAILED) {
                    taskRepository.markTerminalByBusinessState(task.getId(), TaskStatus.FAILED, now);
                    queueRedisService.removeFromReady(cfg.getGroupCode(), taskId);
                    skipped++;
                    log.info("dispatch short-circuit failed by biz state, taskId={}, taskNo={}, group={}",
                            task.getId(), task.getTaskNo(), task.getGroupCode());
                    continue;
                }
                if (state != BusinessTaskState.NEED_RUNNING) {
                    LocalDateTime nextCheckAt = nextRetryTime(task);
                    boolean deferred = taskRepository.rescheduleToRunnable(
                            task.getId(),
                            nextCheckAt,
                            "BIZ_STATE_NOT_READY",
                            "business state is " + state,
                            now
                    );
                    queueRedisService.removeFromReady(cfg.getGroupCode(), taskId);
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

            int userLimit = dynamicUserLimitService.calculate(cfg, groupRunning);
            String executeNo = workerService.newExecuteNo();
            boolean acquired = concurrencyGuard.tryAcquire(
                    cfg.getGroupCode(), task.getUserId(), task.getId(),
                    cfg.getMaxConcurrency(), userLimit, cfg.getLockExpireSec(), executeNo
            );
            if (!acquired) {
                skipped++;
                log.debug("dispatch acquire failed, taskId={}, taskNo={}, group={}, user={}, groupRunning={}, groupMax={}, userLimit={}",
                        task.getId(), task.getTaskNo(), cfg.getGroupCode(), task.getUserId(), groupRunning, cfg.getMaxConcurrency(), userLimit);
                continue;
            }

            boolean cas = taskRepository.casToRunning(task.getId(), properties.getInstanceId(), Thread.currentThread().getName(), now);
            if (!cas) {
                concurrencyGuard.release(cfg.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                skipped++;
                log.debug("dispatch CAS to RUNNING failed, taskId={}, taskNo={}, group={}",
                        task.getId(), task.getTaskNo(), cfg.getGroupCode());
                continue;
            }

            try {
                workerService.submit(task, cfg, executeNo);
                queueRedisService.removeFromReady(cfg.getGroupCode(), task.getId());
                groupRunning++;
                dispatched++;
                log.info("task dispatched, taskId={}, taskNo={}, executeNo={}, group={}, user={}, priority={}, groupRunningAfter={}",
                        task.getId(), task.getTaskNo(), executeNo, task.getGroupCode(), task.getUserId(), task.getPriority(), groupRunning);
            } catch (RuntimeException ex) {
                concurrencyGuard.release(cfg.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                LocalDateTime nextCheckAt = nextRetryTime(task);
                boolean rollback = taskRepository.rescheduleToRunnable(
                        task.getId(),
                        nextCheckAt,
                        "DISPATCH_SUBMIT_REJECTED",
                        ex.getClass().getSimpleName() + ":" + ex.getMessage(),
                        now
                );
                queueRedisService.removeFromReady(cfg.getGroupCode(), task.getId());
                if (rollback) {
                    task.setExecuteAt(nextCheckAt);
                    queueRedisService.enqueue(task);
                }
                skipped++;
                log.error("dispatch submit failed and rolled back, taskId={}, taskNo={}, executeNo={}, rollback={}, nextCheckAt={}",
                        task.getId(), task.getTaskNo(), executeNo, rollback, nextCheckAt, ex);
            }
        }

        if (dispatched > 0 || groupRunning > 0) {
            log.info("dispatch group summary, group={}, promoted={}, readyScanned={}, dispatched={}, skipped={}, groupRunning={}, costMs={}",
                    cfg.getGroupCode(), promoted, ready.size(), dispatched, skipped, groupRunning, System.currentTimeMillis() - begin);
        }
    }

    private LocalDateTime nextRetryTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.retryDelaySec(properties.getDefaultRetryDelaySec()));
    }
}
