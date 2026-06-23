package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.TaskHandler;
import org.dong.scheduler.core.util.ThreadContextUtil;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import jakarta.annotation.PreDestroy;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class WorkerService {
    private final SchedulerProperties properties;
    private final TaskRepository taskRepository;
    private final TaskHandlerRegistry handlerRegistry;
    private final ConcurrencyGuard concurrencyGuard;
    private final QueueRedisService queueRedisService;
    private final RecoveryService recoveryService;
    private final ThreadPoolTaskExecutor workerExecutor;
    private final ScheduledExecutorService heartbeatExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry;
    private final TaskStateService taskStateService;

    public WorkerService(SchedulerProperties properties,
                         TaskRepository taskRepository,
                         TaskHandlerRegistry handlerRegistry,
                         ConcurrencyGuard concurrencyGuard,
                         QueueRedisService queueRedisService,
                         RecoveryService recoveryService,
                         ThreadPoolTaskExecutor workerExecutor,
                         BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry,
                         TaskStateService taskStateService) {
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.handlerRegistry = handlerRegistry;
        this.concurrencyGuard = concurrencyGuard;
        this.queueRedisService = queueRedisService;
        this.recoveryService = recoveryService;
        this.workerExecutor = workerExecutor;
        this.businessTaskStateProviderRegistry = businessTaskStateProviderRegistry;
        this.taskStateService = taskStateService;
        int heartbeatThreads = properties.getHeartbeatThreads() > 0
                ? properties.getHeartbeatThreads()
                : Math.max(2, properties.getWorkerThreads() / 4);
        int timeoutMonitorThreads = Math.max(1, properties.getTimeoutMonitorThreads());
        this.heartbeatExecutor = new ScheduledThreadPoolExecutor(heartbeatThreads);
        this.timeoutExecutor = new ScheduledThreadPoolExecutor(timeoutMonitorThreads);
    }

    public void submit(SchedulerTask task, GroupConfig groupConfig, String executeNo) {
        log.info("task submitted to worker pool, taskId={}, taskNo={}, executeNo={}, group={}, user={}",
                task.getId(), task.getTaskNo(), executeNo, task.getGroupCode(), task.getUserId());
        workerExecutor.execute(ThreadContextUtil.addNewContext(() -> executeInternal(task, groupConfig, executeNo)));
    }

    public void executeDirect(SchedulerTask task, GroupConfig groupConfig, String executeNo) {
        executeInternal(task, groupConfig, executeNo);
    }

    private void executeInternal(SchedulerTask task, GroupConfig groupConfig, String executeNo) {
        long begin = System.currentTimeMillis();
        String instanceId = properties.getInstanceId();
        LocalDateTime now = LocalDateTime.now();
        log.info("worker run start, taskId={}, taskNo={}, executeNo={}, bizType={}, retryCount={}/{}",
                task.getId(), task.getTaskNo(), executeNo, task.getBizType(), task.getRetryCount(), task.getMaxRetryCount());
        taskRepository.insertExecutionStart(task, executeNo, instanceId, instanceId, now);

        Future<?> heartbeat = heartbeatExecutor.scheduleAtFixedRate(
                ThreadContextUtil.addContext(() -> {
                    try {
                        taskRepository.heartbeat(task.getId(), LocalDateTime.now());
                        boolean renewed = concurrencyGuard.renewLease(task.getId(), executeNo, groupConfig.getLockExpireSec());
                        if (!renewed) {
                            log.warn("lease renew failed, taskId={}, taskNo={}, executeNo={}", task.getId(), task.getTaskNo(), executeNo);
                        } else {
                            log.debug("heartbeat+lease renewed, taskId={}, executeNo={}", task.getId(), executeNo);
                        }
                    } catch (Exception e) {
                        log.warn("heartbeat task failed, taskId={}, taskNo={}, executeNo={}", task.getId(), task.getTaskNo(), executeNo, e);
                    }
                }),
                properties.getHeartbeatIntervalSec(),
                properties.getHeartbeatIntervalSec(),
                TimeUnit.SECONDS
        );

        TaskStatus finalStatus = TaskStatus.FAILED;
        String errorCode = null;
        String errorMsg = null;

        try {
            BusinessTaskStateProvider stateProvider = businessTaskStateProviderRegistry.find(task.getBizType());
            if (stateProvider != null) {
                BusinessTaskState state = stateProvider.query(task);
                if (state == BusinessTaskState.SUCCESS) {
                    taskStateService.markSuccess(task.getId(), LocalDateTime.now());
                    finalStatus = TaskStatus.SUCCESS;
                    log.info("worker short-circuit success by biz state, taskId={}, taskNo={}, executeNo={}",
                            task.getId(), task.getTaskNo(), executeNo);
                    return;
                }
                if (state == BusinessTaskState.FAILED) {
                    taskStateService.markFailed(task.getId(), "BIZ_FAILED", "business state already failed", LocalDateTime.now());
                    finalStatus = TaskStatus.FAILED;
                    log.info("worker short-circuit failed by biz state, taskId={}, taskNo={}, executeNo={}",
                            task.getId(), task.getTaskNo(), executeNo);
                    return;
                }
                if (state != BusinessTaskState.NEED_RUNNING && state != BusinessTaskState.RUNNING) {
                    LocalDateTime nextCheckAt = nextRetryTime(task);
                    boolean deferred = taskRepository.rescheduleToRunnable(
                            task.getId(),
                            nextCheckAt,
                            "BIZ_STATE_NOT_READY",
                            "business state is " + state,
                            LocalDateTime.now()
                    );
                    if (deferred) {
                        task.setExecuteAt(nextCheckAt);
                        queueRedisService.enqueue(task);
                    }
                    finalStatus = TaskStatus.WAIT_RETRY;
                    log.info("worker deferred by biz state, taskId={}, taskNo={}, executeNo={}, state={}, nextCheckAt={}",
                            task.getId(), task.getTaskNo(), executeNo, state, nextCheckAt);
                    return;
                }
            }

            TaskHandler handler = handlerRegistry.find(task.getBizType());
            if (handler == null) {
                throw new IllegalStateException("no TaskHandler found for bizType=" + task.getBizType());
            }

            TaskExecuteResult result;
            try {
                result = executeWithTimeout(task, handler);
            } finally {
                persistTaskExtInfo(task);
            }
            if (result.isSuccess()) {
                taskStateService.markSuccess(task.getId(), LocalDateTime.now());
                finalStatus = TaskStatus.SUCCESS;
                log.info("task execute success, taskId={}, taskNo={}, executeNo={}",
                        task.getId(), task.getTaskNo(), executeNo);
            } else if (result.isWaitHold()) {
                if (task.getHoldRoundCount() + 1 > task.getHoldMaxRounds()) {
                    errorCode = "WAIT_HOLD_ROUNDS_EXHAUSTED";
                    errorMsg = "wait hold rounds exhausted";
                    taskStateService.markFailed(task.getId(), errorCode, errorMsg, LocalDateTime.now());
                    finalStatus = TaskStatus.FAILED;
                    log.error("task wait hold exhausted, taskId={}, taskNo={}, executeNo={}, holdRoundCount={}, holdMaxRounds={}",
                            task.getId(), task.getTaskNo(), executeNo, task.getHoldRoundCount(), task.getHoldMaxRounds());
                } else {
                    LocalDateTime nextHoldTime = nextHoldTime(task);
                    boolean deferred = taskRepository.markWaitHold(task.getId(), nextHoldTime, task.getExtInfo(), LocalDateTime.now());
                    if (deferred) {
                        task.setStatus(TaskStatus.WAIT_HOLD);
                        task.setExecuteAt(nextHoldTime);
                        task.setHoldRoundCount(task.getHoldRoundCount() + 1);
                        queueRedisService.enqueue(task);
                    }
                    finalStatus = TaskStatus.WAIT_HOLD;
                    log.info("task enter wait hold, taskId={}, taskNo={}, executeNo={}, nextExecuteAt={}, holdRoundCount={}/{}",
                            task.getId(), task.getTaskNo(), executeNo, nextHoldTime, task.getHoldRoundCount(), task.getHoldMaxRounds());
                }
            } else if ("TASK_TIMEOUT_UNINTERRUPTIBLE".equals(result.getErrorCode())) {
                errorCode = result.getErrorCode();
                errorMsg = result.getErrorMsg();
                if (task.canRetry()) {
                    LocalDateTime nextRetry = nextRetryTime(task);
                    taskRepository.markWaitRetry(task.getId(), nextRetry, errorCode, errorMsg, LocalDateTime.now());
                    taskRepository.findById(task.getId()).ifPresent(queueRedisService::enqueue);
                    finalStatus = TaskStatus.WAIT_RETRY;
                    log.error("task timeout uninterruptible, retry scheduled (business idempotency required), "
                                    + "taskId={}, taskNo={}, executeNo={}, nextRetryAt={}, retryCount={}/{}",
                            task.getId(), task.getTaskNo(), executeNo, nextRetry, task.getRetryCount() + 1, task.getMaxRetryCount());
                } else {
                    taskStateService.markFailed(task.getId(), errorCode, errorMsg, LocalDateTime.now());
                    finalStatus = TaskStatus.FAILED;
                    log.error("task timeout uninterruptible, retry exhausted -> FAILED, taskId={}, taskNo={}, executeNo={}, retryCount={}/{}",
                            task.getId(), task.getTaskNo(), executeNo, task.getRetryCount(), task.getMaxRetryCount());
                }
            } else if (result.isRetryable() && task.canRetry()) {
                LocalDateTime nextRetry = nextRetryTime(task);
                taskRepository.markWaitRetry(task.getId(), nextRetry, result.getErrorCode(), result.getErrorMsg(), LocalDateTime.now());
                taskRepository.findById(task.getId()).ifPresent(queueRedisService::enqueue);
                finalStatus = TaskStatus.WAIT_RETRY;
                errorCode = result.getErrorCode();
                errorMsg = result.getErrorMsg();
                log.info("task execute retry scheduled, taskId={}, taskNo={}, executeNo={}, errorCode={}, "
                                + "nextRetryAt={}, retryCount={}/{}",
                        task.getId(), task.getTaskNo(), executeNo, errorCode, nextRetry, task.getRetryCount() + 1, task.getMaxRetryCount());
            } else {
                taskStateService.markFailed(task.getId(), result.getErrorCode(), result.getErrorMsg(), LocalDateTime.now());
                finalStatus = TaskStatus.FAILED;
                errorCode = result.getErrorCode();
                errorMsg = result.getErrorMsg();
                log.error("task execute failed, taskId={}, taskNo={}, executeNo={}, errorCode={}, errorMsg={}",
                        task.getId(), task.getTaskNo(), executeNo, errorCode, errorMsg);
            }
        } catch (Exception ex) {
            log.error("task execute exception, taskId={}", task.getId(), ex);
            errorCode = "TASK_EXCEPTION";
            errorMsg = ex.getMessage();
            if (task.canRetry()) {
                LocalDateTime nextRetry = nextRetryTime(task);
                taskRepository.markWaitRetry(task.getId(), nextRetry, errorCode, errorMsg, LocalDateTime.now());
                taskRepository.findById(task.getId()).ifPresent(queueRedisService::enqueue);
                finalStatus = TaskStatus.WAIT_RETRY;
            } else {
                taskStateService.markFailed(task.getId(), errorCode, errorMsg, LocalDateTime.now());
                finalStatus = TaskStatus.FAILED;
            }
        } finally {
            heartbeat.cancel(true);
            if (finalStatus == TaskStatus.WAIT_HOLD) {
                concurrencyGuard.releaseLease(task.getId(), executeNo);
            } else {
                boolean released = concurrencyGuard.release(task.getGroupCode(), task.getUserId(), task.getId(), executeNo);
                if (!released) {
                    String currentLease = concurrencyGuard.leaseValue(task.getId());
                    log.warn("worker release mismatch, skip blind repair to avoid decrementing another execution "
                                    + "counters, taskId={}, taskNo={}, executeNo={}, currentLease={}, group={}, user={}",
                            task.getId(), task.getTaskNo(), executeNo, currentLease, task.getGroupCode(), task.getUserId());
                    recoveryService.reconcileRunningCountersImmediately(task.getGroupCode(), task.getUserId(), "worker-release-mismatch");
                }
            }
            taskRepository.finishExecution(executeNo, finalStatus, errorCode, errorMsg, LocalDateTime.now());
            long cost = System.currentTimeMillis() - begin;
            log.info("worker run end, taskId={}, taskNo={}, executeNo={}, finalStatus={}, errorCode={}, costMs={}",
                    task.getId(), task.getTaskNo(), executeNo, finalStatus, errorCode, cost);
        }
    }

    public String newExecuteNo() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private TaskExecuteResult executeWithTimeout(SchedulerTask task, TaskHandler handler) throws Exception {
        Integer timeout = task.getExecuteTimeoutSec();
        int timeoutSec = timeout == null ? properties.getDefaultExecuteTimeoutSec() : timeout;
        if (timeoutSec <= 0) {
            TaskExecuteResult result = handler.execute(task);
            if (result == null) {
                log.warn("task handler returned null result, taskId={}, taskNo={}", task.getId(), task.getTaskNo());
                return TaskExecuteResult.failed("TASK_HANDLER_NULL_RESULT", "task handler returned null result", false);
            }
            return result;
        }

        Thread workerThread = Thread.currentThread();
        AtomicBoolean timedOut = new AtomicBoolean(false);
        Future<?> timeoutFuture = timeoutExecutor.schedule(
                ThreadContextUtil.addContext(() -> {
                    timedOut.set(true);
                    workerThread.interrupt();
                    log.warn("task execute timeout, interrupt worker thread, taskId={}, taskNo={}, timeoutSec={}",
                            task.getId(), task.getTaskNo(), timeoutSec);
                }),
                timeoutSec,
                TimeUnit.SECONDS
        );
        try {
            TaskExecuteResult result = handler.execute(task);
            if (timedOut.get()) {
                return TaskExecuteResult.failed("TASK_TIMEOUT", "task execution timeout", true);
            }
            if (result == null) {
                log.warn("task handler returned null result, taskId={}, taskNo={}", task.getId(), task.getTaskNo());
                return TaskExecuteResult.failed("TASK_HANDLER_NULL_RESULT", "task handler returned null result", false);
            }
            return result;
        } catch (InterruptedException e) {
            if (timedOut.get()) {
                return TaskExecuteResult.failed("TASK_TIMEOUT", "task execution timeout", true);
            }
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            timeoutFuture.cancel(false);
            if (timedOut.get()) {
                Thread.interrupted();
            }
        }
    }

    private LocalDateTime nextRetryTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.retryDelaySec(properties.getDefaultRetryDelaySec()));
    }

    private LocalDateTime nextHoldTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.getHoldRetryDelaySec());
    }

    private void persistTaskExtInfo(SchedulerTask task) {
        taskRepository.updateExtInfo(task.getId(), task.getExtInfo(), LocalDateTime.now());
    }

    @PreDestroy
    public void shutdown() {
        try {
            heartbeatExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("shutdown heartbeatExecutor failed", e);
        }
        try {
            timeoutExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("shutdown timeoutExecutor failed", e);
        }
    }
}
