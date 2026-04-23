package org.dong.scheduler.core.service;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import jakarta.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerService {
    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final SchedulerProperties properties;
    private final TaskRepository taskRepository;
    private final TaskHandlerRegistry handlerRegistry;
    private final ConcurrencyGuard concurrencyGuard;
    private final QueueRedisService queueRedisService;
    private final ThreadPoolTaskExecutor workerExecutor;
    private final ScheduledExecutorService heartbeatExecutor;
    private final ExecutorService invokeExecutor;
    private final Optional<BusinessTaskStateProvider> businessTaskStateProvider;

    public WorkerService(SchedulerProperties properties,
                         TaskRepository taskRepository,
                         TaskHandlerRegistry handlerRegistry,
                         ConcurrencyGuard concurrencyGuard,
                         QueueRedisService queueRedisService,
                         ThreadPoolTaskExecutor workerExecutor,
                         Optional<BusinessTaskStateProvider> businessTaskStateProvider) {
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.handlerRegistry = handlerRegistry;
        this.concurrencyGuard = concurrencyGuard;
        this.queueRedisService = queueRedisService;
        this.workerExecutor = workerExecutor;
        this.businessTaskStateProvider = businessTaskStateProvider;
        this.heartbeatExecutor = new ScheduledThreadPoolExecutor(Math.max(2, properties.getWorkerThreads() / 4));
        int invokeThreads = Math.max(2, properties.getWorkerThreads());
        BlockingQueue<Runnable> invokeQueue = new LinkedBlockingQueue<>(invokeThreads * 4);
        this.invokeExecutor = new ThreadPoolExecutor(
                invokeThreads,
                invokeThreads,
                60L,
                TimeUnit.SECONDS,
                invokeQueue,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public void submit(SchedulerTask task, GroupConfig groupConfig, String executeNo) {
        log.info("task submitted to worker pool, taskId={}, taskNo={}, executeNo={}, group={}, user={}",
                task.getId(), task.getTaskNo(), executeNo, task.getGroupCode(), task.getUserId());
        workerExecutor.execute(() -> run(task, groupConfig, executeNo));
    }

    private void run(SchedulerTask task, GroupConfig groupConfig, String executeNo) {
        long begin = System.currentTimeMillis();
        String instanceId = properties.getInstanceId();
        LocalDateTime now = LocalDateTime.now();
        log.info("worker run start, taskId={}, taskNo={}, executeNo={}, bizType={}, retryCount={}/{}",
                task.getId(), task.getTaskNo(), executeNo, task.getBizType(), task.getRetryCount(), task.getMaxRetryCount());
        taskRepository.insertExecutionStart(task, executeNo, instanceId, instanceId, now);

        Future<?> heartbeat = heartbeatExecutor.scheduleAtFixedRate(
                () -> {
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
                },
                properties.getHeartbeatIntervalSec(),
                properties.getHeartbeatIntervalSec(),
                TimeUnit.SECONDS
        );

        TaskStatus finalStatus = TaskStatus.FAILED;
        String errorCode = null;
        String errorMsg = null;

        try {
            if (businessTaskStateProvider.isPresent()) {
                BusinessTaskState state = businessTaskStateProvider.get().query(task);
                if (state == BusinessTaskState.SUCCESS) {
                    taskRepository.markSuccess(task.getId(), LocalDateTime.now());
                    finalStatus = TaskStatus.SUCCESS;
                    log.info("worker short-circuit success by biz state, taskId={}, taskNo={}, executeNo={}",
                            task.getId(), task.getTaskNo(), executeNo);
                    return;
                }
                if (state == BusinessTaskState.FAILED) {
                    taskRepository.markFailed(task.getId(), "BIZ_FAILED", "business state already failed", LocalDateTime.now());
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

            TaskExecuteResult result = executeWithTimeout(task, handler);
            persistExtInfoIfPresent(task, result.getExtInfo());
            if (result.isSuccess()) {
                taskRepository.markSuccess(task.getId(), LocalDateTime.now());
                finalStatus = TaskStatus.SUCCESS;
                log.info("task execute success, taskId={}, taskNo={}, executeNo={}",
                        task.getId(), task.getTaskNo(), executeNo);
            } else if ("TASK_TIMEOUT_UNINTERRUPTIBLE".equals(result.getErrorCode())) {
                errorCode = result.getErrorCode();
                errorMsg = result.getErrorMsg();
                if (task.canRetry()) {
                    LocalDateTime nextRetry = nextRetryTime(task);
                    taskRepository.markWaitRetry(task.getId(), nextRetry, errorCode, errorMsg, LocalDateTime.now());
                    taskRepository.findById(task.getId()).ifPresent(queueRedisService::enqueue);
                    finalStatus = TaskStatus.WAIT_RETRY;
                    log.error("task timeout uninterruptible, retry scheduled (business idempotency required), taskId={}, taskNo={}, executeNo={}, nextRetryAt={}, retryCount={}/{}",
                            task.getId(), task.getTaskNo(), executeNo, nextRetry, task.getRetryCount() + 1, task.getMaxRetryCount());
                } else {
                    taskRepository.markFailed(task.getId(), errorCode, errorMsg, LocalDateTime.now());
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
                log.warn("task execute retry scheduled, taskId={}, taskNo={}, executeNo={}, errorCode={}, nextRetryAt={}, retryCount={}/{}",
                        task.getId(), task.getTaskNo(), executeNo, errorCode, nextRetry, task.getRetryCount() + 1, task.getMaxRetryCount());
            } else {
                taskRepository.markFailed(task.getId(), result.getErrorCode(), result.getErrorMsg(), LocalDateTime.now());
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
                taskRepository.markFailed(task.getId(), errorCode, errorMsg, LocalDateTime.now());
                finalStatus = TaskStatus.FAILED;
            }
        } finally {
            heartbeat.cancel(true);
            boolean released = concurrencyGuard.release(task.getGroupCode(), task.getUserId(), task.getId(), executeNo);
            if (!released) {
                concurrencyGuard.repairRelease(task.getGroupCode(), task.getUserId());
                log.warn("worker release mismatch, repaired running counters, taskId={}, taskNo={}, executeNo={}, group={}, user={}",
                        task.getId(), task.getTaskNo(), executeNo, task.getGroupCode(), task.getUserId());
            }
            taskRepository.finishExecution(executeNo, finalStatus, errorCode, errorMsg, LocalDateTime.now());
            long cost = System.currentTimeMillis() - begin;
            log.info("worker run end, taskId={}, taskNo={}, executeNo={}, finalStatus={}, errorCode={}, costMs={}",
                    task.getId(), task.getTaskNo(), executeNo, finalStatus, errorCode, cost);
        }
    }

    private TaskExecuteResult executeWithTimeout(SchedulerTask task, TaskHandler handler) throws Exception {
        Integer timeout = task.getExecuteTimeoutSec();
        int timeoutSec = timeout == null ? properties.getDefaultExecuteTimeoutSec() : timeout;
        int graceSec = Math.max(1, properties.getTimeoutInterruptGraceSec());

        AtomicReference<Thread> runningThread = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Future<TaskExecuteResult> future = invokeExecutor.submit(() -> {
            runningThread.set(Thread.currentThread());
            try {
                return handler.execute(task);
            } finally {
                done.countDown();
            }
        });
        try {
            return future.get(timeoutSec, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            Thread t = runningThread.get();
            if (t != null) {
                t.interrupt();
            }
            boolean stopped = done.await(graceSec, TimeUnit.SECONDS);
            log.warn("task execute timeout, taskId={}, taskNo={}, timeoutSec={}", task.getId(), task.getTaskNo(), timeoutSec);
            if (!stopped) {
                log.error("task execute timeout and thread still alive after grace, taskId={}, taskNo={}, timeoutSec={}, graceSec={}",
                        task.getId(), task.getTaskNo(), timeoutSec, graceSec);
                return TaskExecuteResult.failed("TASK_TIMEOUT_UNINTERRUPTIBLE",
                        "task execution timeout and thread not stopped after interrupt", false);
            }
            return TaskExecuteResult.failed("TASK_TIMEOUT", "task execution timeout", true);
        } catch (RejectedExecutionException e) {
            log.warn("task execute rejected by bounded invoke pool, taskId={}, taskNo={}", task.getId(), task.getTaskNo());
            return TaskExecuteResult.failed("TASK_EXECUTOR_REJECTED", "invoke executor queue is full", true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskExecuteResult.failed("TASK_EXECUTOR_INTERRUPTED", "worker interrupted while waiting task result", true);
        }
    }

    public String newExecuteNo() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private LocalDateTime nextRetryTime(SchedulerTask task) {
        return LocalDateTime.now().plusSeconds(task.retryDelaySec(properties.getDefaultRetryDelaySec()));
    }

    private void persistExtInfoIfPresent(SchedulerTask task, String extInfo) {
        if (extInfo == null) {
            return;
        }
        taskRepository.updateExtInfo(task.getId(), extInfo, LocalDateTime.now());
        task.setExtInfo(extInfo);
    }

    @PreDestroy
    public void shutdown() {
        try {
            heartbeatExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("shutdown heartbeatExecutor failed", e);
        }
        try {
            invokeExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("shutdown invokeExecutor failed", e);
        }
    }
}
