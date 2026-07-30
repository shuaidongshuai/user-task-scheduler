package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WorkerServiceReleaseMismatchTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private RecoveryService recoveryService;
    @Mock
    private TaskHandler taskHandler;
    @Mock
    private TaskStateService taskStateService;
    @Mock
    private GroupConfigRepository groupConfigRepository;

    private ThreadPoolTaskExecutor workerExecutor;
    private WorkerService workerService;

    @AfterEach
    void tearDown() {
        if (workerService != null) {
            workerService.shutdown();
        }
        if (workerExecutor != null) {
            workerExecutor.shutdown();
        }
    }

    @Test
    void shouldTriggerImmediateReconcileWhenReleaseMismatched() throws Exception {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setWorkerThreads(2);
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultExecuteTimeoutSec(5);

        workerExecutor = new ThreadPoolTaskExecutor();
        workerExecutor.setCorePoolSize(1);
        workerExecutor.setMaxPoolSize(1);
        workerExecutor.setQueueCapacity(8);
        workerExecutor.initialize();

        when(taskHandler.bizTypes()).thenReturn(List.of("demo.biz"));
        when(taskHandler.execute(any(SchedulerTask.class))).thenReturn(TaskExecuteResult.success());
        when(taskStateService.markSuccess(anyLong(), any(LocalDateTime.class))).thenReturn(true);
        when(concurrencyGuard.release("g1", "u1", 1L, "exec-old")).thenReturn(false);
        when(concurrencyGuard.leaseValue(1L)).thenReturn("exec-new");

        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(taskHandler));
        BusinessTaskStateProviderRegistry stateProviderRegistry = new BusinessTaskStateProviderRegistry(List.of());
        workerService = new WorkerService(
                properties,
                taskRepository,
                registry,
                concurrencyGuard,
                queueRedisService,
                recoveryService,
                workerExecutor,
                stateProviderRegistry,
                taskStateService
        );

        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setExecuteAt(LocalDateTime.now());
        task.setMaxRetryCount(3);

        GroupConfig cfg = new GroupConfig();
        cfg.setLockExpireSec(30);

        workerService.executeDirect(task, cfg, "exec-old");

        verify(concurrencyGuard).release("g1", "u1", 1L, "exec-old");
        verify(recoveryService).reconcileRunningCountersImmediately("g1", "u1", "worker-release-mismatch");
        verify(taskRepository).finishExecution(eq("exec-old"), eq(org.dong.scheduler.core.enums.TaskStatus.SUCCESS), any(), any(), any(LocalDateTime.class));
        verify(taskRepository).insertExecutionStart(eq(task), eq("exec-old"), anyString(), anyString(), any(LocalDateTime.class));
    }

    @Test
    void shouldTreatNullHandlerResultAsFailedExecution() throws Exception {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setWorkerThreads(2);
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultExecuteTimeoutSec(5);

        workerExecutor = new ThreadPoolTaskExecutor();
        workerExecutor.setCorePoolSize(1);
        workerExecutor.setMaxPoolSize(1);
        workerExecutor.setQueueCapacity(8);
        workerExecutor.initialize();

        when(taskHandler.bizTypes()).thenReturn(List.of("demo.biz"));
        when(taskHandler.execute(any(SchedulerTask.class))).thenReturn(null);
        when(taskStateService.markFailed(eq(1L), eq("TASK_HANDLER_NULL_RESULT"),
                eq("task handler returned null result"), any(LocalDateTime.class))).thenReturn(true);
        when(concurrencyGuard.release("g1", "u1", 1L, "exec-null")).thenReturn(true);

        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(taskHandler));
        BusinessTaskStateProviderRegistry stateProviderRegistry = new BusinessTaskStateProviderRegistry(List.of());
        workerService = new WorkerService(
                properties,
                taskRepository,
                registry,
                concurrencyGuard,
                queueRedisService,
                recoveryService,
                workerExecutor,
                stateProviderRegistry,
                taskStateService
        );

        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setExecuteAt(LocalDateTime.now());
        task.setMaxRetryCount(0);

        GroupConfig cfg = new GroupConfig();
        cfg.setLockExpireSec(30);

        workerService.executeDirect(task, cfg, "exec-null");

        verify(taskStateService).markFailed(eq(1L), eq("TASK_HANDLER_NULL_RESULT"),
                eq("task handler returned null result"), any(LocalDateTime.class));
        verify(taskRepository).finishExecution(eq("exec-null"), eq(org.dong.scheduler.core.enums.TaskStatus.FAILED),
                eq("TASK_HANDLER_NULL_RESULT"), eq("task handler returned null result"), any(LocalDateTime.class));
    }

    @Test
    void shouldKeepConcurrencyAndMoveTaskToWaitHoldWhenHandlerRequestsHold() throws Exception {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setWorkerThreads(2);
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultExecuteTimeoutSec(5);

        workerExecutor = new ThreadPoolTaskExecutor();
        workerExecutor.setCorePoolSize(1);
        workerExecutor.setMaxPoolSize(1);
        workerExecutor.setQueueCapacity(8);
        workerExecutor.initialize();

        when(taskHandler.bizTypes()).thenReturn(List.of("demo.biz"));
        when(taskHandler.execute(any(SchedulerTask.class))).thenAnswer(invocation -> {
            SchedulerTask task = invocation.getArgument(0);
            task.setExtInfo("{\"phase\":\"polling\"}");
            return TaskExecuteResult.waitHold();
        });

        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(taskHandler));
        BusinessTaskStateProviderRegistry stateProviderRegistry = new BusinessTaskStateProviderRegistry(List.of());
        workerService = new WorkerService(
                properties,
                taskRepository,
                registry,
                concurrencyGuard,
                queueRedisService,
                recoveryService,
                workerExecutor,
                stateProviderRegistry,
                taskStateService
        );

        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setExecuteAt(LocalDateTime.now());
        task.setMaxRetryCount(3);
        task.setHoldRetryDelaySec(7);
        task.setHoldRoundCount(0);
        task.setHoldMaxRounds(5);

        GroupConfig cfg = new GroupConfig();
        cfg.setLockExpireSec(30);

        workerService.executeDirect(task, cfg, "exec-hold");

        verify(taskRepository).markWaitHold(eq(1L), any(LocalDateTime.class), eq("{\"phase\":\"polling\"}"), any(LocalDateTime.class));
        verify(taskRepository).updateExtInfo(eq(1L), eq("{\"phase\":\"polling\"}"), any(LocalDateTime.class));
        verify(taskRepository, never()).markWaitRetry(anyLong(), any(LocalDateTime.class), anyString(), anyString(), any(LocalDateTime.class));
        verify(concurrencyGuard, never()).release("g1", "u1", 1L, "exec-hold");
        verify(concurrencyGuard).releaseLease(1L, "exec-hold");
        verify(taskRepository).finishExecution(eq("exec-hold"), eq(org.dong.scheduler.core.enums.TaskStatus.WAIT_HOLD),
                any(), any(), any(LocalDateTime.class));
    }

    @Test
    void shouldRethrowHandlerExceptionForDirectExecutionAfterSchedulingRetry() throws Exception {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setWorkerThreads(2);
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultExecuteTimeoutSec(5);
        properties.setDefaultRetryDelaySec(30);

        workerExecutor = new ThreadPoolTaskExecutor();
        workerExecutor.setCorePoolSize(1);
        workerExecutor.setMaxPoolSize(1);
        workerExecutor.setQueueCapacity(8);
        workerExecutor.initialize();

        Exception handlerFailure = new Exception("boom");
        SchedulerTask reloadedTask = new SchedulerTask();
        reloadedTask.setId(1L);
        when(taskHandler.bizTypes()).thenReturn(List.of("demo.biz"));
        when(taskHandler.execute(any(SchedulerTask.class))).thenThrow(handlerFailure);
        when(taskRepository.findById(1L)).thenReturn(java.util.Optional.of(reloadedTask));
        when(concurrencyGuard.release("g1", "u1", 1L, "exec-throw")).thenReturn(true);

        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(taskHandler));
        BusinessTaskStateProviderRegistry stateProviderRegistry = new BusinessTaskStateProviderRegistry(List.of());
        workerService = new WorkerService(
                properties,
                taskRepository,
                registry,
                concurrencyGuard,
                queueRedisService,
                recoveryService,
                workerExecutor,
                stateProviderRegistry,
                taskStateService
        );

        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setExecuteAt(LocalDateTime.now());
        task.setMaxRetryCount(3);
        task.setRetryCount(0);

        GroupConfig cfg = new GroupConfig();
        cfg.setLockExpireSec(30);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> workerService.executeDirect(task, cfg, "exec-throw"));

        assertEquals("sync task execute failed, taskId=1, errorCode=TASK_EXCEPTION, errorMsg=boom", ex.getMessage());
        assertNotNull(ex.getCause());
        assertEquals("boom", ex.getCause().getMessage());
        verify(taskRepository).markWaitRetry(eq(1L), any(LocalDateTime.class),
                eq("TASK_EXCEPTION"), eq("boom"), any(LocalDateTime.class));
        verify(queueRedisService).enqueue(reloadedTask);
        verify(taskRepository).finishExecution(eq("exec-throw"), eq(org.dong.scheduler.core.enums.TaskStatus.WAIT_RETRY),
                eq("TASK_EXCEPTION"), eq("boom"), any(LocalDateTime.class));
    }

    @Test
    void shouldSwitchGroupBeforeSchedulingHandlerRequestedRetry() throws Exception {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setWorkerThreads(2);
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultExecuteTimeoutSec(5);
        properties.setDefaultRetryDelaySec(30);

        workerExecutor = new ThreadPoolTaskExecutor();
        workerExecutor.setCorePoolSize(1);
        workerExecutor.setMaxPoolSize(1);
        workerExecutor.setQueueCapacity(8);
        workerExecutor.initialize();

        SchedulerTask rescheduled = new SchedulerTask();
        rescheduled.setId(1L);
        rescheduled.setGroupCode("g2");
        GroupConfig target = new GroupConfig();
        target.setGroupCode("g2");
        target.setEnabled(true);
        when(taskHandler.bizTypes()).thenReturn(List.of("demo.biz"));
        when(taskHandler.execute(any(SchedulerTask.class)))
                .thenReturn(TaskExecuteResult.retryableOnGroup("TEMPORARY", "try target group", "g2"));
        when(groupConfigRepository.findEnabledByGroupCode("g2")).thenReturn(Optional.of(target));
        when(taskRepository.markWaitRetryOnGroup(eq(1L), any(LocalDateTime.class), eq("TEMPORARY"),
                eq("try target group"), eq("g1"), eq("g2"), any(LocalDateTime.class))).thenReturn(true);
        when(taskRepository.findById(1L)).thenReturn(Optional.of(rescheduled));
        when(concurrencyGuard.release("g1", "u1", 1L, "exec-switch")).thenReturn(true);

        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(taskHandler));
        BusinessTaskStateProviderRegistry stateProviderRegistry = new BusinessTaskStateProviderRegistry(List.of());
        workerService = new WorkerService(properties, taskRepository, registry, concurrencyGuard,
                queueRedisService, recoveryService, workerExecutor, stateProviderRegistry, taskStateService,
                groupConfigRepository);

        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setExecuteAt(LocalDateTime.now());
        task.setMaxRetryCount(1);

        GroupConfig source = new GroupConfig();
        source.setLockExpireSec(30);
        workerService.executeDirect(task, source, "exec-switch");

        verify(taskRepository).markWaitRetryOnGroup(eq(1L), any(LocalDateTime.class), eq("TEMPORARY"),
                eq("try target group"), eq("g1"), eq("g2"), any(LocalDateTime.class));
        verify(taskRepository, never()).markWaitRetry(eq(1L), any(LocalDateTime.class), anyString(),
                anyString(), any(LocalDateTime.class));
        org.mockito.InOrder queueOrder = inOrder(queueRedisService);
        queueOrder.verify(queueRedisService).removeQueueReferences(task);
        queueOrder.verify(queueRedisService).enqueue(rescheduled);
        verify(concurrencyGuard).release("g1", "u1", 1L, "exec-switch");
        verify(taskRepository).finishExecution(eq("exec-switch"), eq(org.dong.scheduler.core.enums.TaskStatus.WAIT_RETRY),
                eq("TEMPORARY"), eq("try target group"), any(LocalDateTime.class));
    }
}
