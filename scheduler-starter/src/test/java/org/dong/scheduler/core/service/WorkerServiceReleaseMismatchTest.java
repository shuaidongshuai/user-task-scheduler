package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

        when(taskHandler.bizType()).thenReturn("demo.biz");
        when(taskHandler.execute(any(SchedulerTask.class))).thenReturn(TaskExecuteResult.success());
        when(taskRepository.markSuccess(anyLong(), any(LocalDateTime.class))).thenReturn(true);
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
                stateProviderRegistry
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

        ReflectionTestUtils.invokeMethod(workerService, "run", task, cfg, "exec-old");

        verify(concurrencyGuard).release("g1", "u1", 1L, "exec-old");
        verify(recoveryService).reconcileRunningCountersImmediately("g1", "u1", "worker-release-mismatch");
        verify(taskRepository).finishExecution(eq("exec-old"), eq(org.dong.scheduler.core.enums.TaskStatus.SUCCESS), any(), any(), any(LocalDateTime.class));
        verify(taskRepository).insertExecutionStart(eq(task), eq("exec-old"), anyString(), anyString(), any(LocalDateTime.class));
    }
}
