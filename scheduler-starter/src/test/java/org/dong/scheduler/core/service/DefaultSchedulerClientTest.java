package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.model.batch.BatchSubmitTaskRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultSchedulerClientTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TaskStateService taskStateService;
    @Mock
    private GroupConfigRepository groupConfigRepository;
    @Mock
    private DynamicUserLimitService dynamicUserLimitService;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private WorkerService workerService;

    private DefaultSchedulerClient schedulerClient;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDefaultGroupCode("default-group");
        properties.setDispatchRoute("route-a");
        properties.setInstanceId("ins-test");
        schedulerClient = new DefaultSchedulerClient(taskRepository, queueRedisService, properties, taskStateService,
                groupConfigRepository, dynamicUserLimitService, concurrencyGuard, workerService);
    }

    @Test
    void shouldExecuteSyncInCallerThread() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-sync")
                .setExecuteAt(LocalDateTime.now());
        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setGroupCode("default-group");
        groupConfig.setMaxConcurrency(10);
        groupConfig.setUserBaseConcurrency(2);
        SchedulerTask runningTask = new SchedulerTask();
        runningTask.setId(101L);
        runningTask.setTaskNo("task-101");
        runningTask.setGroupCode("default-group");
        runningTask.setUserId("u1");
        runningTask.setBizType("demo.biz");
        SchedulerTask successTask = new SchedulerTask();
        successTask.setId(101L);
        successTask.setTaskNo("task-101");
        successTask.setStatus(org.dong.scheduler.core.enums.TaskStatus.SUCCESS);
        when(groupConfigRepository.findEnabledByGroupCode("default-group")).thenReturn(Optional.of(groupConfig));
        when(concurrencyGuard.groupRunning("default-group")).thenReturn(0L);
        when(dynamicUserLimitService.calculate(groupConfig, 0L)).thenReturn(2);
        when(taskStateService.submitDirect(anyString(), any(TaskSubmitRequest.class), eq(groupConfig), eq(2),
                eq("ins-test"), anyString(), anyString())).thenReturn(101L);
        when(taskRepository.findById(101L)).thenReturn(Optional.of(runningTask), Optional.of(successTask));

        long taskId = schedulerClient.executeSync(request);

        assertEquals(101L, taskId);
        verify(taskStateService).submitDirect(anyString(), any(TaskSubmitRequest.class), eq(groupConfig), eq(2),
                eq("ins-test"), anyString(), anyString());
        verify(workerService).executeDirect(eq(runningTask), eq(groupConfig), anyString());
    }

    @Test
    void shouldDefaultDispatchRouteFromLocalServiceConfig() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setExecuteAt(LocalDateTime.now());
        when(taskStateService.submit(anyString(), any(TaskSubmitRequest.class))).thenReturn(101L);
        SchedulerTask task = new SchedulerTask();
        task.setId(101L);
        task.setTaskNo("task-101");
        task.setStatus(org.dong.scheduler.core.enums.TaskStatus.RUNNABLE);
        task.setExecuteAt(LocalDateTime.now());
        when(taskRepository.findById(101L)).thenReturn(Optional.of(task));

        schedulerClient.submit(request);

        ArgumentCaptor<TaskSubmitRequest> captor = ArgumentCaptor.forClass(TaskSubmitRequest.class);
        verify(taskStateService).submit(anyString(), captor.capture());
        assertEquals("route-a", captor.getValue().getDispatchRoute());
        assertEquals("default-group", captor.getValue().getGroupCode());
    }

    @Test
    void shouldKeepDispatchRouteNullWhenLocalServiceConfigIsBlank() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDefaultGroupCode("default-group");
        properties.setInstanceId("ins-test");
        schedulerClient = new DefaultSchedulerClient(taskRepository, queueRedisService, properties, taskStateService,
                groupConfigRepository, dynamicUserLimitService, concurrencyGuard, workerService);
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setExecuteAt(LocalDateTime.now());
        when(taskStateService.submit(anyString(), any(TaskSubmitRequest.class))).thenReturn(101L);
        SchedulerTask task = new SchedulerTask();
        task.setId(101L);
        task.setTaskNo("task-101");
        task.setStatus(org.dong.scheduler.core.enums.TaskStatus.RUNNABLE);
        task.setExecuteAt(LocalDateTime.now());
        when(taskRepository.findById(101L)).thenReturn(Optional.of(task));

        schedulerClient.submit(request);

        ArgumentCaptor<TaskSubmitRequest> captor = ArgumentCaptor.forClass(TaskSubmitRequest.class);
        verify(taskStateService).submit(anyString(), captor.capture());
        assertEquals(null, captor.getValue().getDispatchRoute());
    }

    @Test
    void shouldThrowWhenSyncTaskIsThrottled() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-sync")
                .setExecuteAt(LocalDateTime.now());
        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setGroupCode("default-group");
        groupConfig.setMaxConcurrency(10);
        groupConfig.setUserBaseConcurrency(2);
        when(groupConfigRepository.findEnabledByGroupCode("default-group")).thenReturn(Optional.of(groupConfig));
        when(concurrencyGuard.groupRunning("default-group")).thenReturn(0L);
        when(dynamicUserLimitService.calculate(groupConfig, 0L)).thenReturn(2);
        when(taskStateService.submitDirect(anyString(), any(TaskSubmitRequest.class), eq(groupConfig), eq(2),
                eq("ins-test"), anyString(), anyString()))
                .thenThrow(new IllegalStateException("sync task is throttled by concurrency limit"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> schedulerClient.executeSync(request));

        assertEquals("sync task is throttled by concurrency limit", ex.getMessage());
    }

    @Test
    void shouldRejectBatchDependencyCycle() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "B", DependencyTargetState.SUCCESS))),
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("B")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-b")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)))
        ));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("in-batch dependency cycle detected", ex.getMessage());
    }

    @Test
    void shouldRejectDependencyWhenIdAndRefAreBothProvided() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(1L, "B", DependencyTargetState.SUCCESS)))
        ));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("dependency requires exactly one of dependsOnTaskId/dependsOnClientTaskRef", ex.getMessage());
    }

    @Test
    void shouldRejectSingleSubmitWhenDependencyTaskIdDoesNotExist() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setExecuteAt(LocalDateTime.now())
                .setDependencies(List.of(new TaskDependencyRequest(123L, DependencyTargetState.SUCCESS)));
        when(taskRepository.findExistingTaskIds(List.of(123L))).thenReturn(List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submit(request));

        assertEquals("dependency taskId not found: 123", ex.getMessage());
    }

    @Test
    void shouldRejectBatchSubmitWhenDependencyTaskIdDoesNotExist() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(123L, null, DependencyTargetState.SUCCESS)))
        ));
        when(taskRepository.findExistingTaskIds(List.of(123L))).thenReturn(List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("dependency taskId not found: 123", ex.getMessage());
    }

    @Test
    void shouldNormalizeBatchTasksBeforeDelegating() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setPriority(-5)
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of()),
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("B")
                        .setGroupCode("g1")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-b")
                        .setMaxRetryCount(-1)
                        .setRetryDelaySec(-2)
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)))
        ));
        when(taskStateService.submitBatch(anyList())).thenReturn(List.of(
                new BatchSubmitResultItem("A", 101L, "task-a"),
                new BatchSubmitResultItem("B", 102L, "task-b")
        ));

        List<BatchSubmitResultItem> result = schedulerClient.submitBatch(request);

        assertEquals(2, result.size());
        ArgumentCaptor<List<TaskStateService.BatchSubmitCommand>> captor = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(taskStateService).submitBatch(captor.capture());
        List<TaskStateService.BatchSubmitCommand> commands = captor.getValue();
        assertEquals(2, commands.size());
        assertEquals("default-group", commands.getFirst().request().getGroupCode());
        assertEquals("route-a", commands.getFirst().request().getDispatchRoute());
        assertEquals(0, commands.getFirst().request().getPriority());
        assertEquals("g1", commands.get(1).request().getGroupCode());
        assertEquals("route-a", commands.get(1).request().getDispatchRoute());
        assertEquals(0, commands.get(1).request().getMaxRetryCount());
        assertEquals(0, commands.get(1).request().getRetryDelaySec());
    }

    @Test
    void shouldRejectNonPositiveMaxWaitSecOnSingleSubmit() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setExecuteAt(LocalDateTime.now())
                .setMaxWaitSec(0);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submit(request));

        assertEquals("maxWaitSec must be greater than 0", ex.getMessage());
    }

    @Test
    void shouldCalculateWaitDeadlineFromExecuteAtOnSingleSubmit() {
        LocalDateTime executeAt = LocalDateTime.of(2026, 6, 12, 12, 0, 0);
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-wait-single")
                .setExecuteAt(executeAt)
                .setMaxWaitSec(600);
        SchedulerTask task = new SchedulerTask();
        task.setId(101L);
        task.setTaskNo("task-101");
        task.setStatus(org.dong.scheduler.core.enums.TaskStatus.PENDING);
        task.setExecuteAt(executeAt);
        when(taskStateService.submit(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(TaskSubmitRequest.class)))
                .thenReturn(101L);
        when(taskRepository.findById(101L)).thenReturn(Optional.of(task));

        schedulerClient.submit(request);

        ArgumentCaptor<TaskSubmitRequest> captor = ArgumentCaptor.forClass(TaskSubmitRequest.class);
        verify(taskStateService).submit(org.mockito.ArgumentMatchers.anyString(), captor.capture());
        assertEquals(executeAt.plusSeconds(600), captor.getValue().getWaitDeadlineAt());
    }

    @Test
    void shouldCalculateWaitDeadlineFromExecuteAtOnBatchSubmit() {
        LocalDateTime executeAt = LocalDateTime.of(2026, 6, 12, 12, 0, 0);
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(executeAt)
                        .setMaxWaitSec(300)
                        .setDependencies(List.of())
        ));
        when(taskStateService.submitBatch(anyList())).thenReturn(List.of(
                new BatchSubmitResultItem("A", 101L, "task-a")
        ));

        schedulerClient.submitBatch(request);

        ArgumentCaptor<List<TaskStateService.BatchSubmitCommand>> captor = ArgumentCaptor.forClass(List.class);
        verify(taskStateService).submitBatch(captor.capture());
        assertEquals(executeAt.plusSeconds(300), captor.getValue().getFirst().request().getWaitDeadlineAt());
    }
}
