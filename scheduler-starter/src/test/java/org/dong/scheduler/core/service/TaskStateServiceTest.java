package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.enums.SchedulerErrorCode;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.exception.SchedulerException;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskStateServiceTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private TaskDependencyService taskDependencyService;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TransactionTemplate transactionTemplate;

    private TaskStateService taskStateService;

    @BeforeEach
    void setUp() {
        taskStateService = new TaskStateService(taskRepository, taskDependencyService, concurrencyGuard, queueRedisService, transactionTemplate);
        when(transactionTemplate.execute(any(TransactionCallback.class)))
                .thenAnswer(invocation -> {
                    TransactionCallback<?> callback = invocation.getArgument(0);
                    return callback.doInTransaction(null);
                });
    }

    @Test
    void shouldEnqueueDependentTaskAfterSubmitWhenDependenciesAlreadySatisfied() {
        LocalDateTime now = LocalDateTime.now();
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-1")
                .setPriority(10)
                .setExecuteAt(now.plusMinutes(10))
                .setMaxRetryCount(3)
                .setDependencies(List.of(new TaskDependencyRequest(10L, DependencyTargetState.SUCCESS)));

        SchedulerTask persistedTask = new SchedulerTask();
        persistedTask.setId(100L);
        persistedTask.setStatus(TaskStatus.PENDING);
        persistedTask.setExecuteAt(request.getExecuteAt());

        SchedulerTask queueTask = new SchedulerTask();
        queueTask.setId(100L);
        queueTask.setGroupCode("g1");
        queueTask.setStatus(TaskStatus.PENDING);
        queueTask.setExecuteAt(request.getExecuteAt());

        when(taskRepository.insert(eq("task-1"), eq(request), eq(request.getExtInfo()), eq(TaskStatus.PENDING))).thenReturn(100L);
        when(taskRepository.findById(100L)).thenReturn(Optional.of(persistedTask));
        when(taskDependencyService.refreshTaskAfterSubmit(eq(100L), any(LocalDateTime.class))).thenReturn(queueTask);

        long taskId = taskStateService.submit("task-1", request);

        assertEquals(100L, taskId);
        verify(taskDependencyService).createDependencies(eq(100L), eq(request.getDependencies()), any(LocalDateTime.class));
        verify(queueRedisService).enqueue(queueTask);
    }

    @Test
    void shouldAddReadyQueueDirectlyForDueRunnableTaskAfterSubmit() {
        LocalDateTime now = LocalDateTime.now();
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-ready")
                .setPriority(10)
                .setExecuteAt(now.minusSeconds(1))
                .setMaxRetryCount(3);

        SchedulerTask persistedTask = new SchedulerTask();
        persistedTask.setId(110L);
        persistedTask.setStatus(TaskStatus.RUNNABLE);
        persistedTask.setGroupCode("g1");
        persistedTask.setExecuteAt(request.getExecuteAt());

        when(taskRepository.insert(eq("task-ready"), eq(request), eq(request.getExtInfo()), eq(TaskStatus.RUNNABLE))).thenReturn(110L);
        when(taskRepository.findById(110L)).thenReturn(Optional.of(persistedTask));

        long taskId = taskStateService.submit("task-ready", request);

        assertEquals(110L, taskId);
        verify(queueRedisService).enqueueReady(persistedTask);
        verify(queueRedisService, never()).enqueue(persistedTask);
    }

    @Test
    void shouldThrowDedicatedExceptionWhenSyncSubmitHitsConcurrencyLimit() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-sync")
                .setExecuteAt(LocalDateTime.now().minusSeconds(1));
        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setGroupCode("g1");
        groupConfig.setMaxConcurrency(10);
        groupConfig.setLockExpireSec(30);

        when(taskRepository.insert(eq("task-sync"), eq(request), eq(request.getExtInfo()), eq(TaskStatus.RUNNABLE)))
                .thenReturn(120L);
        when(concurrencyGuard.tryAcquire(eq("g1"), eq("u1"), eq(120L), eq(10), eq(2), anyInt(), eq("exec-1")))
                .thenReturn(false);

        SchedulerException ex = assertThrows(
                SchedulerException.class,
                () -> taskStateService.submitDirect("task-sync", request, groupConfig, 2, "ins-test", "main", "exec-1")
        );

        assertEquals(SchedulerErrorCode.CONCURRENCY_LIMIT.getCode(), ex.getErrorCode());
        assertEquals(SchedulerErrorCode.CONCURRENCY_LIMIT.getMessage(), ex.getMessage());
        verify(concurrencyGuard, never()).release("g1", "u1", 120L, "exec-1");
    }

    @Test
    void shouldEnqueueDependentTasksAfterUpstreamSuccessTransition() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask downstream1 = new SchedulerTask();
        downstream1.setId(201L);
        downstream1.setGroupCode("g1");
        downstream1.setStatus(TaskStatus.RUNNABLE);
        downstream1.setExecuteAt(now.minusSeconds(1));
        SchedulerTask downstream2 = new SchedulerTask();
        downstream2.setId(202L);
        downstream2.setGroupCode("g1");
        downstream2.setStatus(TaskStatus.PENDING);
        downstream2.setExecuteAt(now.plusSeconds(30));

        when(taskRepository.markSuccess(200L, now)).thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(200L, TaskStatus.SUCCESS, now))
                .thenReturn(List.of(downstream1, downstream2));

        boolean changed = taskStateService.markSuccess(200L, now);

        assertTrue(changed);
        verify(queueRedisService).enqueueReady(downstream1);
        verify(queueRedisService).enqueue(downstream2);
    }

    @Test
    void shouldResolveBatchDependencyByClientTaskRefAndSubmitInOrder() {
        LocalDateTime now = LocalDateTime.now();
        TaskSubmitRequest taskA = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setPriority(10)
                .setExecuteAt(now.minusSeconds(1))
                .setMaxRetryCount(3);
        TaskSubmitRequest taskB = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-b")
                .setPriority(10)
                .setExecuteAt(now.plusMinutes(1))
                .setMaxRetryCount(3);
        TaskStateService.BatchSubmitCommand cmdA = new TaskStateService.BatchSubmitCommand(
                "A", "task-a", taskA, List.of());
        TaskStateService.BatchSubmitCommand cmdB = new TaskStateService.BatchSubmitCommand(
                "B", "task-b", taskB,
                List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)));

        SchedulerTask persistedA = new SchedulerTask();
        persistedA.setId(101L);
        persistedA.setStatus(TaskStatus.RUNNABLE);
        persistedA.setGroupCode("g1");
        persistedA.setExecuteAt(taskA.getExecuteAt());

        SchedulerTask persistedB = new SchedulerTask();
        persistedB.setId(102L);
        persistedB.setStatus(TaskStatus.PENDING);
        persistedB.setGroupCode("g1");
        persistedB.setExecuteAt(taskB.getExecuteAt());

        when(taskRepository.insert(eq("task-a"), eq(taskA), eq(taskA.getExtInfo()), eq(TaskStatus.RUNNABLE))).thenReturn(101L);
        when(taskRepository.insert(eq("task-b"), eq(taskB), eq(taskB.getExtInfo()), eq(TaskStatus.PENDING))).thenReturn(102L);
        when(taskRepository.findById(101L)).thenReturn(Optional.of(persistedA));
        when(taskRepository.findById(102L)).thenReturn(Optional.of(persistedB));
        when(taskDependencyService.refreshTaskAfterSubmit(eq(102L), any(LocalDateTime.class))).thenReturn(persistedB);

        List<BatchSubmitResultItem> result = taskStateService.submitBatch(List.of(cmdA, cmdB));

        assertEquals(2, result.size());
        assertEquals("A", result.get(0).getClientTaskRef());
        assertEquals(101L, result.get(0).getTaskId());
        verify(taskDependencyService).createDependencies(eq(102L),
                eq(List.of(new TaskDependencyRequest(101L, DependencyTargetState.SUCCESS))),
                any(LocalDateTime.class));
        verify(queueRedisService).enqueueReady(persistedA);
        verify(queueRedisService).enqueue(persistedB);
    }

    @Test
    void shouldFailBatchWhenDependencyRefMissing() {
        TaskSubmitRequest task = new TaskSubmitRequest()
                .setGroupCode("g1")
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setPriority(10)
                .setExecuteAt(LocalDateTime.now())
                .setMaxRetryCount(3);
        TaskStateService.BatchSubmitCommand cmd = new TaskStateService.BatchSubmitCommand(
                "B", "task-b", task,
                List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)));
        SchedulerTask persisted = new SchedulerTask();
        persisted.setId(201L);
        persisted.setStatus(TaskStatus.PENDING);
        persisted.setExecuteAt(task.getExecuteAt());

        when(taskRepository.insert(eq("task-b"), eq(task), eq(task.getExtInfo()), eq(TaskStatus.PENDING))).thenReturn(201L);
        when(taskRepository.findById(201L)).thenReturn(Optional.of(persisted));

        assertThrows(IllegalArgumentException.class, () -> taskStateService.submitBatch(List.of(cmd)));
        verify(queueRedisService, never()).enqueueReady(any());
        verify(queueRedisService, never()).enqueue(any());
    }

    @Test
    void shouldExpireWaitingTasksAndTriggerDependencyRefresh() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = new SchedulerTask();
        source.setId(201L);
        source.setGroupCode("g1");
        source.setUserId("u1");
        source.setStatus(TaskStatus.RUNNABLE);
        source.setVersion(3);
        source.setWaitDeadlineAt(now.minusSeconds(1));
        SchedulerTask downstream = new SchedulerTask();
        downstream.setId(301L);
        downstream.setGroupCode("g1");
        downstream.setStatus(TaskStatus.RUNNABLE);
        downstream.setExecuteAt(now.minusSeconds(1));

        when(taskRepository.findWaitingTimeoutTaskIds(now, 100)).thenReturn(List.of(201L));
        when(taskRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(source));
        when(taskRepository.markFailedByWaitDeadline(
                source,
                "SCHEDULE_WAIT_TIMEOUT",
                "task exceeded max wait before running",
                now
        )).thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(201L, TaskStatus.FAILED, now)).thenReturn(List.of(downstream));

        int expired = taskStateService.expireWaitingTasks(100, now);

        assertEquals(1, expired);
        verify(queueRedisService).removeQueueReferences(source);
        verify(queueRedisService).enqueueReady(downstream);
    }
}
