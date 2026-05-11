package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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
    private QueueRedisService queueRedisService;
    @Mock
    private TransactionTemplate transactionTemplate;

    private TaskStateService taskStateService;

    @BeforeEach
    void setUp() {
        taskStateService = new TaskStateService(taskRepository, taskDependencyService, queueRedisService, transactionTemplate);
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
        verify(queueRedisService).addToReady(persistedTask);
        verify(queueRedisService, never()).enqueue(persistedTask);
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
        verify(queueRedisService).addToReady(downstream1);
        verify(queueRedisService).enqueue(downstream2);
    }
}
