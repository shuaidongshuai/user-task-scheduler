package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryServiceQueueRefillTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TaskStateService taskStateService;

    private RecoveryService recoveryService;
    private SchedulerProperties properties;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setQueueRefillLimit(500);
        recoveryService = new RecoveryService(properties, taskRepository, concurrencyGuard, queueRedisService, taskStateService);
    }

    @Test
    void shouldRefillReadyQueueForDueRunnableTaskMissingInRedis() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask task = new SchedulerTask();
        task.setId(301L);
        task.setTaskNo("task-301");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setBizKey("biz-301");
        task.setPriority(5);
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(now.minusSeconds(5));

        when(taskRepository.findRunnableForQueueRefill(any(LocalDateTime.class), eq(500))).thenReturn(List.of(task));
        when(queueRedisService.existsInReady("g1", 301L)).thenReturn(false);

        int refilled = recoveryService.refillQueue();

        assertEquals(1, refilled);
        verify(taskRepository).promotePendingToRunnable(any(LocalDateTime.class), eq(500));
        verify(queueRedisService).addToReady(task);
        verify(queueRedisService).removeFromTime("g1", 301L);
        verify(queueRedisService, never()).enqueue(task);
    }

    @Test
    void shouldRefillTimeQueueForFutureRunnableTaskMissingInRedis() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask task = new SchedulerTask();
        task.setId(302L);
        task.setTaskNo("task-302");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setBizType("demo.biz");
        task.setBizKey("biz-302");
        task.setPriority(5);
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(now.plusMinutes(1));

        when(taskRepository.findRunnableForQueueRefill(any(LocalDateTime.class), eq(500))).thenReturn(List.of(task));
        when(queueRedisService.existsInTime("g1", 302L)).thenReturn(false);

        int refilled = recoveryService.refillQueue();

        assertEquals(1, refilled);
        verify(taskRepository).promotePendingToRunnable(any(LocalDateTime.class), eq(500));
        verify(queueRedisService).enqueue(task);
        verify(queueRedisService, never()).addToReady(task);
        verify(queueRedisService, never()).removeFromTime("g1", 302L);
    }
}
