package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryServiceWaitTimeoutLockTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TaskStateService taskStateService;

    private RecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setScheduledJobLockSec(10);
        properties.setRecoveryScanLimit(200);
        recoveryService = new RecoveryService(properties, taskRepository, concurrencyGuard, queueRedisService, taskStateService);
    }

    @Test
    void shouldSkipExpireWaitingTasksWhenLockBusy() {
        when(concurrencyGuard.tryAcquireJobLock(eq("expire-waiting"), anyString(), eq(10))).thenReturn(false);

        int expired = recoveryService.expireWaitingTasks();

        assertEquals(0, expired);
        verify(taskStateService, never()).expireWaitingTasks(anyInt(), any());
        verify(concurrencyGuard, never()).releaseJobLock(eq("expire-waiting"), anyString());
    }

    @Test
    void shouldReleaseLockAfterExpireWaitingTasks() {
        when(concurrencyGuard.tryAcquireJobLock(eq("expire-waiting"), anyString(), eq(10))).thenReturn(true);
        when(taskStateService.expireWaitingTasks(eq(200), any())).thenReturn(3);

        int expired = recoveryService.expireWaitingTasks();

        assertEquals(3, expired);
        verify(taskStateService).expireWaitingTasks(eq(200), any());
        verify(concurrencyGuard).releaseJobLock(eq("expire-waiting"), anyString());
    }
}
