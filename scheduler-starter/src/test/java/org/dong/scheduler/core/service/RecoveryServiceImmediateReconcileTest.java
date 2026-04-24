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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryServiceImmediateReconcileTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private QueueRedisService queueRedisService;

    private RecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setInstanceId("ins-test");
        properties.setReconcileLockSec(30);
        properties.setImmediateReconcileThrottleSec(3);
        recoveryService = new RecoveryService(properties, taskRepository, concurrencyGuard, queueRedisService);
    }

    @Test
    void shouldSkipImmediateReconcileWhenThrottleDenied() {
        when(concurrencyGuard.tryAcquireGroupReconcileThrottle("g1", 3)).thenReturn(false);

        boolean reconciled = recoveryService.reconcileRunningCountersImmediately("g1", "u1", "unit-test");

        assertFalse(reconciled);
        verify(concurrencyGuard).tryAcquireGroupReconcileThrottle("g1", 3);
        verify(concurrencyGuard, never()).tryAcquireReconcileLock(anyString(), anyInt());
        verify(taskRepository, never()).countRunningByGroup(anyString());
    }

    @Test
    void shouldSkipImmediateReconcileWhenGlobalLockBusy() {
        when(concurrencyGuard.tryAcquireGroupReconcileThrottle("g1", 3)).thenReturn(true);
        when(concurrencyGuard.tryAcquireReconcileLock(anyString(), eq(30))).thenReturn(false);

        boolean reconciled = recoveryService.reconcileRunningCountersImmediately("g1", "u1", "unit-test");

        assertFalse(reconciled);
        verify(concurrencyGuard).tryAcquireGroupReconcileThrottle("g1", 3);
        verify(concurrencyGuard).tryAcquireReconcileLock(anyString(), eq(30));
        verify(taskRepository, never()).countRunningByGroup(anyString());
        verify(concurrencyGuard, never()).releaseReconcileLock(anyString());
    }

    @Test
    void shouldReconcileByUserHintWithoutGlobalUserScan() {
        when(concurrencyGuard.tryAcquireGroupReconcileThrottle("g1", 3)).thenReturn(true);
        when(concurrencyGuard.tryAcquireReconcileLock(anyString(), eq(30))).thenReturn(true);
        when(taskRepository.countRunningByGroup("g1")).thenReturn(0L);
        when(taskRepository.countRunningByUserInGroup("g1", "u1")).thenReturn(0L);
        when(concurrencyGuard.groupRunning("g1")).thenReturn(1L, 0L, 0L);
        when(concurrencyGuard.userRunning("g1", "u1")).thenReturn(1L, 0L, 0L);
        when(concurrencyGuard.reduceUserAndGroupRunning("g1", "u1", 1L)).thenReturn(1L);

        boolean reconciled = recoveryService.reconcileRunningCountersImmediately("g1", "u1", "unit-test");

        assertTrue(reconciled);
        verify(concurrencyGuard).reduceUserAndGroupRunning("g1", "u1", 1L);
        verify(concurrencyGuard, never()).listUserRunning(anyString());
        verify(concurrencyGuard, never()).setGroupRunning(anyString(), anyInt());
        verify(concurrencyGuard).releaseReconcileLock(anyString());
    }

    @Test
    void shouldForceGroupBaselineWhenUserHintCannotReduceOverflow() {
        when(concurrencyGuard.tryAcquireGroupReconcileThrottle("g1", 3)).thenReturn(true);
        when(concurrencyGuard.tryAcquireReconcileLock(anyString(), eq(30))).thenReturn(true);
        when(taskRepository.countRunningByGroup("g1")).thenReturn(0L);
        when(taskRepository.countRunningByUserInGroup("g1", "u1")).thenReturn(0L);
        when(concurrencyGuard.groupRunning("g1")).thenReturn(1L, 1L, 0L);
        when(concurrencyGuard.userRunning("g1", "u1")).thenReturn(0L, 0L);

        boolean reconciled = recoveryService.reconcileRunningCountersImmediately("g1", "u1", "unit-test");

        assertTrue(reconciled);
        verify(concurrencyGuard, never()).reduceUserAndGroupRunning(anyString(), anyString(), anyInt());
        verify(concurrencyGuard).setGroupRunning("g1", 0L);
        verify(concurrencyGuard).releaseReconcileLock(anyString());
    }
}
