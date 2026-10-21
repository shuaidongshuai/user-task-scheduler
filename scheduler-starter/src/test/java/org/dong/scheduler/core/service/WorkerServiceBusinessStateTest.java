package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkerServiceBusinessStateTest {
    private final TaskRepository tasks = mock(TaskRepository.class);
    private final ConcurrencyGuard guard = mock(ConcurrencyGuard.class);
    private final QueueRedisService queue = mock(QueueRedisService.class);
    private final TaskHandlerRegistry handlers = mock(TaskHandlerRegistry.class);
    private final TaskStateService states = mock(TaskStateService.class);
    private final BusinessTaskStateProvider provider = mock(BusinessTaskStateProvider.class);
    private WorkerService worker;
    private SchedulerTask task;
    private GroupConfig group;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setHeartbeatIntervalSec(60);
        properties.setDefaultRetryDelaySec(30);
        BusinessTaskStateProviderRegistry providers = mock(BusinessTaskStateProviderRegistry.class);
        when(providers.find("biz")).thenReturn(provider);
        worker = new WorkerService(properties, tasks, handlers, guard, queue, mock(RecoveryService.class),
                mock(ThreadPoolTaskExecutor.class), providers, states);
        task = new SchedulerTask();
        task.setId(1L);
        task.setGroupCode("g");
        task.setUserId("owner");
        task.setBizType("biz");
        // Dispatch CAS changes the database to RUNNING but preserves the source status in this snapshot.
        task.setStatus(TaskStatus.WAIT_HOLD);
        task.setHoldRoundCount(2);
        task.setHoldRetryDelaySec(7);
        group = new GroupConfig();
        group.setLockExpireSec(60);
    }

    @AfterEach
    void tearDown() {
        worker.shutdown();
    }

    @Test
    void shouldKeepHeldSlotAndUseHoldDelayWhenBusinessStateIsUnknown() {
        when(provider.query(task)).thenReturn(BusinessTaskState.UNKNOWN);
        when(tasks.rollbackToWaitHold(eq(1L), any(), any())).thenReturn(true);
        LocalDateTime before = LocalDateTime.now();

        worker.executeDirect(task, group, "execution");

        ArgumentCaptor<LocalDateTime> nextCheck = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(tasks).rollbackToWaitHold(eq(1L), nextCheck.capture(), any());
        assertFalse(nextCheck.getValue().isBefore(before.plusSeconds(7)));
        assertFalse(nextCheck.getValue().isAfter(LocalDateTime.now().plusSeconds(7)));
        assertEquals(TaskStatus.WAIT_HOLD, task.getStatus());
        assertEquals(nextCheck.getValue(), task.getExecuteAt());
        assertEquals(2, task.getHoldRoundCount());
        verify(queue).enqueue(task);
        verify(guard).releaseLease(1L, "execution");
        verify(guard, never()).release(any(), any(), anyLong(), any());
        verify(tasks, never()).rescheduleToRunnable(anyLong(), any(), any(), any(), any());
        verify(tasks, never()).markWaitHold(anyLong(), any(), any(), any());
        verify(handlers, never()).find(any());
        verify(tasks).finishExecution(eq("execution"), eq(TaskStatus.WAIT_HOLD), isNull(), isNull(), any());
    }

    @Test
    void shouldKeepFreshTaskDeferralAndReleaseBehaviorUnchanged() {
        task.setStatus(TaskStatus.RUNNABLE);
        when(provider.query(task)).thenReturn(BusinessTaskState.UNKNOWN);
        when(tasks.rescheduleToRunnable(eq(1L), any(), eq("BIZ_STATE_NOT_READY"), any(), any())).thenReturn(true);
        when(guard.release("g", "owner", 1L, "execution")).thenReturn(true);
        LocalDateTime before = LocalDateTime.now();

        worker.executeDirect(task, group, "execution");

        ArgumentCaptor<LocalDateTime> nextCheck = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(tasks).rescheduleToRunnable(eq(1L), nextCheck.capture(), eq("BIZ_STATE_NOT_READY"), any(), any());
        assertFalse(nextCheck.getValue().isBefore(before.plusSeconds(30)));
        assertFalse(nextCheck.getValue().isAfter(LocalDateTime.now().plusSeconds(30)));
        verify(guard).release("g", "owner", 1L, "execution");
        verify(guard, never()).releaseLease(anyLong(), any());
        verify(tasks, never()).rollbackToWaitHold(anyLong(), any(), any());
        verify(queue).enqueue(task);
        verify(handlers, never()).find(any());
        verify(tasks).finishExecution(eq("execution"), eq(TaskStatus.WAIT_RETRY), isNull(), isNull(), any());
    }

    @Test
    void shouldReleaseHeldSlotWhenBusinessStateIsSuccess() {
        when(provider.query(task)).thenReturn(BusinessTaskState.SUCCESS);
        when(states.markSuccess(eq(1L), any())).thenReturn(true);
        when(guard.release("g", "owner", 1L, "execution")).thenReturn(true);

        worker.executeDirect(task, group, "execution");

        verify(states).markSuccess(eq(1L), any());
        verify(guard).release("g", "owner", 1L, "execution");
        verify(guard, never()).releaseLease(anyLong(), any());
        verify(queue, never()).enqueue(any());
        verify(tasks).finishExecution(eq("execution"), eq(TaskStatus.SUCCESS), isNull(), isNull(), any());
    }

    @Test
    void shouldReleaseHeldSlotWhenBusinessStateIsFailed() {
        when(provider.query(task)).thenReturn(BusinessTaskState.FAILED);
        when(states.markFailed(eq(1L), eq("BIZ_FAILED"), any(), any())).thenReturn(true);
        when(guard.release("g", "owner", 1L, "execution")).thenReturn(true);

        worker.executeDirect(task, group, "execution");

        verify(states).markFailed(eq(1L), eq("BIZ_FAILED"), any(), any());
        verify(guard).release("g", "owner", 1L, "execution");
        verify(guard, never()).releaseLease(anyLong(), any());
        verify(queue, never()).enqueue(any());
        verify(tasks).finishExecution(eq("execution"), eq(TaskStatus.FAILED), isNull(), isNull(), any());
    }
}
