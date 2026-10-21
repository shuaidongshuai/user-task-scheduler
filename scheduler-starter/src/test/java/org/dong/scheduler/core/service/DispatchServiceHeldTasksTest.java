package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.repo.UserConcurrencyConfigRepository;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.TaskRejectedException;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DispatchServiceHeldTasksTest {
    private final TaskRepository tasks = mock(TaskRepository.class);
    private final QueueRedisService queue = mock(QueueRedisService.class);
    private final ConcurrencyGuard guard = mock(ConcurrencyGuard.class);
    private final WorkerService worker = mock(WorkerService.class);
    private final TaskHandlerRegistry handlers = mock(TaskHandlerRegistry.class);
    private final DynamicUserLimitService limits = mock(DynamicUserLimitService.class);
    private final TaskStateService states = mock(TaskStateService.class);
    private GroupConfig group;
    private DispatchService service;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDispatchRoute("api");
        properties.setReadyScanPageLimit(1);
        group = new GroupConfig();
        group.setGroupCode("g");
        group.setMaxConcurrency(2);
        group.setDispatchBatchSize(2);
        group.setLockExpireSec(60);
        GroupConfigRepository groups = mock(GroupConfigRepository.class);
        when(groups.listEnabled()).thenReturn(List.of(group));
        when(queue.tryAcquireActiveUserLock(eq("g"), eq("api"), anyString(), anyLong())).thenReturn("lock");
        when(guard.groupRunning("g")).thenReturn(2L);
        when(guard.userRunning(eq("g"), anyString())).thenReturn(2L);
        when(handlers.find("biz")).thenReturn(mock(TaskHandler.class));
        when(worker.newExecuteNo()).thenReturn("execution");
        when(guard.acquireLease(anyLong(), eq("execution"), eq(60))).thenReturn(true);
        when(tasks.casWaitHoldToRunning(anyLong(), any(), anyString(), any())).thenReturn(true);
        service = new DispatchService(properties, groups, mock(UserConcurrencyConfigRepository.class), tasks,
                queue, guard, limits, worker, mock(RecoveryService.class), handlers,
                mock(BusinessTaskStateProviderRegistry.class), states);
    }

    @Test
    void shouldResumeOtherUsersHeldTasksBeforeBlockedHighestPriorityUser() {
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "other-owner");
        candidates(a, b);
        freshQueue("highest-priority", 3);

        service.dispatchOnce();

        var order = inOrder(worker, queue);
        order.verify(worker).submit(a, group, "execution");
        order.verify(worker).submit(b, group, "execution");
        order.verify(queue).peekNextActiveUser("g", "api");
        verify(worker, times(2)).submit(any(), any(), any());
        verify(guard, never()).tryAcquire(any(), any(), anyLong(), anyInt(), anyInt(), anyInt(), any());
        verify(guard, never()).repairRelease(any(), any());
    }

    @Test
    void shouldResumeHeldBeyondReadyPageWithoutAdmittingFreshTaskAtUserLimit() {
        group.setMaxConcurrency(5);
        SchedulerTask held = held(200, "owner");
        candidates(held);
        freshQueue("owner", 1);
        when(limits.calculate(eq(group), any(), anyLong())).thenReturn(2);

        service.dispatchOnce();

        verify(worker).submit(held, group, "execution");
        verify(worker, times(1)).submit(any(), any(), any());
        verify(guard, never()).tryAcquire(any(), any(), anyLong(), anyInt(), anyInt(), anyInt(), any());
    }

    @Test
    void shouldResumeHistoricalHeldTaskWithoutAnyRedisQueueReferenceOrWaitDeadlineCheck() {
        SchedulerTask held = held(1, "owner");
        held.setWaitDeadlineAt(LocalDateTime.now().minusDays(1));
        candidates(held);

        service.dispatchOnce();

        verify(worker).submit(held, group, "execution");
        verify(states, never()).markFailedByWaitDeadline(any(), any());
        verify(queue, never()).peekReadyHeadPriority(any(), any(), any());
    }

    @Test
    void shouldSkipWhenOldInstanceOwnsSameUserLock() {
        candidates(held(1, "owner"));
        when(queue.tryAcquireActiveUserLock(eq("g"), eq("api"), eq("owner"), anyLong())).thenReturn(null);

        service.dispatchOnce();

        verify(tasks, never()).findById(any());
        verify(worker, never()).submit(any(), any(), any());
        verify(guard, never()).acquireLease(anyLong(), any(), anyInt());
    }

    @Test
    void shouldReReadAfterUserLockAndSkipTaskAdvancedByOldInstance() {
        candidates(held(1, "owner"));
        SchedulerTask current = held(1, "owner");
        current.setStatus(TaskStatus.RUNNING);
        when(tasks.findById(1L)).thenReturn(Optional.of(current));

        service.dispatchOnce();

        var order = inOrder(queue, tasks);
        order.verify(queue).tryAcquireActiveUserLock(eq("g"), eq("api"), eq("owner"), anyLong());
        order.verify(tasks).findById(1L);
        verify(worker, never()).submit(any(), any(), any());
        verify(queue).releaseActiveUserLock("g", "api", "owner", "lock");
    }

    @Test
    void shouldNotExecuteNextRoundRescheduledByOldInstance() {
        candidates(held(1, "owner"));
        SchedulerTask current = held(1, "owner");
        current.setExecuteAt(LocalDateTime.now().plusMinutes(5));
        when(tasks.findById(1L)).thenReturn(Optional.of(current));

        service.dispatchOnce();

        verify(worker, never()).submit(any(), any(), any());
        verify(queue, never()).removeFromReadyQueue(any(SchedulerTask.class));
    }

    @Test
    void shouldRejectTaskMovedToAnotherRouteUnderLock() {
        candidates(held(1, "owner"));
        SchedulerTask current = held(1, "owner");
        current.setDispatchRoute("other");
        when(tasks.findById(1L)).thenReturn(Optional.of(current));

        service.dispatchOnce();

        verify(worker, never()).submit(any(), any(), any());
        verify(queue, never()).removeFromReadyQueue(any(SchedulerTask.class));
    }

    @Test
    void shouldSkipContendedLeaseAndContinueToAnotherHeldTask() {
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "owner");
        candidates(a, b);
        when(guard.acquireLease(1L, "execution", 60)).thenReturn(false);

        service.dispatchOnce();

        verify(worker).submit(b, group, "execution");
        verify(worker, times(1)).submit(any(), any(), any());
        verify(tasks, never()).casWaitHoldToRunning(eq(1L), any(), any(), any());
    }

    @Test
    void shouldReleaseOnlyLeaseOnCasMiss() {
        candidates(held(1, "owner"));
        when(tasks.casWaitHoldToRunning(eq(1L), any(), any(), any())).thenReturn(false);

        service.dispatchOnce();

        verify(guard).releaseLease(1L, "execution");
        verify(guard, never()).release(any(), any(), anyLong(), any());
        verify(guard, never()).repairRelease(any(), any());
        verify(worker, never()).submit(any(), any(), any());
    }

    @Test
    void shouldPreserveSlotAndStopFreshDispatchWhenWorkerRejects() {
        SchedulerTask held = held(1, "owner");
        candidates(held);
        doThrow(new TaskRejectedException("full")).when(worker).submit(held, group, "execution");
        when(tasks.rollbackToWaitHold(eq(1L), any(), any())).thenReturn(true);

        service.dispatchOnce();

        verify(guard).releaseLease(1L, "execution");
        verify(tasks).rollbackToWaitHold(eq(1L), any(), any());
        verify(queue).enqueue(held);
        verify(queue, never()).peekNextActiveUser(any(), any());
        verify(guard, never()).release(any(), any(), anyLong(), any());
        verify(guard, never()).repairRelease(any(), any());
    }

    @Test
    void shouldOnlyReleaseExhaustedHeldSlotAfterSuccessfulTerminalTransition() {
        SchedulerTask held = held(1, "owner");
        held.setHoldRoundCount(10);
        candidates(held);
        when(states.markFailed(eq(1L), eq("WAIT_HOLD_ROUNDS_EXHAUSTED"), any(), any())).thenReturn(true);

        service.dispatchOnce();

        verify(guard).repairRelease("g", "owner");
        verify(worker, never()).submit(any(), any(), any());
    }

    @Test
    void shouldNotReleaseSlotWhenExhaustedTerminalTransitionLosesRace() {
        SchedulerTask held = held(1, "owner");
        held.setHoldRoundCount(10);
        candidates(held);

        service.dispatchOnce();

        verify(states).markFailed(eq(1L), eq("WAIT_HOLD_ROUNDS_EXHAUSTED"), any(), any());
        verify(guard, never()).repairRelease(any(), any());
        verify(worker, never()).submit(any(), any(), any());
    }

    @Test
    void shouldReleaseUserLockAndContinueAfterOneCandidateReadFails() {
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "other-owner");
        candidates(a, b);
        when(tasks.findById(1L)).thenThrow(new IllegalStateException("transient read failure"));

        service.dispatchOnce();

        verify(queue).releaseActiveUserLock("g", "api", "owner", "lock");
        verify(worker).submit(b, group, "execution");
        verify(worker, times(1)).submit(any(), any(), any());
    }

    @Test
    void shouldRecheckCapacityForFreshTasksAfterHeldContinuation() {
        SchedulerTask held = held(1, "owner");
        candidates(held);
        freshQueue("fresh-owner", 3);
        when(guard.groupRunning("g")).thenReturn(2L, 2L, 1L);
        when(guard.userRunning("g", "fresh-owner")).thenReturn(0L);
        when(limits.calculate(eq(group), any(), anyLong())).thenReturn(2);
        when(guard.tryAcquire("g", "fresh-owner", 3L, 2, 2, 60, "execution")).thenReturn(true);
        when(tasks.casToRunning(eq(3L), eq("g"), eq(0), any(), any(), any())).thenReturn(true);

        service.dispatchOnce();

        var order = inOrder(worker);
        order.verify(worker).submit(held, group, "execution");
        order.verify(worker).submit(argThat(t -> t.getId() == 3L), eq(group), eq("execution"));
    }

    @Test
    void shouldContinueBoundedScanPastBlockedPageAndWrapOnLaterTick() {
        group.setDispatchBatchSize(1);
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "owner");
        b.setExecuteAt(a.getExecuteAt());
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), isNull(), isNull(), eq(1))).thenReturn(List.of(a));
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), eq(a.getExecuteAt()), eq(1L), eq(1))).thenReturn(List.of(b));
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), eq(b.getExecuteAt()), eq(2L), eq(1))).thenReturn(List.of());
        when(tasks.findById(1L)).thenReturn(Optional.of(a));
        when(tasks.findById(2L)).thenReturn(Optional.of(b));
        when(handlers.find("biz")).thenReturn(null);

        service.dispatchOnce();
        service.dispatchOnce();
        service.dispatchOnce();

        verify(tasks, times(2)).findById(1L);
        verify(tasks).findById(2L);
        verify(worker, never()).submit(any(), any(), any());
    }

    @Test
    void shouldRevisitBlockedHeadDespiteAnotherTaskContinuouslyBecomingDue() {
        group.setDispatchBatchSize(1);
        LocalDateTime start = LocalDateTime.of(2026, 10, 21, 10, 0);
        AtomicReference<LocalDateTime> now = new AtomicReference<>(start);
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "other-owner");
        a.setExecuteAt(start.minusSeconds(10));
        b.setExecuteAt(start.minusSeconds(9));
        when(tasks.findById(1L)).thenReturn(Optional.of(a));
        when(tasks.findById(2L)).thenReturn(Optional.of(b));
        when(guard.acquireLease(1L, "execution", 60)).thenReturn(false);
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), any(), any(), eq(1))).thenAnswer(call -> {
            LocalDateTime cutoff = call.getArgument(2);
            LocalDateTime afterTime = call.getArgument(3);
            Long afterId = call.getArgument(4);
            return List.of(a, b).stream()
                    .filter(t -> !t.getExecuteAt().isAfter(cutoff))
                    .filter(t -> afterTime == null || t.getExecuteAt().isAfter(afterTime)
                            || (t.getExecuteAt().equals(afterTime) && t.getId() > afterId))
                    .sorted(Comparator.comparing(SchedulerTask::getExecuteAt).thenComparing(SchedulerTask::getId))
                    .limit(1).toList();
        });
        doAnswer(call -> {
            b.setExecuteAt(now.get().plusSeconds(1));
            return null;
        }).when(worker).submit(b, group, "execution");

        try (var time = mockStatic(LocalDateTime.class, CALLS_REAL_METHODS)) {
            time.when(LocalDateTime::now).thenAnswer(call -> now.get());
            service.dispatchOnce(); // A is skipped; the sweep cutoff stays at start.
            now.set(start.plusSeconds(1));
            service.dispatchOnce(); // B requeues itself for start + 2 seconds.
            now.set(start.plusSeconds(2));
            service.dispatchOnce(); // B is due again, but the old sweep ends and A is revisited first.
        }

        verify(guard, times(2)).acquireLease(1L, "execution", 60);
        verify(worker, times(1)).submit(b, group, "execution");
        verify(tasks).findDueHeldTasks("g", "api", start, a.getExecuteAt(), 1L, 1);
        verify(tasks).findDueHeldTasks("g", "api", start, start.minusSeconds(9), 2L, 1);
        verify(tasks).findDueHeldTasks("g", "api", start.plusSeconds(2), null, null, 1);
        assertEquals(start.plusSeconds(2), b.getExecuteAt());
    }

    @Test
    void shouldPickUpTaskBecomingDueDuringSweepInTheNextSweep() {
        group.setDispatchBatchSize(1);
        LocalDateTime start = LocalDateTime.of(2026, 10, 21, 10, 0);
        AtomicReference<LocalDateTime> now = new AtomicReference<>(start);
        SchedulerTask a = held(1, "owner");
        SchedulerTask b = held(2, "owner");
        SchedulerTask c = held(3, "owner");
        a.setExecuteAt(start.minusSeconds(2));
        b.setExecuteAt(start.minusSeconds(1));
        c.setExecuteAt(start.plusSeconds(1));
        when(tasks.findById(1L)).thenReturn(Optional.of(a));
        when(tasks.findById(2L)).thenReturn(Optional.of(b));
        when(tasks.findById(3L)).thenReturn(Optional.of(c));
        when(handlers.find("biz")).thenReturn(null);
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), any(), any(), eq(1))).thenAnswer(call -> {
            LocalDateTime cutoff = call.getArgument(2);
            LocalDateTime afterTime = call.getArgument(3);
            Long afterId = call.getArgument(4);
            return List.of(a, b, c).stream()
                    .filter(t -> !t.getExecuteAt().isAfter(cutoff))
                    .filter(t -> afterTime == null || t.getExecuteAt().isAfter(afterTime)
                            || (t.getExecuteAt().equals(afterTime) && t.getId() > afterId))
                    .sorted(Comparator.comparing(SchedulerTask::getExecuteAt).thenComparing(SchedulerTask::getId))
                    .limit(1).toList();
        });

        try (var time = mockStatic(LocalDateTime.class, CALLS_REAL_METHODS)) {
            time.when(LocalDateTime::now).thenAnswer(call -> now.get());
            service.dispatchOnce();
            now.set(start.plusSeconds(1));
            service.dispatchOnce();
            verify(tasks, never()).findById(3L); // Due now, but outside the first sweep's fixed window.
            for (int second = 2; second <= 4; second++) {
                now.set(start.plusSeconds(second));
                service.dispatchOnce();
            }
        }

        verify(tasks).findById(3L);
        verify(tasks).findDueHeldTasks("g", "api", start, b.getExecuteAt(), 2L, 1);
        verify(tasks).findDueHeldTasks("g", "api", start.plusSeconds(2), b.getExecuteAt(), 2L, 1);
        verify(worker, never()).submit(any(), any(), any());
    }

    @Test
    void shouldNotRetrySameHeldCandidateFromLegacyReadyQueueInSameTick() {
        SchedulerTask held = held(1, "owner");
        candidates(held);
        freshQueue("owner", 1);
        when(tasks.findByIds(List.of(1L))).thenReturn(Map.of(1L, held));
        when(guard.acquireLease(1L, "execution", 60)).thenReturn(false);

        service.dispatchOnce();

        verify(guard, times(1)).acquireLease(1L, "execution", 60);
        verify(worker, never()).submit(any(), any(), any());
    }

    private void candidates(SchedulerTask... held) {
        when(tasks.findDueHeldTasks(eq("g"), eq("api"), any(), any(), any(), eq(2))).thenReturn(List.of(held));
        for (SchedulerTask task : held) {
            when(tasks.findById(task.getId())).thenReturn(Optional.of(task));
        }
    }

    private void freshQueue(String user, long id) {
        SchedulerTask fresh = held(id, user);
        fresh.setStatus(TaskStatus.RUNNABLE);
        when(queue.peekNextActiveUser("g", "api")).thenReturn(user);
        when(queue.peekReadyHeadPriority("g", "api", user)).thenReturn(0);
        when(queue.peekReadyTasksByPriority("g", "api", user, 0, 2)).thenReturn(List.of(id));
        when(tasks.findByIds(List.of(id))).thenReturn(Map.of(id, fresh));
    }

    private SchedulerTask held(long id, String user) {
        SchedulerTask task = new SchedulerTask();
        task.setId(id);
        task.setGroupCode("g");
        task.setDispatchRoute("api");
        task.setUserId(user);
        task.setBizType("biz");
        task.setStatus(TaskStatus.WAIT_HOLD);
        task.setExecuteAt(LocalDateTime.now().minusMinutes(1));
        task.setHoldRoundCount(1);
        task.setHoldMaxRounds(10);
        task.setHoldRetryDelaySec(3);
        return task;
    }
}
