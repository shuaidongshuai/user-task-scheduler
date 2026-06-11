package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DispatchServiceTest {
    private SchedulerProperties properties;

    @Mock
    private GroupConfigRepository groupConfigRepository;
    @Mock
    private TaskRepository taskRepository;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private DynamicUserLimitService dynamicUserLimitService;
    @Mock
    private WorkerService workerService;
    @Mock
    private RecoveryService recoveryService;
    @Mock
    private TaskStateService taskStateService;

    private DispatchService dispatchService;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        BusinessTaskStateProviderRegistry providerRegistry = new BusinessTaskStateProviderRegistry(List.of());
        dispatchService = new DispatchService(
                properties,
                groupConfigRepository,
                taskRepository,
                queueRedisService,
                concurrencyGuard,
                dynamicUserLimitService,
                workerService,
                recoveryService,
                providerRegistry,
                taskStateService
        );
    }

    @Test
    void shouldAddPromotedRunnableTaskToReadyWithoutRecheckingDueFlag() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(10);
        group.setDispatchBatchSize(100);

        SchedulerTask task = new SchedulerTask();
        task.setId(401L);
        task.setTaskNo("task-401");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(LocalDateTime.now().plusMinutes(1));

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(100)))
                .thenReturn(List.of(401L));
        when(taskRepository.findById(401L)).thenReturn(Optional.of(task));
        when(concurrencyGuard.groupRunning("g1")).thenReturn(10L);

        dispatchService.dispatchOnce();

        verify(queueRedisService).addToReady(task);
        verify(queueRedisService, never()).enqueue(task);
    }

    @Test
    void shouldPageReadyQueueAndSkipSaturatedUserInSameRound() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(5);
        group.setDispatchBatchSize(2);
        group.setLockExpireSec(60);

        SchedulerTask saturatedFirst = runnableTask(101L, "u-full");
        SchedulerTask saturatedSecond = runnableTask(102L, "u-full");
        SchedulerTask runnableOtherUser = runnableTask(103L, "u-ok");
        SchedulerTask saturatedThird = runnableTask(104L, "u-full");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(3L, 3L, 3L, 3L, 5L);
        when(queueRedisService.peekReady("g1", 0, 2)).thenReturn(List.of(101L, 102L));
        when(queueRedisService.peekReady("g1", 2, 2)).thenReturn(List.of(103L, 104L));
        when(taskRepository.findByIds(List.of(101L, 102L))).thenReturn(Map.of(
                101L, saturatedFirst,
                102L, saturatedSecond
        ));
        when(taskRepository.findByIds(List.of(103L, 104L))).thenReturn(Map.of(
                103L, runnableOtherUser,
                104L, saturatedThird
        ));
        when(dynamicUserLimitService.calculate(group, 3L)).thenReturn(3);
        when(concurrencyGuard.tryAcquire("g1", "u-full", 101L, 5, 3, 60, "exec-103")).thenReturn(false);
        when(concurrencyGuard.userRunning("g1", "u-full")).thenReturn(3L);
        when(workerService.newExecuteNo()).thenReturn("exec-103", "exec-104");
        when(concurrencyGuard.tryAcquire("g1", "u-ok", 103L, 5, 3, 60, "exec-104")).thenReturn(true);
        when(taskRepository.casToRunning(eq(103L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class)))
                .thenReturn(true);

        dispatchService.dispatchOnce();

        verify(concurrencyGuard).tryAcquire("g1", "u-full", 101L, 5, 3, 60, "exec-103");
        verify(concurrencyGuard, never()).tryAcquire(eq("g1"), eq("u-full"), eq(102L), eq(5), eq(3), eq(60), eq("exec-104"));
        verify(concurrencyGuard, never()).tryAcquire(eq("g1"), eq("u-full"), eq(104L), eq(5), eq(3), eq(60), eq("exec-104"));
        verify(workerService).submit(runnableOtherUser, group, "exec-104");
        verify(queueRedisService).removeFromReady("g1", 103L);
        verify(queueRedisService).peekReady("g1", 0, 2);
        verify(queueRedisService).peekReady("g1", 2, 2);
        verify(taskRepository).findByIds(List.of(101L, 102L));
        verify(taskRepository).findByIds(List.of(103L, 104L));
        verify(workerService, times(2)).newExecuteNo();
    }

    @Test
    void shouldUseDispatchBatchSizeAsReadyPageSize() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(5);
        group.setDispatchBatchSize(2);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L);
        when(queueRedisService.peekReady("g1", 0, 2)).thenReturn(List.of());

        dispatchService.dispatchOnce();

        verify(queueRedisService).peekReady("g1", 0, 2);
    }

    @Test
    void shouldStopReadyScanAtConfiguredPageLimit() {
        properties.setReadyScanPageLimit(2);

        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(5);
        group.setDispatchBatchSize(1);
        group.setLockExpireSec(60);

        SchedulerTask first = runnableTask(301L, "u-full");
        SchedulerTask second = runnableTask(302L, "u-full");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 0L);
        when(queueRedisService.peekReady("g1", 0, 1)).thenReturn(List.of(301L));
        when(queueRedisService.peekReady("g1", 1, 1)).thenReturn(List.of(302L));
        when(taskRepository.findByIds(List.of(301L))).thenReturn(Map.of(301L, first));
        when(taskRepository.findByIds(List.of(302L))).thenReturn(Map.of(302L, second));
        when(dynamicUserLimitService.calculate(group, 0L)).thenReturn(1);
        when(workerService.newExecuteNo()).thenReturn("exec-301", "exec-302");
        when(concurrencyGuard.tryAcquire("g1", "u-full", 301L, 5, 1, 60, "exec-301")).thenReturn(false);
        when(concurrencyGuard.tryAcquire("g1", "u-full", 302L, 5, 1, 60, "exec-302")).thenReturn(false);
        when(concurrencyGuard.userRunning("g1", "u-full")).thenReturn(0L);

        dispatchService.dispatchOnce();

        verify(queueRedisService).peekReady("g1", 0, 1);
        verify(queueRedisService).peekReady("g1", 1, 1);
        verify(queueRedisService, never()).peekReady("g1", 2, 1);
    }

    @Test
    void shouldRepeatReadyScanWhileGroupStillHasCapacity() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(2);
        group.setDispatchBatchSize(1);
        group.setLockExpireSec(60);

        SchedulerTask first = runnableTask(201L, "u1");
        SchedulerTask second = runnableTask(202L, "u2");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 1L);
        when(queueRedisService.peekReady("g1", 0, 1)).thenReturn(List.of(201L), List.of(202L));
        when(taskRepository.findByIds(List.of(201L))).thenReturn(Map.of(201L, first));
        when(taskRepository.findByIds(List.of(202L))).thenReturn(Map.of(202L, second));
        when(dynamicUserLimitService.calculate(group, 0L)).thenReturn(1);
        when(dynamicUserLimitService.calculate(group, 1L)).thenReturn(1);
        when(workerService.newExecuteNo()).thenReturn("exec-201", "exec-202");
        when(concurrencyGuard.tryAcquire("g1", "u1", 201L, 2, 1, 60, "exec-201")).thenReturn(true);
        when(concurrencyGuard.tryAcquire("g1", "u2", 202L, 2, 1, 60, "exec-202")).thenReturn(true);
        when(taskRepository.casToRunning(eq(201L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class)))
                .thenReturn(true);
        when(taskRepository.casToRunning(eq(202L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class)))
                .thenReturn(true);

        dispatchService.dispatchOnce();

        verify(queueRedisService, times(2)).peekReady("g1", 0, 1);
        verify(taskRepository).findByIds(List.of(201L));
        verify(taskRepository).findByIds(List.of(202L));
        verify(workerService).submit(first, group, "exec-201");
        verify(workerService).submit(second, group, "exec-202");
    }

    @Test
    void shouldFailTimedOutTaskBeforeDispatchingToWorker() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(2);
        group.setDispatchBatchSize(1);

        SchedulerTask timedOut = runnableTask(501L, "u1");
        timedOut.setWaitDeadlineAt(LocalDateTime.now().minusSeconds(1));

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L);
        when(queueRedisService.peekReady("g1", 0, 1)).thenReturn(List.of(501L), List.of());
        when(taskRepository.findByIds(List.of(501L))).thenReturn(Map.of(501L, timedOut));
        when(taskStateService.markFailedByWaitDeadline(eq(501L), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(taskRepository).findByIds(List.of(501L));
        verify(taskStateService).markFailedByWaitDeadline(eq(501L), any(LocalDateTime.class));
        verify(queueRedisService).removeFromReady("g1", 501L);
        verify(workerService, never()).submit(any(), eq(group), any());
    }

    private SchedulerTask runnableTask(long id, String userId) {
        SchedulerTask task = new SchedulerTask();
        task.setId(id);
        task.setTaskNo("task-" + id);
        task.setGroupCode("g1");
        task.setUserId(userId);
        task.setBizType("biz");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setPriority(0);
        task.setExecuteAt(LocalDateTime.now().minusSeconds(1));
        task.setCreateTime(LocalDateTime.now().minusSeconds(10));
        return task;
    }
}
