package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.task.TaskRejectedException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DispatchServiceTest {
    private static final String ROUTE = "route-a";
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
        properties.setDispatchRoute(ROUTE);
        TaskHandlerRegistry taskHandlerRegistry = new TaskHandlerRegistry(List.of(new TaskHandler() {
            @Override
            public List<String> bizTypes() {
                return List.of("biz");
            }

            @Override
            public TaskExecuteResult execute(SchedulerTask task) {
                throw new UnsupportedOperationException("not used in dispatch tests");
            }
        }));
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
                taskHandlerRegistry,
                providerRegistry,
                taskStateService
        );
    }

    @Test
    void shouldSkipTaskWithoutHandlerBeforeCheckingBusinessState() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(2);
        group.setDispatchBatchSize(1);

        SchedulerTask task = runnableTask(701L, "u1");
        task.setBizType("demo.biz");

        BusinessTaskStateProvider stateProvider = new BusinessTaskStateProvider() {
            @Override
            public String bizType() {
                return "demo.biz";
            }

            @Override
            public org.dong.scheduler.core.enums.BusinessTaskState query(SchedulerTask ignored) {
                throw new AssertionError("state provider should not be queried when handler is missing");
            }
        };

        dispatchService = new DispatchService(
                properties,
                groupConfigRepository,
                taskRepository,
                queueRedisService,
                concurrencyGuard,
                dynamicUserLimitService,
                workerService,
                recoveryService,
                new TaskHandlerRegistry(List.of()),
                new BusinessTaskStateProviderRegistry(List.of(stateProvider)),
                taskStateService
        );

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 1)).thenReturn(List.of(701L), List.of());
        when(taskRepository.findByIds(List.of(701L))).thenReturn(Map.of(701L, task));

        dispatchService.dispatchOnce();

        verify(workerService, never()).newExecuteNo();
        verify(concurrencyGuard, never()).tryAcquire(any(), any(), anyLong(), anyInt(), anyInt(), anyInt(), any());
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(100)))
                .thenReturn(List.of(401L));
        when(taskRepository.findByIds(List.of(401L))).thenReturn(Map.of(401L, task));
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(3L, 3L, 3L, 3L, 5L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 2)).thenReturn(List.of(101L, 102L));
        when(queueRedisService.peekReady("g1", ROUTE, 2, 2)).thenReturn(List.of(103L, 104L));
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
        verify(queueRedisService).removeFromReady("g1", ROUTE, 103L);
        verify(queueRedisService).peekReady("g1", ROUTE, 0, 2);
        verify(queueRedisService).peekReady("g1", ROUTE, 2, 2);
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 2)).thenReturn(List.of());

        dispatchService.dispatchOnce();

        verify(queueRedisService).peekReady("g1", ROUTE, 0, 2);
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 0L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 1)).thenReturn(List.of(301L));
        when(queueRedisService.peekReady("g1", ROUTE, 1, 1)).thenReturn(List.of(302L));
        when(taskRepository.findByIds(List.of(301L))).thenReturn(Map.of(301L, first));
        when(taskRepository.findByIds(List.of(302L))).thenReturn(Map.of(302L, second));
        when(dynamicUserLimitService.calculate(group, 0L)).thenReturn(1);
        when(workerService.newExecuteNo()).thenReturn("exec-301", "exec-302");
        when(concurrencyGuard.tryAcquire("g1", "u-full", 301L, 5, 1, 60, "exec-301")).thenReturn(false);
        when(concurrencyGuard.tryAcquire("g1", "u-full", 302L, 5, 1, 60, "exec-302")).thenReturn(false);
        when(concurrencyGuard.userRunning("g1", "u-full")).thenReturn(0L);

        dispatchService.dispatchOnce();

        verify(queueRedisService).peekReady("g1", ROUTE, 0, 1);
        verify(queueRedisService).peekReady("g1", ROUTE, 1, 1);
        verify(queueRedisService, never()).peekReady("g1", ROUTE, 2, 1);
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 1L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 1)).thenReturn(List.of(201L), List.of(202L));
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

        verify(queueRedisService, times(2)).peekReady("g1", ROUTE, 0, 1);
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
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(1))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 1)).thenReturn(List.of(501L), List.of());
        when(taskRepository.findByIds(List.of(501L))).thenReturn(Map.of(501L, timedOut));
        when(taskStateService.markFailedByWaitDeadline(eq(501L), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(taskRepository).findByIds(List.of(501L));
        verify(taskStateService).markFailedByWaitDeadline(eq(501L), any(LocalDateTime.class));
        verify(queueRedisService).removeFromReady("g1", ROUTE, 501L);
        verify(workerService, never()).submit(any(), eq(group), any());
    }

    @Test
    void shouldStopCurrentDispatchRoundAfterWorkerPoolReject() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(5);
        group.setDispatchBatchSize(2);
        group.setLockExpireSec(60);

        SchedulerTask first = runnableTask(601L, "u1");
        SchedulerTask second = runnableTask(602L, "u2");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekReady("g1", ROUTE, 0, 2)).thenReturn(List.of(601L, 602L));
        when(taskRepository.findByIds(List.of(601L, 602L))).thenReturn(Map.of(
                601L, first,
                602L, second
        ));
        when(dynamicUserLimitService.calculate(group, 0L)).thenReturn(4);
        when(workerService.newExecuteNo()).thenReturn("exec-601");
        when(concurrencyGuard.tryAcquire("g1", "u1", 601L, 5, 4, 60, "exec-601")).thenReturn(true);
        when(taskRepository.casToRunning(eq(601L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class)))
                .thenReturn(true);
        doThrow(new TaskRejectedException("worker pool full"))
                .when(workerService).submit(first, group, "exec-601");
        when(concurrencyGuard.release("g1", "u1", 601L, "exec-601")).thenReturn(true);
        when(taskRepository.rescheduleToRunnable(eq(601L), any(LocalDateTime.class),
                eq("DISPATCH_SUBMIT_REJECTED"), any(), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(workerService).submit(first, group, "exec-601");
        verify(workerService, never()).submit(second, group, "exec-602");
        verify(taskRepository, never()).casToRunning(eq(602L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class));
        verify(queueRedisService).removeFromReady("g1", ROUTE, 601L);
        verify(queueRedisService).enqueue(first);
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
