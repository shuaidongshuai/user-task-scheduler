package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.model.UserConcurrencyConfig;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.repo.UserConcurrencyConfigRepository;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DispatchServiceTest {
    private static final String ROUTE = "route-a";

    private SchedulerProperties properties;

    @Mock
    private GroupConfigRepository groupConfigRepository;
    @Mock
    private UserConcurrencyConfigRepository userConcurrencyConfigRepository;
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
        properties.setActiveUserLockTtlMs(5000);
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
                userConcurrencyConfigRepository,
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
    void shouldProcessOnlyCurrentHighestPriorityPageThenRotateToNextUser() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask u1p5a = runnableTask(101L, "u1", 5);
        SchedulerTask u1p5b = runnableTask(102L, "u1", 5);
        SchedulerTask u2p1 = runnableTask(201L, "u2", 1);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 2L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", "u2", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u2", 5000L)).thenReturn("lock-u2");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(5, 9);
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u2")).thenReturn(1, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 5, 2)).thenReturn(List.of(101L, 102L));
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u2", 1, 2)).thenReturn(List.of(201L));
        when(taskRepository.findByIds(List.of(101L, 102L))).thenReturn(Map.of(101L, u1p5a, 102L, u1p5b));
        when(taskRepository.findByIds(List.of(201L))).thenReturn(Map.of(201L, u2p1));
        when(dynamicUserLimitService.calculate(eq(group), isNull(), anyLong())).thenReturn(2);
        when(workerService.newExecuteNo()).thenReturn("exec-101", "exec-102", "exec-201");
        when(concurrencyGuard.tryAcquire("g1", "u1", 101L, 5, 2, 60, "exec-101")).thenReturn(true);
        when(concurrencyGuard.tryAcquire("g1", "u1", 102L, 5, 2, 60, "exec-102")).thenReturn(true);
        when(concurrencyGuard.tryAcquire("g1", "u2", 201L, 5, 2, 60, "exec-201")).thenReturn(true);
        when(taskRepository.casToRunning(eq(101L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);
        when(taskRepository.casToRunning(eq(102L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);
        when(taskRepository.casToRunning(eq(201L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(queueRedisService).peekReadyTasksByPriority("g1", ROUTE, "u1", 5, 2);
        verify(queueRedisService, never()).peekReadyTasksByPriority("g1", ROUTE, "u1", 9, 2);
        verify(queueRedisService).peekReadyTasksByPriority("g1", ROUTE, "u2", 1, 2);
        verify(queueRedisService).rebalanceActiveUser("g1", ROUTE, "u1");
        verify(queueRedisService).rebalanceActiveUser("g1", ROUTE, "u2");
        verify(workerService).submit(u1p5a, group, "exec-101");
        verify(workerService).submit(u1p5b, group, "exec-102");
        verify(workerService).submit(u2p1, group, "exec-201");
    }

    @Test
    void shouldRotateToNextUserWhenCurrentUserHitsConcurrencyLimit() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask u1p0 = runnableTask(301L, "u1", 0);
        SchedulerTask u2p9 = runnableTask(401L, "u2", 9);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L, 1L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", "u2", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u2", 5000L)).thenReturn("lock-u2");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, 0);
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u2")).thenReturn(9, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(301L));
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u2", 9, 2)).thenReturn(List.of(401L));
        when(taskRepository.findByIds(List.of(301L))).thenReturn(Map.of(301L, u1p0));
        when(taskRepository.findByIds(List.of(401L))).thenReturn(Map.of(401L, u2p9));
        when(dynamicUserLimitService.calculate(eq(group), isNull(), anyLong())).thenReturn(1);
        when(workerService.newExecuteNo()).thenReturn("exec-301", "exec-401");
        when(concurrencyGuard.tryAcquire("g1", "u1", 301L, 5, 1, 60, "exec-301")).thenReturn(false);
        when(concurrencyGuard.userRunning("g1", "u1")).thenReturn(0L, 1L);
        when(concurrencyGuard.tryAcquire("g1", "u2", 401L, 5, 1, 60, "exec-401")).thenReturn(true);
        when(taskRepository.casToRunning(eq(401L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(queueRedisService).rebalanceActiveUser("g1", ROUTE, "u1");
        verify(workerService, never()).submit(u1p0, group, "exec-301");
        verify(workerService).submit(u2p9, group, "exec-401");
    }

    @Test
    void shouldPreferUserConcurrencyConfigForCurrentGroup() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask task = runnableTask(451L, "u1", 0);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserId("u1");
        userConfig.setGroupCode("g1");
        userConfig.setUserBaseConcurrency(1);
        userConfig.setLoadStrategyJson("{}");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(451L));
        when(taskRepository.findByIds(List.of(451L))).thenReturn(Map.of(451L, task));
        when(userConcurrencyConfigRepository.findByUserIdAndGroupCode("u1", "g1"))
                .thenReturn(Optional.of(userConfig));
        when(dynamicUserLimitService.calculate(eq(group), eq(userConfig), anyLong())).thenReturn(1);
        when(workerService.newExecuteNo()).thenReturn("exec-451");
        when(concurrencyGuard.tryAcquire("g1", "u1", 451L, 5, 1, 60, "exec-451")).thenReturn(true);
        when(taskRepository.casToRunning(eq(451L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(dynamicUserLimitService).calculate(group, userConfig, 0L);
        verify(concurrencyGuard).tryAcquire("g1", "u1", 451L, 5, 1, 60, "exec-451");
        verify(workerService).submit(task, group, "exec-451");
    }

    @Test
    void shouldStopBeforeAnotherRedisAcquireAfterLocalUserLimitIsReached() {
        GroupConfig group = group("g1", 5, 3, 60);
        SchedulerTask first = runnableTask(461L, "u1", 0);
        SchedulerTask second = runnableTask(462L, "u1", 0);
        SchedulerTask third = runnableTask(463L, "u1", 0);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(3))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 3))
                .thenReturn(List.of(461L, 462L, 463L));
        when(taskRepository.findByIds(List.of(461L, 462L, 463L)))
                .thenReturn(Map.of(461L, first, 462L, second, 463L, third));
        when(dynamicUserLimitService.calculate(eq(group), isNull(), anyLong())).thenReturn(2);
        when(workerService.newExecuteNo()).thenReturn("exec-461", "exec-462");
        when(concurrencyGuard.tryAcquire("g1", "u1", 461L, 5, 2, 60, "exec-461")).thenReturn(true);
        when(concurrencyGuard.tryAcquire("g1", "u1", 462L, 5, 2, 60, "exec-462")).thenReturn(true);
        when(taskRepository.casToRunning(eq(461L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);
        when(taskRepository.casToRunning(eq(462L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(concurrencyGuard).tryAcquire("g1", "u1", 461L, 5, 2, 60, "exec-461");
        verify(concurrencyGuard).tryAcquire("g1", "u1", 462L, 5, 2, 60, "exec-462");
        verify(concurrencyGuard, never()).tryAcquire("g1", "u1", 463L, 5, 2, 60, "exec-463");
        verify(workerService, never()).submit(third, group, "exec-463");
    }

    @Test
    void shouldStopCurrentTickAfterWorkerPoolReject() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask first = runnableTask(501L, "u1", 0);
        SchedulerTask second = runnableTask(601L, "u2", 1);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, 0);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(501L));
        when(taskRepository.findByIds(List.of(501L))).thenReturn(Map.of(501L, first));
        when(dynamicUserLimitService.calculate(eq(group), isNull(), anyLong())).thenReturn(2);
        when(workerService.newExecuteNo()).thenReturn("exec-501");
        when(concurrencyGuard.tryAcquire("g1", "u1", 501L, 5, 2, 60, "exec-501")).thenReturn(true);
        when(taskRepository.casToRunning(eq(501L), eq("g1"), eq(0), eq(null),
                eq(Thread.currentThread().getName()), any(LocalDateTime.class))).thenReturn(true);
        doThrow(new TaskRejectedException("worker pool full")).when(workerService).submit(first, group, "exec-501");
        when(concurrencyGuard.release("g1", "u1", 501L, "exec-501")).thenReturn(true);
        when(taskRepository.rescheduleToRunnable(eq(501L), any(LocalDateTime.class),
                eq("DISPATCH_SUBMIT_REJECTED"), any(), any(LocalDateTime.class))).thenReturn(true);

        dispatchService.dispatchOnce();

        verify(workerService).submit(first, group, "exec-501");
        verify(workerService, never()).submit(second, group, "exec-601");
        verify(queueRedisService).removeFromReadyQueue(first);
        verify(queueRedisService).enqueue(first);
    }

    @Test
    void shouldRequeueReadyTaskToTimeQueueWhenExecuteAtIsSlightlyInFuture() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask futureTask = runnableTask(701L, "u1", 0);
        futureTask.setExecuteAt(LocalDateTime.now().plusSeconds(2));

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(701L));
        when(taskRepository.findByIds(List.of(701L))).thenReturn(Map.of(701L, futureTask));
        dispatchService.dispatchOnce();

        verify(queueRedisService).removeFromReadyQueue(futureTask);
        verify(queueRedisService).enqueue(futureTask);
        verify(workerService, never()).submit(eq(futureTask), any(), any());
        assertTrue(futureTask.getExecuteAt().isAfter(LocalDateTime.now().minusSeconds(1)));
    }

    @Test
    void shouldResumeWaitHoldTaskWithoutReacquiringConcurrency() {
        GroupConfig group = group("g1", 1, 2, 60);
        SchedulerTask holdTask = runnableTask(801L, "u1", 0);
        holdTask.setStatus(TaskStatus.WAIT_HOLD);
        holdTask.setHoldRoundCount(1);
        holdTask.setHoldMaxRounds(5);
        holdTask.setHoldRetryDelaySec(3);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(1L, 1L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(801L));
        when(taskRepository.findByIds(List.of(801L))).thenReturn(Map.of(801L, holdTask));
        when(workerService.newExecuteNo()).thenReturn("exec-801");
        when(concurrencyGuard.acquireLease(801L, "exec-801", 60)).thenReturn(true);
        when(taskRepository.casWaitHoldToRunning(eq(801L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class)))
                .thenReturn(true);

        dispatchService.dispatchOnce();

        verify(concurrencyGuard, never()).tryAcquire(anyString(), anyString(), anyLong(), anyInt(), anyInt(), anyInt(), anyString());
        verify(concurrencyGuard).acquireLease(801L, "exec-801", 60);
        verify(dynamicUserLimitService, never()).calculate(any(GroupConfig.class), anyLong());
        verify(taskRepository).casWaitHoldToRunning(eq(801L), eq(null), eq(Thread.currentThread().getName()), any(LocalDateTime.class));
        verify(workerService).submit(holdTask, group, "exec-801");
    }

    @Test
    void shouldRemoveStaleReadyMemberBeforeAcquiringConcurrency() {
        GroupConfig group = group("g1", 5, 2, 60);
        SchedulerTask stale = runnableTask(901L, "u1", 0);
        stale.setGroupCode("g2");

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), eq(ROUTE), anyLong(), eq(2))).thenReturn(List.of());
        when(concurrencyGuard.groupRunning("g1")).thenReturn(0L, 0L);
        when(queueRedisService.peekNextActiveUser("g1", ROUTE)).thenReturn("u1", null);
        when(queueRedisService.tryAcquireActiveUserLock("g1", ROUTE, "u1", 5000L)).thenReturn("lock-u1");
        when(queueRedisService.peekReadyHeadPriority("g1", ROUTE, "u1")).thenReturn(0, null);
        when(queueRedisService.peekReadyTasksByPriority("g1", ROUTE, "u1", 0, 2)).thenReturn(List.of(901L));
        when(taskRepository.findByIds(List.of(901L))).thenReturn(Map.of(901L, stale));
        dispatchService.dispatchOnce();

        verify(queueRedisService).removeFromReadyQueue("g1", ROUTE, "u1", 901L);
        verify(concurrencyGuard, never()).tryAcquire(anyString(), anyString(), anyLong(),
                anyInt(), anyInt(), anyInt(), anyString());
        verify(workerService, never()).submit(any(), any(), anyString());
    }

    private GroupConfig group(String groupCode, int maxConcurrency, int dispatchBatchSize, int lockExpireSec) {
        GroupConfig group = new GroupConfig();
        group.setGroupCode(groupCode);
        group.setEnabled(true);
        group.setMaxConcurrency(maxConcurrency);
        group.setDispatchBatchSize(dispatchBatchSize);
        group.setLockExpireSec(lockExpireSec);
        return group;
    }

    private SchedulerTask runnableTask(long id, String userId, int priority) {
        SchedulerTask task = new SchedulerTask();
        task.setId(id);
        task.setTaskNo("task-" + id);
        task.setGroupCode("g1");
        task.setDispatchRoute(ROUTE);
        task.setUserId(userId);
        task.setBizType("biz");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setPriority(priority);
        task.setExecuteAt(LocalDateTime.now().minusSeconds(1));
        task.setCreateTime(LocalDateTime.now().minusSeconds(10));
        return task;
    }
}
