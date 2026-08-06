package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupFallbackAction;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GroupFallbackScannerTest {
    @Mock
    private TaskRepository taskRepository;
    @Mock
    private TaskStateService taskStateService;
    @Mock
    private GroupFallbackService fallbackService;

    private ThreadPoolExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldInvokeHandlerInDedicatedExecutorAndApplyDecision() {
        SchedulerProperties properties = properties();
        SchedulerTask task = dueTask();
        TaskHandler handler = handler(GroupFallbackDecision.stopChecking());
        executor = executor();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.applyWaitingDecision(eq(task), eq(GroupFallbackDecision.stopChecking()),
                any(LocalDateTime.class)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.STOP_CHECKING));

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(handler)), taskStateService, fallbackService, executor);

        assertEquals(1, scanner.scanOnce());
        verify(fallbackService).applyWaitingDecision(
                eq(task), eq(GroupFallbackDecision.stopChecking()), any(LocalDateTime.class));
    }

    @Test
    void shouldDeferWithoutFailingWhenExecutorRejectsBeforeHandlerStarts() {
        SchedulerProperties properties = properties();
        SchedulerTask task = dueTask();
        executor = executor();
        executor.shutdownNow();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.deferAfterExecutorReject(eq(task), any(LocalDateTime.class)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.deferredResult());

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(handler(GroupFallbackDecision.stopChecking()))),
                taskStateService, fallbackService, executor);

        assertEquals(0, scanner.scanOnce());
        verify(fallbackService).deferAfterExecutorReject(eq(task), any(LocalDateTime.class));
    }

    @Test
    void shouldCancelTimedOutCallbackAndFailWithSnapshotCas() {
        SchedulerProperties properties = properties();
        properties.setFallbackPolicyTimeoutMs(20);
        SchedulerTask task = dueTask();
        TaskHandler handler = new TaskHandler() {
            @Override
            public List<String> bizTypes() {
                return List.of("biz");
            }

            @Override
            public TaskExecuteResult execute(SchedulerTask schedulerTask) {
                return TaskExecuteResult.success();
            }

            @Override
            public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask schedulerTask) {
                try {
                    Thread.sleep(10_000L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return GroupFallbackDecision.stopChecking();
            }
        };
        executor = executor();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.failWaiting(eq(task), eq(GroupFallbackService.POLICY_TIMEOUT),
                any(), any(LocalDateTime.class), eq(true)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.FAIL));

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(handler)), taskStateService, fallbackService, executor);

        assertEquals(1, scanner.scanOnce());
        verify(fallbackService).failWaiting(eq(task), eq(GroupFallbackService.POLICY_TIMEOUT),
                any(), any(LocalDateTime.class), eq(true));
    }

    @Test
    void shouldFailWithoutPolicyCountWhenHandlerIsMissing() {
        SchedulerProperties properties = properties();
        SchedulerTask task = dueTask();
        executor = executor();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.failWaiting(eq(task), eq(GroupFallbackService.HANDLER_NOT_FOUND),
                any(), any(LocalDateTime.class), eq(false)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.FAIL));

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of()), taskStateService, fallbackService, executor);

        assertEquals(1, scanner.scanOnce());
        verify(fallbackService).failWaiting(eq(task), eq(GroupFallbackService.HANDLER_NOT_FOUND),
                any(), any(LocalDateTime.class), eq(false));
    }

    @Test
    void shouldRetryFallbackCheckAfterTenTimesMinimumDelayWhenHandlerThrows() {
        SchedulerProperties properties = properties();
        SchedulerTask task = dueTask();
        executor = executor();
        TaskHandler throwingHandler = handler(taskSnapshot -> {
            throw new IllegalStateException("policy exploded");
        });
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.applyWaitingDecision(eq(task), any(GroupFallbackDecision.class),
                any(LocalDateTime.class)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.KEEP_CURRENT));

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(throwingHandler)), taskStateService, fallbackService, executor);

        assertEquals(1, scanner.scanOnce());
        verify(fallbackService).applyWaitingDecision(eq(task), org.mockito.ArgumentMatchers.argThat(decision ->
                        decision.action() == GroupFallbackAction.KEEP_CURRENT
                                && decision.nextFallbackCheckAt() != null
                                && decision.nextFallbackCheckAt().isAfter(LocalDateTime.now().plusSeconds(9))
                                && decision.nextFallbackCheckAt().isBefore(LocalDateTime.now().plusSeconds(11))),
                any(LocalDateTime.class));
        verify(fallbackService, never()).failWaiting(eq(task), eq(GroupFallbackService.POLICY_EXCEPTION),
                any(), any(LocalDateTime.class), eq(true));
    }

    @Test
    void shouldPassNullDecisionToValidationFailure() {
        SchedulerProperties properties = properties();
        SchedulerTask task = dueTask();
        executor = executor();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(task));
        when(fallbackService.applyWaitingDecision(eq(task), eq(null), any(LocalDateTime.class)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.FAIL));

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(handler((GroupFallbackDecision) null))),
                taskStateService, fallbackService, executor);

        assertEquals(1, scanner.scanOnce());
        verify(fallbackService).applyWaitingDecision(eq(task), eq(null), any(LocalDateTime.class));
    }

    @Test
    void shouldDeferFollowingTasksWhenTimedOutHandlerIgnoresInterruptAndSaturatesExecutor() {
        SchedulerProperties properties = properties();
        properties.setFallbackPolicyTimeoutMs(30);
        SchedulerTask first = dueTask();
        SchedulerTask second = dueTask();
        second.setId(2L);
        second.setTaskNo("task-2");
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean firstStarted = new AtomicBoolean();
        TaskHandler blockingHandler = handler(task -> {
            if (task.getId() == 1L) {
                firstStarted.set(true);
                while (release.getCount() > 0) {
                    try {
                        release.await(10, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                        // Intentionally ignore cancellation to simulate a stuck policy callback.
                    }
                }
            }
            return GroupFallbackDecision.stopChecking();
        });
        executor = executor();
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L, 2L));
        when(taskRepository.findById(1L)).thenReturn(Optional.of(first));
        when(taskRepository.findById(2L)).thenReturn(Optional.of(second));
        when(fallbackService.failWaiting(eq(first), eq(GroupFallbackService.POLICY_TIMEOUT),
                any(), any(LocalDateTime.class), eq(true)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.applied(GroupFallbackAction.FAIL));
        when(fallbackService.deferAfterExecutorReject(eq(second), any(LocalDateTime.class)))
                .thenReturn(GroupFallbackService.FallbackApplyResult.deferredResult());

        try {
            GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                    new TaskHandlerRegistry(List.of(blockingHandler)), taskStateService, fallbackService, executor);

            assertEquals(1, scanner.scanOnce());
            assertEquals(true, firstStarted.get());
            verify(fallbackService).failWaiting(eq(first), eq(GroupFallbackService.POLICY_TIMEOUT),
                    any(), any(LocalDateTime.class), eq(true));
            verify(fallbackService).deferAfterExecutorReject(eq(second), any(LocalDateTime.class));
            verify(fallbackService, never()).applyWaitingDecision(eq(second), any(), any());
        } finally {
            release.countDown();
        }
    }

    @Test
    void shouldNeverRunMoreCallbacksThanConfiguredSlots() {
        SchedulerProperties properties = properties();
        properties.setFallbackCallbackThreads(2);
        properties.setFallbackPolicyTimeoutMs(2000);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        TaskHandler measuringHandler = handler(task -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            try {
                Thread.sleep(40L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                active.decrementAndGet();
            }
            return GroupFallbackDecision.stopChecking();
        });
        executor = executor(2);
        List<SchedulerTask> tasks = List.of(dueTask(1L), dueTask(2L), dueTask(3L), dueTask(4L));
        when(taskRepository.findFallbackDueTaskIds(eq("route-a"), any(LocalDateTime.class), eq(10)))
                .thenReturn(List.of(1L, 2L, 3L, 4L));
        for (SchedulerTask task : tasks) {
            when(taskRepository.findById(task.getId())).thenReturn(Optional.of(task));
            when(fallbackService.applyWaitingDecision(eq(task), any(), any()))
                    .thenReturn(GroupFallbackService.FallbackApplyResult.applied(
                            GroupFallbackAction.STOP_CHECKING));
        }

        GroupFallbackScanner scanner = new GroupFallbackScanner(properties, taskRepository,
                new TaskHandlerRegistry(List.of(measuringHandler)), taskStateService, fallbackService, executor);

        assertEquals(4, scanner.scanOnce());
        assertEquals(2, maximum.get());
    }

    private SchedulerProperties properties() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDispatchRoute("route-a");
        properties.setFallbackScanLimit(10);
        properties.setFallbackCallbackThreads(1);
        properties.setFallbackPolicyTimeoutMs(1000);
        return properties;
    }

    private SchedulerTask dueTask() {
        return dueTask(1L);
    }

    private SchedulerTask dueTask(long id) {
        SchedulerTask task = new SchedulerTask();
        task.setId(id);
        task.setTaskNo("task-" + id);
        task.setGroupCode("g1");
        task.setDispatchRoute("route-a");
        task.setUserId("u1");
        task.setBizType("biz");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(LocalDateTime.now().plusMinutes(1));
        task.setFallbackCheckAt(LocalDateTime.now().minusSeconds(1));
        return task;
    }

    private TaskHandler handler(GroupFallbackDecision decision) {
        return handler(task -> decision);
    }

    private TaskHandler handler(java.util.function.Function<SchedulerTask, GroupFallbackDecision> callback) {
        return new TaskHandler() {
            @Override
            public List<String> bizTypes() {
                return List.of("biz");
            }

            @Override
            public TaskExecuteResult execute(SchedulerTask task) {
                return TaskExecuteResult.success();
            }

            @Override
            public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
                return callback.apply(task);
            }
        };
    }

    private ThreadPoolExecutor executor() {
        return executor(1);
    }

    private ThreadPoolExecutor executor(int threads) {
        ThreadPoolExecutor result = new ThreadPoolExecutor(
                threads, threads, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        return result;
    }
}
