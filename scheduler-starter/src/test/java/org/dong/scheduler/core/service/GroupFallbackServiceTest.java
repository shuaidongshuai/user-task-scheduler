package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.GroupFallbackAction;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GroupFallbackServiceTest {
    @Mock
    private TaskRepository taskRepository;
    @Mock
    private GroupConfigRepository groupConfigRepository;
    @Mock
    private TaskDependencyService taskDependencyService;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TransactionTemplate transactionTemplate;

    private GroupFallbackService service;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setFallbackMinNextCheckDelayMs(1000);
        properties.setFallbackExecutorRejectBackoffMs(5000);
        service = new GroupFallbackService(properties, taskRepository, groupConfigRepository,
                taskDependencyService, queueRedisService, transactionTemplate);
        lenient().when(transactionTemplate.execute(any(TransactionCallback.class))).thenAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        });
    }

    @Test
    void shouldRouteTaskAndMoveQueueAfterTransactionalCas() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        SchedulerTask target = waitingTask(now);
        target.setGroupCode("g2");
        target.setVersion(source.getVersion() + 1);
        target.setFallbackCheckAt(now.plusSeconds(10));
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g2");
        group.setEnabled(true);

        when(groupConfigRepository.findEnabledByGroupCode("g2")).thenReturn(Optional.of(group));
        when(taskRepository.casRouteFallback(source, "g2", now.plusSeconds(10), now)).thenReturn(true);
        when(taskRepository.findById(source.getId())).thenReturn(Optional.of(target));

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(
                source, GroupFallbackDecision.routeTo(" g2 ", now.plusSeconds(10)), now);

        assertTrue(result.changed());
        assertEquals(GroupFallbackAction.ROUTE, result.action());
        verify(taskRepository).insertGroupFallbackLog(source, "g2", now.plusSeconds(10), 1);
        verify(queueRedisService).removeQueueReferences(source);
        verify(queueRedisService).enqueueReady(target);
    }

    @Test
    void shouldKeepCommittedRouteWhenRedisCleanupAndEnqueueFail() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        SchedulerTask target = waitingTask(now);
        target.setGroupCode("g2");
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g2");
        group.setEnabled(true);
        when(groupConfigRepository.findEnabledByGroupCode("g2")).thenReturn(Optional.of(group));
        when(taskRepository.casRouteFallback(source, "g2", null, now)).thenReturn(true);
        when(taskRepository.findById(source.getId())).thenReturn(Optional.of(target));
        doThrow(new IllegalStateException("redis cleanup failed"))
                .when(queueRedisService).removeQueueReferences(source);
        doThrow(new IllegalStateException("redis enqueue failed"))
                .when(queueRedisService).enqueueReady(target);

        GroupFallbackService.FallbackApplyResult result = assertDoesNotThrow(
                () -> service.applyWaitingDecision(source, GroupFallbackDecision.routeTo("g2", null), now));

        assertTrue(result.changed());
        verify(taskRepository).insertGroupFallbackLog(source, "g2", null, 1);
    }

    @Test
    void shouldEnqueuePendingTaskInTargetTimeQueueWhenItIsNotDependencyBlocked() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        source.setStatus(TaskStatus.PENDING);
        source.setExecuteAt(now.plusMinutes(1));
        SchedulerTask target = waitingTask(now);
        target.setStatus(TaskStatus.PENDING);
        target.setGroupCode("g2");
        target.setExecuteAt(now.plusMinutes(1));
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g2");
        group.setEnabled(true);
        when(groupConfigRepository.findEnabledByGroupCode("g2")).thenReturn(Optional.of(group));
        when(taskRepository.casRouteFallback(source, "g2", null, now)).thenReturn(true);
        when(taskRepository.findById(source.getId())).thenReturn(Optional.of(target));
        when(taskDependencyService.hasUnsatisfiedDependencies(target.getId())).thenReturn(false);

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(
                source, GroupFallbackDecision.routeTo("g2", null), now);

        assertTrue(result.changed());
        verify(queueRedisService).removeQueueReferences(source);
        verify(queueRedisService).enqueue(target);
        verify(queueRedisService, never()).enqueueReady(target);
    }

    @Test
    void shouldNotEnqueuePendingTaskWithUnsatisfiedDependencies() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        source.setStatus(TaskStatus.PENDING);
        SchedulerTask target = waitingTask(now);
        target.setStatus(TaskStatus.PENDING);
        target.setGroupCode("g2");
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g2");
        group.setEnabled(true);
        when(groupConfigRepository.findEnabledByGroupCode("g2")).thenReturn(Optional.of(group));
        when(taskRepository.casRouteFallback(source, "g2", null, now)).thenReturn(true);
        when(taskRepository.findById(source.getId())).thenReturn(Optional.of(target));
        when(taskDependencyService.hasUnsatisfiedDependencies(target.getId())).thenReturn(true);

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(
                source, GroupFallbackDecision.routeTo("g2", null), now);

        assertTrue(result.changed());
        verify(queueRedisService).removeQueueReferences(source);
        verify(queueRedisService, never()).enqueue(target);
        verify(queueRedisService, never()).enqueueReady(target);
    }

    @Test
    void shouldFailAndPropagateDependenciesForInvalidDecision() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        SchedulerTask downstream = waitingTask(now);
        downstream.setId(2L);
        when(taskRepository.casFallbackWaitingToFailed(
                eq(source), eq(GroupFallbackService.DECISION_INVALID), any(), eq(now), eq(true)))
                .thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(1L, TaskStatus.FAILED, now))
                .thenReturn(List.of(downstream));

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(
                source, GroupFallbackDecision.keepCurrent(now), now);

        assertTrue(result.changed());
        verify(queueRedisService).removeQueueReferences(source);
        verify(queueRedisService).enqueueReady(downstream);
        verify(taskRepository, never()).casUpdateFallbackCheck(eq(source), any(), eq(now), eq(true));
    }

    @Test
    void shouldClearCheckWithoutIncreasingPolicyCountWhenRejectBackoffCannotBeatDeadline() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        source.setWaitDeadlineAt(now.plusSeconds(2));
        when(taskRepository.casUpdateFallbackCheck(source, null, now, false)).thenReturn(true);

        GroupFallbackService.FallbackApplyResult result = service.deferAfterExecutorReject(source, now);

        assertTrue(result.changed());
        assertTrue(result.deferred());
        verify(taskRepository).casUpdateFallbackCheck(source, null, now, false);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidDecisions")
    void shouldFailEveryInvalidDecisionCombination(String scenario,
                                                   GroupFallbackDecision decision,
                                                   String expectedErrorCode) {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        when(taskRepository.casFallbackWaitingToFailed(
                eq(source), eq(expectedErrorCode), any(), eq(now), eq(true)))
                .thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(1L, TaskStatus.FAILED, now))
                .thenReturn(List.of());

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(source, decision, now);

        assertTrue(result.changed(), scenario);
        assertEquals(GroupFallbackAction.FAIL, result.action());
        verify(taskRepository).casFallbackWaitingToFailed(
                eq(source), eq(expectedErrorCode), any(), eq(now), eq(true));
        verify(taskRepository, never()).casRouteFallback(any(), any(), any(), any());
        verify(taskRepository, never()).casUpdateFallbackCheck(any(), any(), any(), eq(true));
    }

    @Test
    void shouldRejectMissingOrDisabledTargetGroup() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        when(groupConfigRepository.findEnabledByGroupCode("missing")).thenReturn(Optional.empty());
        when(taskRepository.casFallbackWaitingToFailed(
                eq(source), eq(GroupFallbackService.TARGET_DISABLED), any(), eq(now), eq(true)))
                .thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(1L, TaskStatus.FAILED, now))
                .thenReturn(List.of());

        GroupFallbackService.FallbackApplyResult result = service.applyWaitingDecision(
                source, GroupFallbackDecision.routeTo("missing", null), now);

        assertTrue(result.changed());
        verify(taskRepository).casFallbackWaitingToFailed(
                eq(source), eq(GroupFallbackService.TARGET_DISABLED), any(), eq(now), eq(true));
    }

    @Test
    void shouldTruncateExplicitFailureMessageToDatabaseLimit() {
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask source = waitingTask(now);
        String longMessage = "x".repeat(1500);
        when(taskRepository.casFallbackWaitingToFailed(
                eq(source), eq("BUSINESS_FAIL"), any(), eq(now), eq(true)))
                .thenReturn(true);
        when(taskDependencyService.onUpstreamTaskTerminal(1L, TaskStatus.FAILED, now))
                .thenReturn(List.of());

        service.applyWaitingDecision(
                source, GroupFallbackDecision.fail(" BUSINESS_FAIL ", longMessage), now);

        verify(taskRepository).casFallbackWaitingToFailed(
                eq(source), eq("BUSINESS_FAIL"), eq("x".repeat(1024)), eq(now), eq(true));
    }

    private static Stream<Arguments> invalidDecisions() {
        LocalDateTime soon = LocalDateTime.now().plusNanos(100_000_000L);
        return Stream.of(
                Arguments.of("null decision", null, GroupFallbackService.DECISION_INVALID),
                Arguments.of("null action", new GroupFallbackDecision(null, null, null, null, null),
                        GroupFallbackService.DECISION_INVALID),
                Arguments.of("next check too early", GroupFallbackDecision.keepCurrent(soon),
                        GroupFallbackService.DECISION_INVALID),
                Arguments.of("blank route target", GroupFallbackDecision.routeTo(" ", null),
                        GroupFallbackService.TARGET_INVALID),
                Arguments.of("same route target", GroupFallbackDecision.routeTo("g1", null),
                        GroupFallbackService.TARGET_INVALID),
                Arguments.of("oversized route target", GroupFallbackDecision.routeTo("g".repeat(65), null),
                        GroupFallbackService.TARGET_INVALID),
                Arguments.of("fail without code", GroupFallbackDecision.fail(" ", "message"),
                        GroupFallbackService.DECISION_INVALID),
                Arguments.of("oversized fail code", GroupFallbackDecision.fail("e".repeat(65), "message"),
                        GroupFallbackService.DECISION_INVALID)
        );
    }

    private SchedulerTask waitingTask(LocalDateTime now) {
        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setTaskNo("task-1");
        task.setGroupCode("g1");
        task.setDispatchRoute("route-a");
        task.setUserId("u1");
        task.setBizType("biz");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(now.minusSeconds(1));
        task.setCreateTime(now.minusMinutes(1));
        task.setFallbackCheckAt(now.minusSeconds(1));
        task.setVersion(4);
        return task;
    }
}
