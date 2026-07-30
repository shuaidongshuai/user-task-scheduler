package org.dong.demo;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.RedisKeys;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.GroupFallbackScanner;
import org.dong.scheduler.core.service.RecoveryService;
import org.dong.scheduler.core.service.TaskStateService;
import org.dong.scheduler.core.service.TaskDependencyService;
import org.dong.scheduler.core.service.GroupFallbackService;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest(properties = {
        "utask.scheduler.dispatch-enabled=false",
        "utask.scheduler.dispatch-route=codex-fallback-it",
        "utask.scheduler.auto-init-default-group=false",
        "utask.scheduler.instance-id=codex-fallback-it",
        "utask.scheduler.worker-threads=2",
        "utask.scheduler.max-worker-threads=2",
        "utask.scheduler.fallback-callback-threads=4",
        "utask.scheduler.fallback-policy-timeout-ms=3000"
})
class GroupFallbackRealEnvTest {
    private static final String ROUTE = "codex-fallback-it";
    private static final String SOURCE_GROUP = "codex_fallback_source";
    private static final String TARGET_GROUP = "codex_fallback_target";
    private static final String USER = "codex-fallback-user";
    private static final String BIZ_TYPE = "codex.fallback.process";
    private static final String BIZ_PREFIX = "codex-fallback-it-";

    @Autowired
    private SchedulerClient schedulerClient;
    @Autowired
    private GroupFallbackScanner fallbackScanner;
    @Autowired
    private DispatchService dispatchService;
    @Autowired
    private RecoveryService recoveryService;
    @Autowired
    private TaskStateService taskStateService;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private SchedulerProperties schedulerProperties;
    @Autowired
    private GroupConfigRepository groupConfigRepository;
    @Autowired
    private TaskDependencyService taskDependencyService;
    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        cleanup();
        FallbackTestHandler.reset();
    }

    @AfterEach
    void tearDown() {
        FallbackTestHandler.releaseBarrier();
        cleanup();
        FallbackTestHandler.reset();
    }

    @Test
    void shouldRouteBetweenGroupsAndExecuteFromTargetQueue() throws Exception {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        long taskId = submitDueTask("route");
        long existingRunningId = submitTaskWithoutFallback("existing-running");
        jdbcTemplate.update(
                "update scheduler_task set status='RUNNING', version=version+1 where id=?", existingRunningId);
        redisTemplate.opsForZSet().remove(
                RedisKeys.userReadyQueue(SOURCE_GROUP, ROUTE, USER), String.valueOf(existingRunningId));
        redisTemplate.opsForValue().set(RedisKeys.groupRunning(SOURCE_GROUP), "1");
        redisTemplate.opsForValue().set(RedisKeys.userRunning(SOURCE_GROUP, USER), "1");

        assertTrue(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
        assertEquals(1, fallbackScanner.scanOnce());

        SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TARGET_GROUP, routed.getGroupCode());
        assertNull(routed.getFallbackCheckAt());
        assertEquals(1, routed.getFallbackPolicyCount());
        assertEquals(1, routed.getGroupFallbackCount());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
        assertEquals(1, fallbackLogCount(taskId));
        assertEquals("1", redisTemplate.opsForValue().get(RedisKeys.groupRunning(SOURCE_GROUP)));
        assertEquals("1", redisTemplate.opsForValue().get(RedisKeys.userRunning(SOURCE_GROUP, USER)));

        redisTemplate.opsForZSet().remove(
                RedisKeys.userReadyQueue(TARGET_GROUP, ROUTE, USER), String.valueOf(taskId));
        redisTemplate.delete(RedisKeys.activeUsers(TARGET_GROUP, ROUTE));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
        assertTrue(recoveryService.refillQueue() > 0);
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));

        redisTemplate.opsForValue().set(RedisKeys.groupRunning(TARGET_GROUP), "2");
        dispatchService.dispatchOnce();
        assertEquals(TaskStatus.RUNNABLE, taskRepository.findById(taskId).orElseThrow().getStatus());
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));

        redisTemplate.delete(RedisKeys.groupRunning(TARGET_GROUP));
        dispatchService.dispatchOnce();
        waitForStatus(taskId, TaskStatus.SUCCESS, 10_000L);
        assertEquals(1, FallbackTestHandler.executeCount.get());
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    @Test
    void shouldAllowOnlyOneRouteCasAcrossConcurrentScanners() throws Exception {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        FallbackTestHandler.barrier.set(new CyclicBarrier(2));
        long taskId = submitDueTask("concurrent");
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> first = callers.submit(fallbackScanner::scanOnce);
            Future<Integer> second = callers.submit(fallbackScanner::scanOnce);
            int changed = first.get(10, TimeUnit.SECONDS) + second.get(10, TimeUnit.SECONDS);

            SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
            assertEquals(1, changed);
            assertEquals(2, FallbackTestHandler.callbackCount.get());
            assertEquals(TARGET_GROUP, routed.getGroupCode());
            assertEquals(1, routed.getFallbackPolicyCount());
            assertEquals(1, routed.getGroupFallbackCount());
            assertEquals(1, fallbackLogCount(taskId));
            assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
            assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void shouldPersistKeepCurrentAndStopCheckingDecisions() {
        long taskId = submitDueTask("keep-stop");
        LocalDateTime nextCheckAt = LocalDateTime.now().plusSeconds(5);
        FallbackTestHandler.decision.set(GroupFallbackDecision.keepCurrent(nextCheckAt));

        assertEquals(1, fallbackScanner.scanOnce());
        SchedulerTask kept = taskRepository.findById(taskId).orElseThrow();
        assertEquals(SOURCE_GROUP, kept.getGroupCode());
        assertNotNull(kept.getFallbackCheckAt());
        assertEquals(1, kept.getFallbackPolicyCount());
        assertEquals(0, kept.getGroupFallbackCount());
        assertEquals(0, fallbackLogCount(taskId));
        assertTrue(existsInReadyQueue(SOURCE_GROUP, taskId));

        jdbcTemplate.update(
                "update scheduler_task set fallback_check_at=?, version=version+1 where id=?",
                LocalDateTime.now().minusSeconds(1),
                taskId
        );
        FallbackTestHandler.decision.set(GroupFallbackDecision.stopChecking());
        assertEquals(1, fallbackScanner.scanOnce());

        SchedulerTask stopped = taskRepository.findById(taskId).orElseThrow();
        assertNull(stopped.getFallbackCheckAt());
        assertEquals(2, stopped.getFallbackPolicyCount());
        assertEquals(0, stopped.getGroupFallbackCount());
        assertTrue(existsInReadyQueue(SOURCE_GROUP, taskId));
    }

    @Test
    void shouldFailTaskAndReleaseFailedDependency() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.fail("TEST_FALLBACK_FAIL", "fallback requested"));
        long upstreamId = submitDueTask("fail-upstream");
        long downstreamId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "fail-downstream-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setDependencies(List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.FAILED))));
        long impossibleDownstreamId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "fail-impossible-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setDependencies(List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.SUCCESS))));

        assertEquals(TaskStatus.PENDING, taskRepository.findById(downstreamId).orElseThrow().getStatus());
        assertEquals(1, fallbackScanner.scanOnce());

        SchedulerTask upstream = taskRepository.findById(upstreamId).orElseThrow();
        SchedulerTask downstream = taskRepository.findById(downstreamId).orElseThrow();
        SchedulerTask impossibleDownstream = taskRepository.findById(impossibleDownstreamId).orElseThrow();
        assertEquals(TaskStatus.FAILED, upstream.getStatus());
        assertEquals("TEST_FALLBACK_FAIL", upstream.getErrorCode());
        assertNull(upstream.getFallbackCheckAt());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, upstreamId));
        assertEquals(TaskStatus.RUNNABLE, downstream.getStatus());
        assertTrue(existsInReadyQueue(SOURCE_GROUP, downstreamId));
        assertEquals(TaskStatus.FAILED, impossibleDownstream.getStatus());
        assertEquals("DEPENDENCY_NOT_SATISFIED", impossibleDownstream.getErrorCode());
    }

    @Test
    void shouldPreferExpiredWaitDeadlineWithoutCallingHandler() {
        long taskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "wait-timeout-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(10))
                .setMaxWaitSec(5)
                .setFallbackCheckAt(LocalDateTime.now().minusSeconds(8))
                .setMaxRetryCount(0));
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));

        assertEquals(1, fallbackScanner.scanOnce());

        SchedulerTask failed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.FAILED, failed.getStatus());
        assertEquals("SCHEDULE_WAIT_TIMEOUT", failed.getErrorCode());
        assertEquals(0, FallbackTestHandler.callbackCount.get());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    @Test
    void shouldRouteWaitRetryWithoutChangingRetryCount() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        long taskId = submitDueTask("wait-retry");
        jdbcTemplate.update(
                "update scheduler_task set status='WAIT_RETRY', retry_count=2, version=version+1 where id=?",
                taskId
        );

        assertEquals(1, fallbackScanner.scanOnce());

        SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.WAIT_RETRY, routed.getStatus());
        assertEquals(2, routed.getRetryCount());
        assertEquals(TARGET_GROUP, routed.getGroupCode());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    @Test
    void shouldKeepPendingAfterRouteAndRecoverTargetTimeQueueByRefill() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        long taskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "pending-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().plusMinutes(5))
                .setFallbackCheckAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0));

        assertEquals(1, fallbackScanner.scanOnce());
        SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.PENDING, routed.getStatus());
        assertEquals(TARGET_GROUP, routed.getGroupCode());
        assertFalse(existsInTimeQueue(SOURCE_GROUP, taskId));
        assertFalse(existsInTimeQueue(TARGET_GROUP, taskId));

        assertTrue(recoveryService.refillQueue() > 0);
        assertTrue(existsInTimeQueue(TARGET_GROUP, taskId));
    }

    @Test
    void shouldExcludeRunningWaitHoldAndTerminalTasksFromFallbackScan() {
        long runningId = submitDueTask("excluded-running");
        long waitHoldId = submitDueTask("excluded-wait-hold");
        long successId = submitDueTask("excluded-success");
        jdbcTemplate.update("update scheduler_task set status='RUNNING', version=version+1 where id=?", runningId);
        jdbcTemplate.update("update scheduler_task set status='WAIT_HOLD', version=version+1 where id=?", waitHoldId);
        jdbcTemplate.update("update scheduler_task set status='SUCCESS', version=version+1 where id=?", successId);
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));

        assertEquals(0, fallbackScanner.scanOnce());
        assertEquals(0, FallbackTestHandler.callbackCount.get());
        assertEquals(SOURCE_GROUP, taskRepository.findById(runningId).orElseThrow().getGroupCode());
        assertEquals(SOURCE_GROUP, taskRepository.findById(waitHoldId).orElseThrow().getGroupCode());
        assertEquals(SOURCE_GROUP, taskRepository.findById(successId).orElseThrow().getGroupCode());
    }

    @Test
    void shouldNotFallbackAfterDispatcherMovesTaskToRunning() throws Exception {
        long taskId = submitDueTask("dispatcher-race");
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        FallbackTestHandler.callbackStarted.set(new CountDownLatch(1));
        FallbackTestHandler.callbackRelease.set(new CountDownLatch(1));
        FallbackTestHandler.executeStarted.set(new CountDownLatch(1));
        FallbackTestHandler.executeRelease.set(new CountDownLatch(1));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> scan = caller.submit(fallbackScanner::scanOnce);
            assertTrue(FallbackTestHandler.callbackStarted.get().await(5, TimeUnit.SECONDS));

            dispatchService.dispatchOnce();
            assertTrue(FallbackTestHandler.executeStarted.get().await(5, TimeUnit.SECONDS));
            assertEquals(TaskStatus.RUNNING, taskRepository.findById(taskId).orElseThrow().getStatus());

            FallbackTestHandler.callbackRelease.get().countDown();
            assertEquals(0, scan.get(10, TimeUnit.SECONDS));
            SchedulerTask running = taskRepository.findById(taskId).orElseThrow();
            assertEquals(TaskStatus.RUNNING, running.getStatus());
            assertEquals(SOURCE_GROUP, running.getGroupCode());
            assertEquals(0, fallbackLogCount(taskId));

            FallbackTestHandler.executeRelease.get().countDown();
            waitForStatus(taskId, TaskStatus.SUCCESS, 10_000L);
        } finally {
            FallbackTestHandler.releaseAll();
            caller.shutdownNow();
        }
    }

    @Test
    void shouldCleanLatestTargetQueueWhenWaitDeadlineExpiresAfterRoute() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        long taskId = submitDueTask("route-then-timeout");
        assertEquals(1, fallbackScanner.scanOnce());
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
        jdbcTemplate.update(
                "update scheduler_task set wait_deadline_at=?, version=version+1 where id=?",
                LocalDateTime.now().minusSeconds(1),
                taskId
        );

        assertTrue(taskStateService.markFailedByWaitDeadline(taskId, LocalDateTime.now()));

        SchedulerTask failed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.FAILED, failed.getStatus());
        assertEquals(TARGET_GROUP, failed.getGroupCode());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    @Test
    void shouldFailSafelyForMissingThrowingAndNullHandlers() {
        long missingHandlerId = submitDueTask("missing-handler", "codex.fallback.missing");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(missingHandlerId, GroupFallbackService.HANDLER_NOT_FOUND, 0);

        FallbackTestHandler.callbackFailure.set(new IllegalStateException("policy exploded"));
        long throwingHandlerId = submitDueTask("throwing-handler");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(throwingHandlerId, GroupFallbackService.POLICY_EXCEPTION, 1);

        FallbackTestHandler.callbackFailure.set(null);
        FallbackTestHandler.decision.set(null);
        long nullDecisionId = submitDueTask("null-decision");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(nullDecisionId, GroupFallbackService.DECISION_INVALID, 1);
    }

    @Test
    void shouldRejectInvalidDisabledAndPrematurePolicyDecisions() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(SOURCE_GROUP, null));
        long sameGroupId = submitDueTask("same-group");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(sameGroupId, GroupFallbackService.TARGET_INVALID, 1);

        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo("codex_fallback_disabled", null));
        long disabledGroupId = submitDueTask("disabled-group");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(disabledGroupId, GroupFallbackService.TARGET_DISABLED, 1);

        FallbackTestHandler.decision.set(
                GroupFallbackDecision.keepCurrent(LocalDateTime.now().plusNanos(100_000_000L)));
        long prematureCheckId = submitDueTask("premature-check");
        assertEquals(1, fallbackScanner.scanOnce());
        assertFailure(prematureCheckId, GroupFallbackService.DECISION_INVALID, 1);
    }

    @Test
    void shouldKeepCancellationWhenItWinsAgainstFallbackCas() throws Exception {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        FallbackTestHandler.callbackStarted.set(new CountDownLatch(1));
        FallbackTestHandler.callbackRelease.set(new CountDownLatch(1));
        long taskId = submitDueTask("cancel-race");
        String taskNo = taskRepository.findById(taskId).orElseThrow().getTaskNo();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> scan = caller.submit(fallbackScanner::scanOnce);
            assertTrue(FallbackTestHandler.callbackStarted.get().await(5, TimeUnit.SECONDS));
            assertTrue(schedulerClient.cancel(taskNo));
            FallbackTestHandler.callbackRelease.get().countDown();

            assertEquals(0, scan.get(10, TimeUnit.SECONDS));
            SchedulerTask cancelled = taskRepository.findById(taskId).orElseThrow();
            assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());
            assertEquals(SOURCE_GROUP, cancelled.getGroupCode());
            assertEquals(0, fallbackLogCount(taskId));
            assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
        } finally {
            FallbackTestHandler.releaseAll();
            caller.shutdownNow();
        }
    }

    @Test
    void shouldRecoverAfterRealRedisConnectionFailureFollowingCommittedRoute() {
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));
        long taskId = submitDueTask("redis-failure");
        SchedulerTask snapshot = taskRepository.findById(taskId).orElseThrow();
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(200))
                .shutdownTimeout(Duration.ZERO)
                .build();
        RedisStandaloneConfiguration unavailableRedis = new RedisStandaloneConfiguration("127.0.0.1", 1);
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
                unavailableRedis, clientConfiguration);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        try {
            StringRedisTemplate failingTemplate = new StringRedisTemplate(connectionFactory);
            failingTemplate.afterPropertiesSet();
            QueueRedisService failingQueue = new QueueRedisService(failingTemplate, schedulerProperties);
            GroupFallbackService serviceWithUnavailableRedis = new GroupFallbackService(
                    schedulerProperties, taskRepository, groupConfigRepository,
                    taskDependencyService, failingQueue, transactionTemplate);

            GroupFallbackService.FallbackApplyResult result = assertDoesNotThrow(
                    () -> serviceWithUnavailableRedis.applyWaitingDecision(
                            snapshot, GroupFallbackDecision.routeTo(TARGET_GROUP, null), LocalDateTime.now()));
            assertTrue(result.changed());
        } finally {
            connectionFactory.destroy();
        }

        SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TARGET_GROUP, routed.getGroupCode());
        assertEquals(1, fallbackLogCount(taskId));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
        assertTrue(existsInReadyQueue(SOURCE_GROUP, taskId));

        assertTrue(recoveryService.refillQueue() > 0);
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
        redisTemplate.opsForValue().set(RedisKeys.groupRunning(TARGET_GROUP), "2");
        dispatchService.dispatchOnce();
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertTrue(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    @RepeatedTest(3)
    void shouldAllowOnlyOneFallbackAcrossIndependentJvmProcesses() throws Exception {
        long taskId = submitDueTask("cross-jvm");
        Path barrierDirectory = Files.createTempDirectory("fallback-cross-jvm-");
        Process first = null;
        Process second = null;
        try {
            first = startFallbackProcess(taskId, barrierDirectory, "one");
            second = startFallbackProcess(taskId, barrierDirectory, "two");
            awaitFiles(barrierDirectory.resolve("ready-one"), barrierDirectory.resolve("ready-two"));
            Files.createFile(barrierDirectory.resolve("go"));

            String firstOutput = waitForProcess(first);
            String secondOutput = waitForProcess(second);
            int appliedProcesses = countApplied(firstOutput) + countApplied(secondOutput);
            assertEquals(1, appliedProcesses, firstOutput + System.lineSeparator() + secondOutput);

            SchedulerTask routed = taskRepository.findById(taskId).orElseThrow();
            assertEquals(TARGET_GROUP, routed.getGroupCode());
            assertEquals(1, routed.getFallbackPolicyCount());
            assertEquals(1, routed.getGroupFallbackCount());
            assertEquals(1, fallbackLogCount(taskId));
        } finally {
            destroyProcess(first);
            destroyProcess(second);
            deleteIfExists(barrierDirectory.resolve("go"));
            deleteIfExists(barrierDirectory.resolve("ready-one"));
            deleteIfExists(barrierDirectory.resolve("ready-two"));
            deleteIfExists(barrierDirectory);
        }
    }

    @Test
    void shouldFailTimedOutCallbacksAndDeferOverflowWhenAllRealExecutorSlotsAreStuck() throws Exception {
        FallbackTestHandler.stuckCallbacksStarted.set(new CountDownLatch(4));
        FallbackTestHandler.stuckCallbacksRelease.set(new CountDownLatch(1));
        FallbackTestHandler.ignoreCallbackInterrupts.set(true);
        List<Long> taskIds = List.of(
                submitDueTask("executor-stuck-1"),
                submitDueTask("executor-stuck-2"),
                submitDueTask("executor-stuck-3"),
                submitDueTask("executor-stuck-4"),
                submitDueTask("executor-overflow")
        );
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> scan = caller.submit(fallbackScanner::scanOnce);
            assertTrue(FallbackTestHandler.stuckCallbacksStarted.get().await(5, TimeUnit.SECONDS));
            assertEquals(4, scan.get(10, TimeUnit.SECONDS));

            for (Long taskId : taskIds.subList(0, 4)) {
                assertFailure(taskId, GroupFallbackService.POLICY_TIMEOUT, 1);
            }
            SchedulerTask deferred = taskRepository.findById(taskIds.get(4)).orElseThrow();
            assertEquals(TaskStatus.RUNNABLE, deferred.getStatus());
            assertEquals(0, deferred.getFallbackPolicyCount());
            assertNotNull(deferred.getFallbackCheckAt());
            assertTrue(deferred.getFallbackCheckAt().isAfter(LocalDateTime.now()));
            assertEquals(4, FallbackTestHandler.callbackCount.get());
        } finally {
            FallbackTestHandler.releaseAll();
            caller.shutdownNow();
        }
    }

    @Test
    void shouldReleaseDependencyIntoLatestGroupAfterPendingTaskRoutes() {
        long upstreamId = submitTaskWithoutFallback("dependency-upstream");
        jdbcTemplate.update(
                "update scheduler_task set status='RUNNING', version=version+1 where id=?", upstreamId);
        redisTemplate.opsForZSet().remove(
                RedisKeys.userReadyQueue(SOURCE_GROUP, ROUTE, USER), String.valueOf(upstreamId));
        long dependentId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "dependency-target-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setFallbackCheckAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setDependencies(List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.SUCCESS))));
        assertEquals(TaskStatus.PENDING, taskRepository.findById(dependentId).orElseThrow().getStatus());
        FallbackTestHandler.decision.set(GroupFallbackDecision.routeTo(TARGET_GROUP, null));

        assertEquals(1, fallbackScanner.scanOnce());
        SchedulerTask routed = taskRepository.findById(dependentId).orElseThrow();
        assertEquals(TaskStatus.PENDING, routed.getStatus());
        assertEquals(TARGET_GROUP, routed.getGroupCode());

        assertTrue(taskStateService.markSuccess(upstreamId, LocalDateTime.now()));
        SchedulerTask released = taskRepository.findById(dependentId).orElseThrow();
        assertEquals(TaskStatus.RUNNABLE, released.getStatus());
        assertEquals(TARGET_GROUP, released.getGroupCode());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, dependentId));
        assertTrue(existsInReadyQueue(TARGET_GROUP, dependentId));
    }

    @Test
    void shouldRemoveFutureTimeQueueMemberWhenFallbackFailsBeforeExecuteAt() {
        long taskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + "future-fail-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().plusMinutes(5))
                .setFallbackCheckAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0));
        redisTemplate.opsForZSet().add(
                RedisKeys.timeQueue(SOURCE_GROUP, ROUTE), String.valueOf(taskId), System.currentTimeMillis());
        assertTrue(existsInTimeQueue(SOURCE_GROUP, taskId));
        FallbackTestHandler.decision.set(GroupFallbackDecision.fail("EARLY_FAIL", "failed before executeAt"));

        assertEquals(1, fallbackScanner.scanOnce());

        assertFailure(taskId, "EARLY_FAIL", 1);
        assertFalse(existsInTimeQueue(SOURCE_GROUP, taskId));
    }

    private long submitDueTask(String suffix) {
        return submitDueTask(suffix, BIZ_TYPE);
    }

    private long submitDueTask(String suffix, String bizType) {
        return schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(bizType)
                .setBizKey(BIZ_PREFIX + suffix + "-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setFallbackCheckAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0));
    }

    private long submitTaskWithoutFallback(String suffix) {
        return schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(SOURCE_GROUP)
                .setDispatchRoute(ROUTE)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(BIZ_PREFIX + suffix + "-" + UUID.randomUUID().toString().replace("-", ""))
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0));
    }

    private void assertFailure(long taskId, String errorCode, int policyCount) {
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.FAILED, task.getStatus());
        assertEquals(errorCode, task.getErrorCode());
        assertEquals(policyCount, task.getFallbackPolicyCount());
        assertNull(task.getFallbackCheckAt());
        assertFalse(existsInReadyQueue(SOURCE_GROUP, taskId));
        assertFalse(existsInReadyQueue(TARGET_GROUP, taskId));
    }

    private Process startFallbackProcess(long taskId, Path barrierDirectory, String instance) throws IOException {
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        return new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classPath,
                GroupFallbackProcessWorker.class.getName(),
                String.valueOf(taskId), barrierDirectory.toString(), instance
        ).redirectErrorStream(true).start();
    }

    private void awaitFiles(Path first, Path second) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(first) && Files.exists(second)) {
                return;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("independent JVM callbacks did not reach the barrier");
    }

    private String waitForProcess(Process process) throws Exception {
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "fallback JVM did not exit");
        String output = new String(process.getInputStream().readAllBytes());
        assertEquals(0, process.exitValue(), output);
        return output;
    }

    private int countApplied(String output) {
        return output.contains("FALLBACK_CHANGED=1") ? 1 : 0;
    }

    private void destroyProcess(Process process) {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
        }
    }

    private void deleteIfExists(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    private boolean existsInReadyQueue(String groupCode, long taskId) {
        Double score = redisTemplate.opsForZSet().score(
                RedisKeys.userReadyQueue(groupCode, ROUTE, USER), String.valueOf(taskId));
        return score != null;
    }

    private boolean existsInTimeQueue(String groupCode, long taskId) {
        Double score = redisTemplate.opsForZSet().score(
                RedisKeys.timeQueue(groupCode, ROUTE), String.valueOf(taskId));
        return score != null;
    }

    private int fallbackLogCount(long taskId) {
        Integer count = jdbcTemplate.queryForObject(
                "select count(1) from scheduler_task_group_fallback_log where task_id=?",
                Integer.class,
                taskId
        );
        assertNotNull(count);
        return count;
    }

    private void waitForStatus(long taskId, TaskStatus expected, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == expected) {
                return;
            }
            Thread.sleep(50L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task status not reached, expected=" + expected + ", actual=" + task.getStatus());
    }

    private void cleanup() {
        List<Long> taskIds = jdbcTemplate.query(
                "select id from scheduler_task where biz_key like ?",
                (rs, rowNum) -> rs.getLong(1),
                BIZ_PREFIX + "%"
        );
        for (Long taskId : taskIds) {
            redisTemplate.delete(RedisKeys.taskLease(taskId));
        }
        if (!taskIds.isEmpty()) {
            String ids = taskIds.stream().map(String::valueOf).reduce((left, right) -> left + "," + right).orElseThrow();
            jdbcTemplate.update("delete from scheduler_task_group_fallback_log where task_id in (" + ids + ")");
            jdbcTemplate.update("delete from scheduler_task_execution where task_id in (" + ids + ")");
            jdbcTemplate.update("delete from scheduler_task_dependency where task_id in (" + ids
                    + ") or depends_on_task_id in (" + ids + ")");
            jdbcTemplate.update("delete from scheduler_task where id in (" + ids + ")");
        }
        for (String group : List.of(SOURCE_GROUP, TARGET_GROUP)) {
            redisTemplate.delete(RedisKeys.timeQueue(group, ROUTE));
            redisTemplate.delete(RedisKeys.readyQueue(group, ROUTE));
            redisTemplate.delete(RedisKeys.userReadyQueue(group, ROUTE, USER));
            redisTemplate.delete(RedisKeys.activeUsers(group, ROUTE));
            redisTemplate.delete(RedisKeys.activeUserLock(group, ROUTE, USER));
            redisTemplate.delete(RedisKeys.groupRunning(group));
            redisTemplate.delete(RedisKeys.userRunning(group, USER));
            redisTemplate.delete(RedisKeys.groupReconcileThrottle(group));
        }
        redisTemplate.delete(RedisKeys.jobLock("refill-queue:" + ROUTE));
    }

    @TestConfiguration
    static class FallbackRealEnvTestConfiguration {
        @Bean
        @Primary
        GroupConfigRepository fallbackGroupConfigRepository() {
            return new GroupConfigRepository() {
                @Override
                public List<GroupConfig> listEnabled() {
                    return List.of(group(SOURCE_GROUP), group(TARGET_GROUP));
                }

                @Override
                public Optional<GroupConfig> findEnabledByGroupCode(String groupCode) {
                    return listEnabled().stream().filter(group -> group.getGroupCode().equals(groupCode)).findFirst();
                }

                private GroupConfig group(String groupCode) {
                    GroupConfig group = new GroupConfig();
                    group.setGroupCode(groupCode);
                    group.setEnabled(true);
                    group.setMaxConcurrency(2);
                    group.setUserBaseConcurrency(2);
                    group.setDispatchBatchSize(20);
                    group.setHeartbeatTimeoutSec(30);
                    group.setLockExpireSec(30);
                    return group;
                }
            };
        }

        @Bean
        TaskHandler fallbackRealEnvTaskHandler() {
            return new FallbackTestHandler();
        }
    }

    static class FallbackTestHandler implements TaskHandler {
        private static final AtomicReference<GroupFallbackDecision> decision = new AtomicReference<>();
        private static final AtomicReference<CyclicBarrier> barrier = new AtomicReference<>();
        private static final AtomicInteger callbackCount = new AtomicInteger();
        private static final AtomicInteger executeCount = new AtomicInteger();
        private static final AtomicReference<CountDownLatch> callbackStarted = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> callbackRelease = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> executeStarted = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> executeRelease = new AtomicReference<>();
        private static final AtomicReference<RuntimeException> callbackFailure = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> stuckCallbacksStarted = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> stuckCallbacksRelease = new AtomicReference<>();
        private static final AtomicReference<Boolean> ignoreCallbackInterrupts = new AtomicReference<>(false);

        @Override
        public List<String> bizTypes() {
            return List.of(BIZ_TYPE);
        }

        @Override
        public TaskExecuteResult execute(SchedulerTask task) {
            executeCount.incrementAndGet();
            awaitIfConfigured(executeStarted.get(), executeRelease.get(), "execute");
            return TaskExecuteResult.success();
        }

        @Override
        public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
            callbackCount.incrementAndGet();
            RuntimeException failure = callbackFailure.get();
            if (failure != null) {
                throw failure;
            }
            awaitStuckCallbackIfConfigured();
            CyclicBarrier currentBarrier = barrier.get();
            if (currentBarrier != null) {
                try {
                    currentBarrier.await(5, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    throw new IllegalStateException("fallback callback barrier failed", ex);
                }
            }
            awaitIfConfigured(callbackStarted.get(), callbackRelease.get(), "callback");
            return decision.get();
        }

        private static void awaitIfConfigured(CountDownLatch started, CountDownLatch release, String phase) {
            if (started == null || release == null) {
                return;
            }
            started.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(phase + " release timed out");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(phase + " interrupted", ex);
            }
        }

        private static void awaitStuckCallbackIfConfigured() {
            CountDownLatch started = stuckCallbacksStarted.get();
            CountDownLatch release = stuckCallbacksRelease.get();
            if (started == null || release == null) {
                return;
            }
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    if (!Boolean.TRUE.equals(ignoreCallbackInterrupts.get())) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("stuck callback interrupted", ex);
                    }
                }
            }
        }

        private static void releaseBarrier() {
            CyclicBarrier current = barrier.getAndSet(null);
            if (current != null) {
                current.reset();
            }
        }

        private static void reset() {
            releaseAll();
            decision.set(GroupFallbackDecision.stopChecking());
            callbackFailure.set(null);
            ignoreCallbackInterrupts.set(false);
            callbackCount.set(0);
            executeCount.set(0);
        }

        private static void releaseAll() {
            releaseBarrier();
            countDown(callbackRelease.getAndSet(null));
            countDown(executeRelease.getAndSet(null));
            countDown(stuckCallbacksRelease.getAndSet(null));
            stuckCallbacksStarted.set(null);
            ignoreCallbackInterrupts.set(false);
            callbackStarted.set(null);
            executeStarted.set(null);
        }

        private static void countDown(CountDownLatch latch) {
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
