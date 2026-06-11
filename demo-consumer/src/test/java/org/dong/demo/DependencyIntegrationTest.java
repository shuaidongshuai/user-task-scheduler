package org.dong.demo;

import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.model.batch.BatchSubmitTaskRequest;
import org.dong.scheduler.core.redis.RedisKeys;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.RecoveryService;
import org.dong.scheduler.core.service.TaskStateService;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class DependencyIntegrationTest {
    private static final String TEST_GROUP = "it-dependency-group";
    private static final String TEST_USER = "it-user";
    private static final String BIZ_TYPE = "demo.biz.process";
    private static final String BIZ_PREFIX = "it-dep-";

    @Autowired
    private SchedulerClient schedulerClient;
    @Autowired
    private TaskStateService taskStateService;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private RecoveryService recoveryService;
    @Autowired
    private DispatchService dispatchService;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        ensureGroupConfig();
        cleanupTestData();
        clearRedis();
    }

    @Test
    void shouldPromoteDependentTaskAfterUpstreamSuccessAndRefillWhenQueueEntryMissing() {
        long upstreamId = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        String downstreamBizKey = nextBizKey();
        long downstreamId = submitTask(
                downstreamBizKey,
                LocalDateTime.now().minusSeconds(1),
                List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.SUCCESS))
        );

        SchedulerTask downstreamBefore = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.PENDING, downstreamBefore.getStatus());
        assertFalse(existsInTimeQueue(TEST_GROUP, downstreamId));
        assertFalse(existsInReadyQueue(TEST_GROUP, downstreamId));

        boolean changed = taskStateService.markTerminalByBusinessState(upstreamId, TaskStatus.SUCCESS, LocalDateTime.now());
        assertTrue(changed);

        SchedulerTask downstreamAfter = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.RUNNABLE, downstreamAfter.getStatus());
        assertTrue(existsInReadyQueue(TEST_GROUP, downstreamId));

        redisTemplate.opsForZSet().remove(RedisKeys.readyQueue(TEST_GROUP), String.valueOf(downstreamId));
        assertFalse(existsInReadyQueue(TEST_GROUP, downstreamId));

        int refilled = recoveryService.refillQueue();
        assertTrue(refilled > 0);
        assertTrue(existsInReadyQueue(TEST_GROUP, downstreamId));

        Optional<SchedulerTask> found = taskRepository.findById(downstreamId);
        assertTrue(found.isPresent());
        assertEquals(TaskStatus.RUNNABLE, found.get().getStatus());
    }

    @Test
    void shouldFailDependentTaskImmediatelyWhenExpectedSuccessButUpstreamFails() {
        long upstreamId = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        long downstreamId = submitTask(
                nextBizKey(),
                LocalDateTime.now().minusSeconds(1),
                List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.SUCCESS))
        );

        boolean changed = taskStateService.markTerminalByBusinessState(upstreamId, TaskStatus.FAILED, LocalDateTime.now());
        assertTrue(changed);

        SchedulerTask downstream = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.FAILED, downstream.getStatus());
        assertEquals("DEPENDENCY_NOT_SATISFIED", downstream.getErrorCode());
        assertEquals("dependency task status not satisfied: taskId=" + upstreamId, downstream.getErrorMsg());

        Integer impossibleCount = jdbcTemplate.queryForObject(
                """
                select count(1)
                  from scheduler_task_dependency
                 where task_id = ?
                   and status = 'IMPOSSIBLE'
                """,
                Integer.class,
                downstreamId
        );
        assertNotNull(impossibleCount);
        assertEquals(1, impossibleCount.intValue());
        assertFalse(existsInTimeQueue(TEST_GROUP, downstreamId));
        assertFalse(existsInReadyQueue(TEST_GROUP, downstreamId));
    }

    @Test
    void shouldSupportMultiLayerDependencyPropagation() {
        long taskA = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        long taskB = submitTask(
                nextBizKey(),
                LocalDateTime.now().minusSeconds(1),
                List.of(new TaskDependencyRequest(taskA, DependencyTargetState.SUCCESS))
        );
        long taskC = submitTask(
                nextBizKey(),
                LocalDateTime.now().minusSeconds(1),
                List.of(new TaskDependencyRequest(taskB, DependencyTargetState.TERMINAL))
        );

        assertEquals(TaskStatus.PENDING, taskRepository.findById(taskB).orElseThrow().getStatus());
        assertEquals(TaskStatus.PENDING, taskRepository.findById(taskC).orElseThrow().getStatus());

        assertTrue(taskStateService.markTerminalByBusinessState(taskA, TaskStatus.SUCCESS, LocalDateTime.now()));
        assertEquals(TaskStatus.RUNNABLE, taskRepository.findById(taskB).orElseThrow().getStatus());
        assertEquals(TaskStatus.PENDING, taskRepository.findById(taskC).orElseThrow().getStatus());

        assertTrue(taskStateService.markTerminalByBusinessState(taskB, TaskStatus.SUCCESS, LocalDateTime.now()));
        assertEquals(TaskStatus.RUNNABLE, taskRepository.findById(taskC).orElseThrow().getStatus());
        assertTrue(existsInReadyQueue(TEST_GROUP, taskC));
    }

    @Test
    void shouldWaitUntilAllDependenciesReachTheirOwnTargetStates() {
        long upstreamSuccess = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        long upstreamFailed = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        long downstreamId = submitTask(
                nextBizKey(),
                LocalDateTime.now().minusSeconds(1),
                List.of(
                        new TaskDependencyRequest(upstreamSuccess, DependencyTargetState.SUCCESS),
                        new TaskDependencyRequest(upstreamFailed, DependencyTargetState.FAILED)
                )
        );

        assertTrue(taskStateService.markTerminalByBusinessState(upstreamSuccess, TaskStatus.SUCCESS, LocalDateTime.now()));
        SchedulerTask downstreamAfterFirst = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.PENDING, downstreamAfterFirst.getStatus());
        assertFalse(existsInTimeQueue(TEST_GROUP, downstreamId));
        assertFalse(existsInReadyQueue(TEST_GROUP, downstreamId));

        assertTrue(taskStateService.markTerminalByBusinessState(upstreamFailed, TaskStatus.FAILED, LocalDateTime.now()));
        SchedulerTask downstreamAfterSecond = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.RUNNABLE, downstreamAfterSecond.getStatus());
        assertTrue(existsInReadyQueue(TEST_GROUP, downstreamId));
    }

    @Test
    void shouldDispatchAndExecuteDependentTaskAfterDependenciesSatisfied() throws Exception {
        long upstreamId = submitTask(nextBizKey(), LocalDateTime.now().minusSeconds(1), List.of());
        String downstreamBizKey = nextBizKey();
        long downstreamId = submitTask(
                downstreamBizKey,
                LocalDateTime.now().minusSeconds(1),
                List.of(new TaskDependencyRequest(upstreamId, DependencyTargetState.SUCCESS)),
                2,
                0
        );

        assertTrue(taskStateService.markTerminalByBusinessState(upstreamId, TaskStatus.SUCCESS, LocalDateTime.now()));
        waitForTaskStatus(downstreamId, TaskStatus.RUNNABLE, 5_000);

        driveSchedulingUntil(downstreamId, TaskStatus.SUCCESS, 20_000);

        SchedulerTask downstream = taskRepository.findById(downstreamId).orElseThrow();
        assertEquals(TaskStatus.SUCCESS, downstream.getStatus());

        String bizStatus = jdbcTemplate.queryForObject(
                "select status from demo_biz_task where biz_key = ?",
                String.class,
                downstreamBizKey
        );
        assertEquals("SUCCESS", bizStatus);
    }

    @Test
    void shouldSubmitBatchWithInBatchDependencies() {
        LocalDateTime now = LocalDateTime.now().minusSeconds(1);
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setGroupCode(TEST_GROUP)
                        .setUserId(TEST_USER)
                        .setBizType(BIZ_TYPE)
                        .setBizKey(nextBizKey())
                        .setPriority(50)
                        .setExecuteAt(now)
                        .setMaxRetryCount(0)
                        .setExecuteTimeoutSec(5)
                        .setDependencies(List.of()),
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("B")
                        .setGroupCode(TEST_GROUP)
                        .setUserId(TEST_USER)
                        .setBizType(BIZ_TYPE)
                        .setBizKey(nextBizKey())
                        .setPriority(40)
                        .setExecuteAt(now.plusMinutes(1))
                        .setMaxRetryCount(0)
                        .setExecuteTimeoutSec(5)
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)))
        ));

        List<BatchSubmitResultItem> items = schedulerClient.submitBatch(request);

        assertEquals(2, items.size());
        long taskAId = items.stream().filter(item -> "A".equals(item.getClientTaskRef())).findFirst().orElseThrow().getTaskId();
        long taskBId = items.stream().filter(item -> "B".equals(item.getClientTaskRef())).findFirst().orElseThrow().getTaskId();

        SchedulerTask taskA = taskRepository.findById(taskAId).orElseThrow();
        SchedulerTask taskB = taskRepository.findById(taskBId).orElseThrow();
        assertEquals(TaskStatus.RUNNABLE, taskA.getStatus());
        assertEquals(TaskStatus.PENDING, taskB.getStatus());

        Long dependsOnTaskId = jdbcTemplate.queryForObject(
                """
                select depends_on_task_id
                  from scheduler_task_dependency
                 where task_id = ?
                """,
                Long.class,
                taskBId
        );
        assertEquals(taskAId, dependsOnTaskId);
        assertTrue(existsInReadyQueue(TEST_GROUP, taskAId));
        assertFalse(existsInTimeQueue(TEST_GROUP, taskBId));
        assertFalse(existsInReadyQueue(TEST_GROUP, taskBId));
    }

    @Test
    void shouldRollbackWholeBatchWhenDependencyInsertFails() {
        LocalDateTime now = LocalDateTime.now();
        String upstreamBizKey = nextBizKey();
        String downstreamBizKey = nextBizKey();
        TaskStateService.BatchSubmitCommand upstream = new TaskStateService.BatchSubmitCommand(
                "A",
                "task-a-" + UUID.randomUUID().toString().replace("-", ""),
                new TaskSubmitRequest()
                        .setGroupCode(TEST_GROUP)
                        .setUserId(TEST_USER)
                        .setBizType(BIZ_TYPE)
                        .setBizKey(upstreamBizKey)
                        .setPriority(50)
                        .setExecuteAt(now)
                        .setMaxRetryCount(0)
                        .setExecuteTimeoutSec(5)
                        .setDependencies(List.of()),
                List.of()
        );
        TaskStateService.BatchSubmitCommand downstream = new TaskStateService.BatchSubmitCommand(
                "B",
                "task-b-" + UUID.randomUUID().toString().replace("-", ""),
                new TaskSubmitRequest()
                        .setGroupCode(TEST_GROUP)
                        .setUserId(TEST_USER)
                        .setBizType(BIZ_TYPE)
                        .setBizKey(downstreamBizKey)
                        .setPriority(40)
                        .setExecuteAt(now)
                        .setMaxRetryCount(0)
                        .setExecuteTimeoutSec(5)
                        .setDependencies(List.of()),
                List.of(
                        new BatchSubmitDependencyRequest(999999L, null, DependencyTargetState.SUCCESS),
                        new BatchSubmitDependencyRequest(999999L, null, DependencyTargetState.SUCCESS)
                )
        );

        assertThrows(Exception.class, () -> taskStateService.submitBatch(List.of(upstream, downstream)));

        Integer taskCount = jdbcTemplate.queryForObject(
                """
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key in (?, ?)
                """,
                Integer.class,
                TEST_GROUP,
                upstreamBizKey,
                downstreamBizKey
        );
        Integer dependencyCount = jdbcTemplate.queryForObject(
                """
                select count(1)
                  from scheduler_task_dependency d
                 where exists (
                     select 1
                       from scheduler_task t
                      where t.id = d.task_id
                        and t.group_code = ?
                        and t.biz_key in (?, ?)
                 )
                """,
                Integer.class,
                TEST_GROUP,
                upstreamBizKey,
                downstreamBizKey
        );
        assertNotNull(taskCount);
        assertNotNull(dependencyCount);
        assertEquals(0, taskCount.intValue());
        assertEquals(0, dependencyCount.intValue());
    }

    private long submitTask(String bizKey, LocalDateTime executeAt, List<TaskDependencyRequest> dependencies) {
        return submitTask(bizKey, executeAt, dependencies, 0, null);
    }

    private long submitTask(String bizKey,
                            LocalDateTime executeAt,
                            List<TaskDependencyRequest> dependencies,
                            int maxRetryCount,
                            Integer retryDelaySec) {
        jdbcTemplate.update(
                """
                insert into demo_biz_task(biz_key, status, payload, create_time, update_time)
                values (?, 'SUBMIT', '{}', now(), now())
                on duplicate key update status = values(status), payload = values(payload), update_time = now()
                """,
                bizKey
        );
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setGroupCode(TEST_GROUP)
                .setUserId(TEST_USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(bizKey)
                .setPriority(50)
                .setExecuteAt(executeAt)
                .setMaxRetryCount(maxRetryCount)
                .setExecuteTimeoutSec(5)
                .setRetryDelaySec(retryDelaySec)
                .setDependencies(dependencies);
        return schedulerClient.submit(request);
    }

    private String nextBizKey() {
        return BIZ_PREFIX + UUID.randomUUID().toString().replace("-", "");
    }

    private void ensureGroupConfig() {
        jdbcTemplate.update(
                """
                insert into scheduler_group_config(
                    group_code, enabled, max_concurrency, user_base_concurrency,
                    dynamic_user_limit_enabled, load_strategy_json,
                    dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec, description
                ) values (?, 1, 20, 2, 0, null, 100, 30, 60, ?)
                on duplicate key update
                    enabled = values(enabled),
                    max_concurrency = values(max_concurrency),
                    user_base_concurrency = values(user_base_concurrency),
                    dynamic_user_limit_enabled = values(dynamic_user_limit_enabled),
                    load_strategy_json = values(load_strategy_json),
                    dispatch_batch_size = values(dispatch_batch_size),
                    heartbeat_timeout_sec = values(heartbeat_timeout_sec),
                    lock_expire_sec = values(lock_expire_sec),
                    description = values(description)
                """,
                TEST_GROUP,
                "integration test group"
        );
    }

    private void cleanupTestData() {
        List<Long> taskIds = jdbcTemplate.query(
                """
                select id
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                """,
                (rs, rowNum) -> rs.getLong(1),
                TEST_GROUP,
                BIZ_PREFIX + "%"
        );
        if (!taskIds.isEmpty()) {
            String ids = taskIds.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("");
            jdbcTemplate.update("delete from scheduler_task_dependency where task_id in (" + ids + ") or depends_on_task_id in (" + ids + ")");
            jdbcTemplate.update("delete from scheduler_task_execution where task_id in (" + ids + ")");
            jdbcTemplate.update("delete from scheduler_task where id in (" + ids + ")");
        }
        jdbcTemplate.update("delete from demo_biz_task where biz_key like ?", BIZ_PREFIX + "%");
    }

    private void clearRedis() {
        redisTemplate.delete(RedisKeys.timeQueue(TEST_GROUP));
        redisTemplate.delete(RedisKeys.readyQueue(TEST_GROUP));
        redisTemplate.delete(RedisKeys.groupRunning(TEST_GROUP));
        redisTemplate.delete(redisTemplate.keys(RedisKeys.userRunningPattern(TEST_GROUP)));
        redisTemplate.delete(RedisKeys.groupReconcileThrottle(TEST_GROUP));
    }

    private void driveSchedulingUntil(long taskId, TaskStatus expectedStatus, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            recoveryService.refillQueue();
            dispatchService.dispatchOnce();
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == expectedStatus) {
                return;
            }
            Thread.sleep(300L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task status not reached, taskId=" + taskId + ", expected=" + expectedStatus + ", actual=" + task.getStatus());
    }

    private void waitForTaskStatus(long taskId, TaskStatus expectedStatus, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == expectedStatus) {
                return;
            }
            Thread.sleep(100L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task status not reached, taskId=" + taskId + ", expected=" + expectedStatus + ", actual=" + task.getStatus());
    }

    private boolean existsInTimeQueue(String groupCode, long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.timeQueue(groupCode), String.valueOf(taskId)) != null;
    }

    private boolean existsInReadyQueue(String groupCode, long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.readyQueue(groupCode), String.valueOf(taskId)) != null;
    }
}
