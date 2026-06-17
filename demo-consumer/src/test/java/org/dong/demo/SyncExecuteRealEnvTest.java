package org.dong.demo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.RedisKeys;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "utask.scheduler.dispatch-enabled=false",
        "utask.scheduler.instance-id=codex-sync-it",
        "utask.scheduler.worker-threads=2",
        "utask.scheduler.max-worker-threads=2",
        "utask.scheduler.heartbeat-interval-sec=1",
        "utask.scheduler.default-retry-delay-sec=1",
        "utask.scheduler.default-execute-timeout-sec=20"
})
class SyncExecuteRealEnvTest {
    private static final String BIZ_TYPE = "codex.sync.process";
    private static final String GROUP_LIMIT_GROUP = "codex_sync_group_limit";
    private static final String USER_LIMIT_GROUP = "codex_sync_user_limit";
    private static final String PREFIX = "codex-sync-";

    @Autowired
    private SchedulerClient schedulerClient;
    @Autowired
    private DispatchService dispatchService;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        cleanupTasks();
        clearRedis();
        SyncTestTaskHandler.clear();
    }

    @AfterEach
    void tearDown() {
        SyncTestTaskHandler.releaseAll();
        cleanupTasks();
        clearRedis();
        SyncTestTaskHandler.clear();
    }

    @Test
    void shouldExecuteSyncTaskInlineAndLeaveNoQueueResidue() {
        String bizKey = nextBizKey("inline");
        String callerThread = Thread.currentThread().getName();

        long taskId = schedulerClient.executeSync(new TaskSubmitRequest()
                .setGroupCode(USER_LIMIT_GROUP)
                .setUserId("sync-inline-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(bizKey)
                .setPriority(60)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setExecuteTimeoutSec(10));

        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.SUCCESS, task.getStatus());
        assertEquals(callerThread, task.getWorkerThread());
        assertFalse(existsInReadyQueue(USER_LIMIT_GROUP, taskId));
        assertFalse(existsInTimeQueue(USER_LIMIT_GROUP, taskId));
        assertEquals(1, executionCount(taskId));
        assertEquals(0L, groupRunning(USER_LIMIT_GROUP));
        assertEquals(0L, userRunning(USER_LIMIT_GROUP, "sync-inline-user"));
    }

    @Test
    void shouldShareGroupConcurrencyWithAsyncTasks() throws Exception {
        String asyncBizKey = nextBizKey("group-async");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SyncTestTaskHandler.registerHold(asyncBizKey, started, release);

        long asyncTaskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(GROUP_LIMIT_GROUP)
                .setUserId("async-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(asyncBizKey)
                .setPriority(50)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setExecuteTimeoutSec(20));

        dispatchUntilRunning(asyncTaskId, started);
        assertEquals(1L, groupRunning(GROUP_LIMIT_GROUP));

        String syncBizKey = nextBizKey("group-sync");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> schedulerClient.executeSync(new TaskSubmitRequest()
                .setGroupCode(GROUP_LIMIT_GROUP)
                .setUserId("sync-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(syncBizKey)
                .setPriority(60)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setExecuteTimeoutSec(10)));

        assertEquals("sync task is throttled by concurrency limit", ex.getMessage());
        assertEquals(0, taskCountByBizKey(syncBizKey));

        release.countDown();
        waitForTaskStatus(asyncTaskId, TaskStatus.SUCCESS, 15_000L);
        assertEquals(0L, groupRunning(GROUP_LIMIT_GROUP));
    }

    @Test
    void shouldShareUserConcurrencyWithAsyncTasks() throws Exception {
        String asyncBizKey = nextBizKey("user-async");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SyncTestTaskHandler.registerHold(asyncBizKey, started, release);

        long asyncTaskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(USER_LIMIT_GROUP)
                .setUserId("shared-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(asyncBizKey)
                .setPriority(50)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setExecuteTimeoutSec(20));

        dispatchUntilRunning(asyncTaskId, started);
        assertEquals(1L, groupRunning(USER_LIMIT_GROUP));
        assertEquals(1L, userRunning(USER_LIMIT_GROUP, "shared-user"));

        String throttledBizKey = nextBizKey("user-sync-same");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> schedulerClient.executeSync(new TaskSubmitRequest()
                .setGroupCode(USER_LIMIT_GROUP)
                .setUserId("shared-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(throttledBizKey)
                .setPriority(60)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setExecuteTimeoutSec(10)));

        assertEquals("sync task is throttled by concurrency limit", ex.getMessage());
        assertEquals(0, taskCountByBizKey(throttledBizKey));

        String allowedBizKey = nextBizKey("user-sync-other");
        long syncTaskId = schedulerClient.executeSync(new TaskSubmitRequest()
                .setGroupCode(USER_LIMIT_GROUP)
                .setUserId("other-user")
                .setBizType(BIZ_TYPE)
                .setBizKey(allowedBizKey)
                .setPriority(60)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setExecuteTimeoutSec(10));

        SchedulerTask syncTask = taskRepository.findById(syncTaskId).orElseThrow();
        assertEquals(TaskStatus.SUCCESS, syncTask.getStatus());
        assertEquals(1, executionCount(syncTaskId));
        assertEquals(1L, userRunning(USER_LIMIT_GROUP, "shared-user"));
        assertEquals(0L, userRunning(USER_LIMIT_GROUP, "other-user"));

        release.countDown();
        waitForTaskStatus(asyncTaskId, TaskStatus.SUCCESS, 15_000L);
        assertEquals(0L, groupRunning(USER_LIMIT_GROUP));
    }

    private void dispatchUntilRunning(long taskId, CountDownLatch started) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            dispatchService.dispatchOnce();
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == TaskStatus.RUNNING && started.await(100, TimeUnit.MILLISECONDS)) {
                return;
            }
            Thread.sleep(100L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task did not enter RUNNING in time, taskId=" + taskId + ", status=" + task.getStatus());
    }

    private void waitForTaskStatus(long taskId, TaskStatus status, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == status) {
                return;
            }
            Thread.sleep(100L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task status not reached, taskId=" + taskId + ", expected=" + status + ", actual=" + task.getStatus());
    }

    private int executionCount(long taskId) {
        Integer count = jdbcTemplate.queryForObject(
                "select count(1) from scheduler_task_execution where task_id = ?",
                Integer.class,
                taskId
        );
        assertNotNull(count);
        return count;
    }

    private int taskCountByBizKey(String bizKey) {
        Integer count = jdbcTemplate.queryForObject(
                "select count(1) from scheduler_task where biz_key = ?",
                Integer.class,
                bizKey
        );
        assertNotNull(count);
        return count;
    }

    private long groupRunning(String groupCode) {
        String value = redisTemplate.opsForValue().get(RedisKeys.groupRunning(groupCode));
        return value == null ? 0L : Long.parseLong(value);
    }

    private long userRunning(String groupCode, String userId) {
        String value = redisTemplate.opsForValue().get(RedisKeys.userRunning(groupCode, userId));
        return value == null ? 0L : Long.parseLong(value);
    }

    private boolean existsInTimeQueue(String groupCode, long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.timeQueue(groupCode), String.valueOf(taskId)) != null;
    }

    private boolean existsInReadyQueue(String groupCode, long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.readyQueue(groupCode), String.valueOf(taskId)) != null;
    }

    private void cleanupTasks() {
        List<Long> taskIds = jdbcTemplate.query(
                """
                select id
                  from scheduler_task
                 where biz_key like ?
                """,
                (rs, rowNum) -> rs.getLong(1),
                PREFIX + "%"
        );
        if (taskIds.isEmpty()) {
            return;
        }
        String ids = taskIds.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("");
        jdbcTemplate.update("delete from scheduler_task_execution where task_id in (" + ids + ")");
        jdbcTemplate.update("delete from scheduler_task_dependency where task_id in (" + ids + ") or depends_on_task_id in (" + ids + ")");
        jdbcTemplate.update("delete from scheduler_task where id in (" + ids + ")");
    }

    private void clearRedis() {
        for (String groupCode : List.of(GROUP_LIMIT_GROUP, USER_LIMIT_GROUP)) {
            redisTemplate.delete(RedisKeys.timeQueue(groupCode));
            redisTemplate.delete(RedisKeys.readyQueue(groupCode));
            redisTemplate.delete(RedisKeys.groupRunning(groupCode));
            redisTemplate.delete(redisTemplate.keys(RedisKeys.userRunningPattern(groupCode)));
            redisTemplate.delete(RedisKeys.groupReconcileThrottle(groupCode));
        }
    }

    private String nextBizKey(String suffix) {
        return PREFIX + suffix + "-" + UUID.randomUUID().toString().replace("-", "");
    }

    @TestConfiguration
    static class SyncExecuteTestConfig {
        @Bean
        @Primary
        GroupConfigRepository syncGroupConfigRepository() {
            return new GroupConfigRepository() {
                @Override
                public List<GroupConfig> listEnabled() {
                    GroupConfig groupLimit = buildGroup(GROUP_LIMIT_GROUP, 1, 1);
                    GroupConfig userLimit = buildGroup(USER_LIMIT_GROUP, 2, 1);
                    return List.of(groupLimit, userLimit);
                }

                @Override
                public Optional<GroupConfig> findEnabledByGroupCode(String groupCode) {
                    return listEnabled().stream()
                            .filter(group -> group.getGroupCode().equals(groupCode))
                            .findFirst();
                }

                private GroupConfig buildGroup(String groupCode, int maxConcurrency, int userBaseConcurrency) {
                    GroupConfig group = new GroupConfig();
                    group.setGroupCode(groupCode);
                    group.setEnabled(true);
                    group.setMaxConcurrency(maxConcurrency);
                    group.setUserBaseConcurrency(userBaseConcurrency);
                    group.setDynamicUserLimitEnabled(false);
                    group.setDispatchBatchSize(20);
                    group.setHeartbeatTimeoutSec(30);
                    group.setLockExpireSec(30);
                    return group;
                }
            };
        }

        @Bean
        @Primary
        TaskHandler syncTestTaskHandler() {
            return new SyncTestTaskHandler();
        }
    }

    static class SyncTestTaskHandler implements TaskHandler {
        private static final Map<String, HoldControl> HOLDS = new ConcurrentHashMap<>();

        static void registerHold(String bizKey, CountDownLatch started, CountDownLatch release) {
            HOLDS.put(bizKey, new HoldControl(started, release));
        }

        static void releaseAll() {
            HOLDS.values().forEach(control -> control.release().countDown());
        }

        static void clear() {
            HOLDS.clear();
        }

        @Override
        public List<String> bizTypes() {
            return List.of(BIZ_TYPE);
        }

        @Override
        public org.dong.scheduler.core.model.TaskExecuteResult execute(SchedulerTask task) throws Exception {
            HoldControl control = HOLDS.get(task.getBizKey());
            if (control != null) {
                control.started().countDown();
                boolean released = control.release().await(15, TimeUnit.SECONDS);
                HOLDS.remove(task.getBizKey());
                if (!released) {
                    return org.dong.scheduler.core.model.TaskExecuteResult.failed(
                            "SYNC_TEST_RELEASE_TIMEOUT",
                            "test release latch timed out",
                            false
                    );
                }
            }
            return org.dong.scheduler.core.model.TaskExecuteResult.success();
        }
    }

    private record HoldControl(CountDownLatch started, CountDownLatch release) {
    }
}
