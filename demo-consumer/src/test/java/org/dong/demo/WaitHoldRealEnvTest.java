package org.dong.demo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "utask.scheduler.dispatch-enabled=false",
        "utask.scheduler.instance-id=codex-wait-hold-it",
        "utask.scheduler.worker-threads=2",
        "utask.scheduler.max-worker-threads=2",
        "utask.scheduler.heartbeat-interval-sec=1",
        "utask.scheduler.default-retry-delay-sec=1",
        "utask.scheduler.default-execute-timeout-sec=20",
        "utask.scheduler.wait-hold-default-delay-sec=1",
        "utask.scheduler.wait-hold-max-rounds=1000"
})
class WaitHoldRealEnvTest {
    private static final String GROUP = "codex_wait_hold_group";
    private static final String USER = "wait-hold-user";
    private static final String BIZ_TYPE = "codex.wait.hold.process";
    private static final String PREFIX = "codex-wait-hold-";

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
        WaitHoldTaskHandler.clear();
    }

    @AfterEach
    void tearDown() {
        cleanupTasks();
        clearRedis();
        WaitHoldTaskHandler.clear();
    }

    @Test
    void shouldKeepConcurrencyDuringWaitHoldAndReleaseAfterSuccess() throws Exception {
        String bizKey = PREFIX + UUID.randomUUID().toString().replace("-", "");
        long taskId = schedulerClient.submit(new TaskSubmitRequest()
                .setGroupCode(GROUP)
                .setUserId(USER)
                .setBizType(BIZ_TYPE)
                .setBizKey(bizKey)
                .setPriority(10)
                .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                .setMaxRetryCount(0)
                .setHoldMaxRounds(5)
                .setHoldRetryDelaySec(1)
                .setExtInfo("{\"phase\":\"submitted\"}"));

        SchedulerTask submitted = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.RUNNABLE, submitted.getStatus());
        assertTrue(existsInUserReadyQueue(taskId));
        assertTrue(existsInActiveUsers());
        assertFalse(existsInTimeQueue(taskId));
        assertEquals(0L, groupRunning());
        assertEquals(0L, userRunning());
        assertNull(taskLease(taskId));

        dispatchUntilStatus(taskId, TaskStatus.WAIT_HOLD, 10_000L);

        SchedulerTask waiting = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.WAIT_HOLD, waiting.getStatus());
        assertEquals(1, waiting.getHoldRoundCount());
        assertEquals("{\"phase\":\"polling\",\"round\":1}", waiting.getExtInfo());
        assertTrue(existsInTimeQueue(taskId));
        assertFalse(existsInUserReadyQueue(taskId));
        assertFalse(existsInActiveUsers());
        assertEquals(1L, groupRunning());
        assertEquals(1L, userRunning());
        assertNull(taskLease(taskId));
        assertEquals(List.of("WAIT_HOLD"), executionStatuses(taskId));

        waitForDue(waiting.getExecuteAt());
        dispatchUntilStatus(taskId, TaskStatus.SUCCESS, 10_000L);

        SchedulerTask finished = taskRepository.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.SUCCESS, finished.getStatus());
        assertEquals(1, finished.getHoldRoundCount());
        assertEquals("{\"phase\":\"success\",\"round\":2}", finished.getExtInfo());
        assertFalse(existsInTimeQueue(taskId));
        assertFalse(existsInUserReadyQueue(taskId));
        assertFalse(existsInActiveUsers());
        assertEquals(0L, groupRunning());
        assertEquals(0L, userRunning());
        assertNull(taskLease(taskId));
        assertEquals(List.of("WAIT_HOLD", "SUCCESS"), executionStatuses(taskId));
    }

    private void dispatchUntilStatus(long taskId, TaskStatus target, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            dispatchService.dispatchOnce();
            SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
            if (task.getStatus() == target) {
                return;
            }
            Thread.sleep(100L);
        }
        SchedulerTask task = taskRepository.findById(taskId).orElseThrow();
        throw new AssertionError("task status not reached, taskId=" + taskId + ", expected=" + target + ", actual=" + task.getStatus());
    }

    private void waitForDue(LocalDateTime executeAt) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (!executeAt.isAfter(LocalDateTime.now())) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("wait hold executeAt never became due: " + executeAt);
    }

    private List<String> executionStatuses(long taskId) {
        return jdbcTemplate.query(
                """
                select status
                  from scheduler_task_execution
                 where task_id = ?
                 order by id asc
                """,
                (rs, rowNum) -> rs.getString(1),
                taskId
        );
    }

    private long groupRunning() {
        String value = redisTemplate.opsForValue().get(RedisKeys.groupRunning(GROUP));
        return value == null ? 0L : Long.parseLong(value);
    }

    private long userRunning() {
        String value = redisTemplate.opsForValue().get(RedisKeys.userRunning(GROUP, USER));
        return value == null ? 0L : Long.parseLong(value);
    }

    private String taskLease(long taskId) {
        return redisTemplate.opsForValue().get(RedisKeys.taskLease(taskId));
    }

    private boolean existsInTimeQueue(long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.timeQueue(GROUP), String.valueOf(taskId)) != null;
    }

    private boolean existsInUserReadyQueue(long taskId) {
        return redisTemplate.opsForZSet().score(RedisKeys.userReadyQueue(GROUP, null, USER), String.valueOf(taskId)) != null;
    }

    private boolean existsInActiveUsers() {
        return redisTemplate.opsForZSet().score(RedisKeys.activeUsers(GROUP, null), USER) != null;
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
        redisTemplate.delete(RedisKeys.timeQueue(GROUP));
        redisTemplate.delete(RedisKeys.readyQueue(GROUP));
        redisTemplate.delete(RedisKeys.activeUsers(GROUP, null));
        redisTemplate.delete(RedisKeys.userReadyQueue(GROUP, null, USER));
        redisTemplate.delete(RedisKeys.groupRunning(GROUP));
        redisTemplate.delete(RedisKeys.userRunning(GROUP, USER));
        redisTemplate.delete(redisTemplate.keys("sched:active-user-lock:" + GROUP + ":*"));
        redisTemplate.delete(redisTemplate.keys("sched:task:lease:*"));
        redisTemplate.delete(RedisKeys.groupReconcileThrottle(GROUP));
    }

    @TestConfiguration
    static class WaitHoldTestConfig {
        @Bean
        @Primary
        GroupConfigRepository waitHoldGroupConfigRepository() {
            return new GroupConfigRepository() {
                @Override
                public List<GroupConfig> listEnabled() {
                    return List.of(buildGroup());
                }

                @Override
                public Optional<GroupConfig> findEnabledByGroupCode(String groupCode) {
                    return GROUP.equals(groupCode) ? Optional.of(buildGroup()) : Optional.empty();
                }

                private GroupConfig buildGroup() {
                    GroupConfig group = new GroupConfig();
                    group.setGroupCode(GROUP);
                    group.setEnabled(true);
                    group.setMaxConcurrency(1);
                    group.setUserBaseConcurrency(1);
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
        TaskHandler waitHoldTaskHandler() {
            return new WaitHoldTaskHandler();
        }
    }

    static class WaitHoldTaskHandler implements TaskHandler {
        private static final Map<String, Integer> ROUNDS = new ConcurrentHashMap<>();

        static void clear() {
            ROUNDS.clear();
        }

        @Override
        public List<String> bizTypes() {
            return List.of(BIZ_TYPE);
        }

        @Override
        public TaskExecuteResult execute(SchedulerTask task) {
            int round = ROUNDS.merge(task.getBizKey(), 1, Integer::sum);
            if (round == 1) {
                task.setExtInfo("{\"phase\":\"polling\",\"round\":1}");
                return TaskExecuteResult.waitHold();
            }
            task.setExtInfo("{\"phase\":\"success\",\"round\":2}");
            return TaskExecuteResult.success();
        }
    }
}
