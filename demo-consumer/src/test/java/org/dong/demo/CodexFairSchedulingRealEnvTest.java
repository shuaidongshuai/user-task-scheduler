package org.dong.demo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.RedisKeys;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "utask.scheduler.dispatch-enabled=false",
        "utask.scheduler.instance-id=codex-it",
        "utask.scheduler.worker-threads=4",
        "utask.scheduler.max-worker-threads=4",
        "utask.scheduler.worker-queue-capacity=0",
        "utask.scheduler.heartbeat-interval-sec=2",
        "utask.scheduler.default-retry-delay-sec=1"
})
class CodexFairSchedulingRealEnvTest {
    private static final String TEST_GROUP = "codex_fair_test";
    private static final String HEAVY_USER = "codex-heavy";
    private static final String LIGHT_USER = "codex-light";
    private static final String BIZ_TYPE = "codex.fair.process";

    @Autowired
    private SchedulerClient schedulerClient;
    @Autowired
    private DispatchService dispatchService;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void shouldScheduleLightUserFairlyWhenHeavyUserHasBacklog() throws Exception {
        ensureDemoTable();
        clearTestRedis();

        String runId = "codex-fair-" + UUID.randomUUID().toString().replace("-", "");
        List<Long> heavyTaskIds = submitTasks(runId, HEAVY_USER, "heavy", 20);

        long heavyWarmupDeadline = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
        while (System.currentTimeMillis() < heavyWarmupDeadline && runningByUser(runId, HEAVY_USER) < 2) {
            dispatchService.dispatchOnce();
            Thread.sleep(100L);
        }
        assertEquals(2, runningByUser(runId, HEAVY_USER), "heavy user should occupy its per-user limit first");

        List<Long> lightTaskIds = submitTasks(runId, LIGHT_USER, "light", 4);
        long lightSubmitAt = System.currentTimeMillis();
        Long firstLightStartMs = null;
        int maxGroupOccupied = 0;
        int maxHeavyOccupied = 0;
        int maxLightOccupied = 0;

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline && successCount(runId) < 24) {
            dispatchService.dispatchOnce();
            maxGroupOccupied = Math.max(maxGroupOccupied, occupiedByGroup(runId));
            maxHeavyOccupied = Math.max(maxHeavyOccupied, runningByUser(runId, HEAVY_USER));
            maxLightOccupied = Math.max(maxLightOccupied, runningByUser(runId, LIGHT_USER));
            if (firstLightStartMs == null && executionStarted(lightTaskIds) > 0) {
                firstLightStartMs = System.currentTimeMillis() - lightSubmitAt;
            }
            Thread.sleep(200L);
        }

        assertNotNull(firstLightStartMs, "light user should get execution while heavy user still has backlog");
        assertTrue(firstLightStartMs < Duration.ofSeconds(8).toMillis(),
                "light user first execution waited too long: " + firstLightStartMs + "ms");
        assertTrue(maxGroupOccupied <= 4, "group occupied slots exceeded max concurrency: " + maxGroupOccupied);
        assertTrue(maxHeavyOccupied <= 2, "heavy user exceeded user limit: " + maxHeavyOccupied);
        assertTrue(maxLightOccupied <= 2, "light user exceeded user limit: " + maxLightOccupied);
        assertEquals(24, successCount(runId), "all submitted tasks should eventually succeed");
        assertEquals(24, terminalCount(runId), "all submitted tasks should be terminal");
        assertTrue(executionStarted(heavyTaskIds) >= 20, "heavy tasks should have execution records");
        assertTrue(executionStarted(lightTaskIds) >= 4, "light tasks should have execution records");
    }

    @Test
    void shouldScheduleLightUserWhenHeavyUserHas120BacklogTasks() throws Exception {
        ensureDemoTable();
        clearTestRedis();

        String runId = "codex-fair-" + UUID.randomUUID().toString().replace("-", "");
        List<Long> heavyTaskIds = submitTasks(runId, HEAVY_USER, "heavy120", 120);

        long heavyWarmupDeadline = System.currentTimeMillis() + Duration.ofSeconds(8).toMillis();
        while (System.currentTimeMillis() < heavyWarmupDeadline && runningByUser(runId, HEAVY_USER) < 2) {
            dispatchService.dispatchOnce();
            Thread.sleep(100L);
        }
        assertEquals(2, runningByUser(runId, HEAVY_USER), "heavy user should occupy its per-user limit first");

        List<Long> lightTaskIds = submitTasks(runId, LIGHT_USER, "light", 4);
        long lightSubmitAt = System.currentTimeMillis();
        Long firstLightStartMs = null;
        int maxGroupOccupied = 0;
        int maxHeavyOccupied = 0;
        int maxLightOccupied = 0;

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        while (System.currentTimeMillis() < deadline && successCount(runId, LIGHT_USER) < 4) {
            dispatchService.dispatchOnce();
            maxGroupOccupied = Math.max(maxGroupOccupied, occupiedByGroup(runId));
            maxHeavyOccupied = Math.max(maxHeavyOccupied, runningByUser(runId, HEAVY_USER));
            maxLightOccupied = Math.max(maxLightOccupied, runningByUser(runId, LIGHT_USER));
            if (firstLightStartMs == null && executionStarted(lightTaskIds) > 0) {
                firstLightStartMs = System.currentTimeMillis() - lightSubmitAt;
            }
            Thread.sleep(100L);
        }

        assertNotNull(firstLightStartMs, "light user should be discovered behind 120 heavy ready tasks");
        assertTrue(firstLightStartMs < Duration.ofSeconds(8).toMillis(),
                "light user first execution waited too long behind 120 heavy tasks: " + firstLightStartMs + "ms");
        assertTrue(maxGroupOccupied <= 4, "group occupied slots exceeded max concurrency: " + maxGroupOccupied);
        assertTrue(maxHeavyOccupied <= 2, "heavy user exceeded user limit: " + maxHeavyOccupied);
        assertTrue(maxLightOccupied <= 2, "light user exceeded user limit: " + maxLightOccupied);
        assertEquals(4, successCount(runId, LIGHT_USER), "light tasks should succeed before heavy backlog drains");
        assertTrue(activeOrPendingHeavyCount(runId) > 0, "heavy backlog should still exist when light tasks finish");
        assertTrue(executionStarted(heavyTaskIds) > 0, "heavy tasks should have started");
    }

    private List<Long> submitTasks(String runId, String userId, String type, int count) {
        List<Long> taskIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String bizKey = runId + "-" + type + "-" + i;
            jdbcTemplate.update("""
                    insert into demo_biz_task(biz_key, status, payload, create_time, update_time)
                    values(?, 'SUBMIT', '{}', now(), now())
                    on duplicate key update status='SUBMIT', payload=values(payload), update_time=now()
                    """, bizKey);
            long taskId = schedulerClient.submit(new TaskSubmitRequest()
                    .setGroupCode(TEST_GROUP)
                    .setUserId(userId)
                    .setBizType(BIZ_TYPE)
                    .setBizKey(bizKey)
                    .setPriority(50)
                    .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                    .setMaxRetryCount(2)
                    .setExecuteTimeoutSec(60)
                    .setRetryDelaySec(1));
            taskIds.add(taskId);
        }
        return taskIds;
    }

    private int occupiedByGroup(String runId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status in ('DISPATCHED','RUNNING')
                """, Integer.class, TEST_GROUP, runId + "%");
    }

    private int runningByUser(String runId, String userId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and user_id = ?
                   and biz_key like ?
                   and status in ('DISPATCHED','RUNNING')
                """, Integer.class, TEST_GROUP, userId, runId + "%");
    }

    private int successCount(String runId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status = 'SUCCESS'
                """, Integer.class, TEST_GROUP, runId + "%");
    }

    private int successCount(String runId, String userId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and user_id = ?
                   and biz_key like ?
                   and status = 'SUCCESS'
                """, Integer.class, TEST_GROUP, userId, runId + "%");
    }

    private int activeOrPendingHeavyCount(String runId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and user_id = ?
                   and biz_key like ?
                   and status in ('RUNNABLE','DISPATCHED','RUNNING','WAIT_RETRY')
                """, Integer.class, TEST_GROUP, HEAVY_USER, runId + "%");
    }

    private int terminalCount(String runId) {
        return jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status in ('SUCCESS','FAILED','CANCELLED')
                """, Integer.class, TEST_GROUP, runId + "%");
    }

    private int executionStarted(List<Long> taskIds) {
        String placeholders = String.join(",", taskIds.stream().map(id -> "?").toList());
        Object[] args = taskIds.stream().map(Long.class::cast).toArray();
        return jdbcTemplate.queryForObject(
                "select count(distinct task_id) from scheduler_task_execution where task_id in (" + placeholders + ")",
                Integer.class,
                args
        );
    }

    private void clearTestRedis() {
        redisTemplate.delete(List.of(
                RedisKeys.timeQueue(TEST_GROUP),
                RedisKeys.readyQueue(TEST_GROUP),
                RedisKeys.groupRunning(TEST_GROUP),
                RedisKeys.userRunning(TEST_GROUP, HEAVY_USER),
                RedisKeys.userRunning(TEST_GROUP, LIGHT_USER),
                RedisKeys.groupReconcileThrottle(TEST_GROUP)
        ));
    }

    private void ensureDemoTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS demo_biz_task (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    biz_key VARCHAR(128) NOT NULL UNIQUE COMMENT '业务键（与调度任务biz_key对应）',
                    status VARCHAR(32) NOT NULL COMMENT 'RUNNING/SUCCESS/FAILED',
                    payload TEXT DEFAULT NULL COMMENT '示例业务负载',
                    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Demo业务任务表示例'
                """);
    }

    @TestConfiguration
    static class FairSchedulingConfig {
        @Bean
        @Primary
        GroupConfigRepository fairOnlyGroupConfigRepository() {
            return new GroupConfigRepository() {
                @Override
                public List<GroupConfig> listEnabled() {
                    GroupConfig group = new GroupConfig();
                    group.setGroupCode(TEST_GROUP);
                    group.setEnabled(true);
                    group.setMaxConcurrency(4);
                    group.setUserBaseConcurrency(2);
                    group.setDynamicUserLimitEnabled(false);
                    group.setDispatchBatchSize(100);
                    group.setHeartbeatTimeoutSec(30);
                    group.setLockExpireSec(60);
                    return List.of(group);
                }

                @Override
                public Optional<GroupConfig> findEnabledByGroupCode(String groupCode) {
                    return listEnabled().stream()
                            .filter(group -> group.getGroupCode().equals(groupCode))
                            .findFirst();
                }
            };
        }

        @Bean
        @Primary
        TaskHandler fairTestTaskHandler() {
            return new TaskHandler() {
                @Override
                public List<String> bizTypes() {
                    return List.of(BIZ_TYPE);
                }

                @Override
                public org.dong.scheduler.core.model.TaskExecuteResult execute(
                        org.dong.scheduler.core.model.SchedulerTask task) throws Exception {
                    if (task.getBizKey() != null && task.getBizKey().contains("-heavy120-")) {
                        Thread.sleep(15_000L);
                    } else {
                        Thread.sleep(500L);
                    }
                    return org.dong.scheduler.core.model.TaskExecuteResult.success();
                }
            };
        }
    }
}
