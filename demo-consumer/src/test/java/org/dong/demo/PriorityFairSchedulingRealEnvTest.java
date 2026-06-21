package org.dong.demo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.redis.RedisKeys;
import org.dong.scheduler.core.repo.GroupConfigRepository;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "utask.scheduler.dispatch-enabled=false",
        "utask.scheduler.instance-id=codex-priority-fair-it",
        "utask.scheduler.worker-threads=16",
        "utask.scheduler.max-worker-threads=16",
        "utask.scheduler.worker-queue-capacity=0",
        "utask.scheduler.heartbeat-interval-sec=1",
        "utask.scheduler.default-retry-delay-sec=1",
        "utask.scheduler.default-execute-timeout-sec=120"
})
class PriorityFairSchedulingRealEnvTest {
    private static final String TEST_GROUP = "codex_priority_fair_test";
    private static final String BIZ_TYPE = "codex.priority.fair.process";
    private static final String PREFIX = "codex-priority-fair-";

    @Autowired
    private SchedulerClient schedulerClient;
    @Autowired
    private DispatchService dispatchService;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        cleanupTasks();
        clearRedis();
    }

    @AfterEach
    void tearDown() {
        FirstWaveHoldController.clear();
        cleanupTasks();
        clearRedis();
    }

    @Test
    void shouldValidatePriorityAndFairnessOnRealDatabase() throws Exception {
        ScenarioConfig config = ScenarioConfig.fromSystemProperties();
        String runId = PREFIX + UUID.randomUUID().toString().replace("-", "");
        FirstWaveHoldController.install(runId, config.groupConcurrency());

        UserScenario heavyUser = config.heavyUser();
        Map<String, List<Long>> taskIdsByUser = new LinkedHashMap<>();
        for (UserScenario user : config.users()) {
            taskIdsByUser.put(user.userId(), submitTasks(runId, user));
        }
        assertTrue(taskIdsByUser.get(heavyUser.userId()).size() > config.userConcurrency(),
                "heavy user should have enough backlog to verify fairness");

        Map<String, Integer> maxRunningByUser = new LinkedHashMap<>();
        Map<String, Long> firstStartLatencyByUser = new LinkedHashMap<>();
        int maxGroupRunning = 0;
        boolean otherStartedWhileHeavyBacklogExists = false;
        boolean firstWaveReleased = false;
        long dispatchStartAt = System.currentTimeMillis();
        long deadline = System.currentTimeMillis() + config.testTimeoutMs();
        while (System.currentTimeMillis() < deadline && terminalCount(runId) < config.totalTaskCount()) {
            dispatchService.dispatchOnce();
            maxGroupRunning = Math.max(maxGroupRunning, occupiedByGroup(runId));
            if (!firstWaveReleased && maxGroupRunning >= config.groupConcurrency()) {
                FirstWaveHoldController.release(runId);
                firstWaveReleased = true;
            }

            for (UserScenario user : config.users()) {
                int running = runningByUser(runId, user.userId());
                maxRunningByUser.merge(user.userId(), running, Math::max);
            }

            for (UserScenario user : config.nonHeavyUsers()) {
                if (firstStartLatencyByUser.containsKey(user.userId())) {
                    continue;
                }
                if (executionStarted(runId, user.userId()) > 0) {
                    firstStartLatencyByUser.put(user.userId(), System.currentTimeMillis() - dispatchStartAt);
                    if (activeOrPendingCount(runId, heavyUser.userId()) > 0) {
                        otherStartedWhileHeavyBacklogExists = true;
                    }
                }
            }
            Thread.sleep(config.pollIntervalMs());
        }
        FirstWaveHoldController.release(runId);

        assertEquals(config.totalTaskCount(), successCount(runId), "all tasks should finish successfully");
        assertEquals(config.totalTaskCount(), terminalCount(runId), "all tasks should become terminal");
        assertEquals(config.groupConcurrency(), maxGroupRunning, "group concurrency should be fully occupied");
        assertTrue(otherStartedWhileHeavyBacklogExists,
                "other users should start while heavy user still has backlog");
        for (UserScenario user : config.users()) {
            Integer peakRunning = maxRunningByUser.get(user.userId());
            assertNotNull(peakRunning, "missing running metric for user " + user.userId());
            assertEquals(Math.min(config.userConcurrency(), user.totalTaskCount()), peakRunning,
                    "user concurrency not fully occupied for " + user.userId());
            assertPriorityOrder(runId, user);
        }

        for (UserScenario user : config.nonHeavyUsers()) {
            Long firstStartLatency = firstStartLatencyByUser.get(user.userId());
            assertNotNull(firstStartLatency, "user never started: " + user.userId());
            assertTrue(firstStartLatency <= config.otherUserStartDeadlineMs(),
                    "user started too late while heavy backlog existed, user=" + user.userId()
                            + ", latencyMs=" + firstStartLatency);
        }
    }

    private void assertPriorityOrder(String runId, UserScenario user) {
        List<Integer> startedPriorities = jdbcTemplate.query("""
                select t.priority
                  from scheduler_task_execution e
                  join scheduler_task t on t.id = e.task_id
                 where t.group_code = ?
                   and t.user_id = ?
                   and t.biz_key like ?
                 order by e.start_time asc, e.id asc
                """, (rs, rowNum) -> rs.getInt(1), TEST_GROUP, user.userId(), runId + "%");
        assertEquals(user.totalTaskCount(), startedPriorities.size(),
                "execution record count mismatch for user " + user.userId());
        for (int i = 1; i < startedPriorities.size(); i++) {
            int previous = startedPriorities.get(i - 1);
            int current = startedPriorities.get(i);
            assertTrue(previous <= current,
                    "priority order broken for user " + user.userId() + ": " + startedPriorities);
        }
    }

    private List<Long> submitTasks(String runId, UserScenario user) {
        List<Long> taskIds = new ArrayList<>();
        int sequence = 0;
        for (PriorityBatch batch : user.batches()) {
            for (int i = 0; i < batch.count(); i++) {
                String bizKey = runId + "-" + user.userId() + "-p" + batch.priority() + "-" + sequence++;
                long taskId = schedulerClient.submit(new TaskSubmitRequest()
                        .setGroupCode(TEST_GROUP)
                        .setUserId(user.userId())
                        .setBizType(BIZ_TYPE)
                        .setBizKey(bizKey)
                        .setPriority(batch.priority())
                        .setExecuteAt(LocalDateTime.now().minusSeconds(1))
                        .setMaxRetryCount(0)
                        .setExecuteTimeoutSec(120)
                        .setRetryDelaySec(1));
                taskIds.add(taskId);
            }
        }
        return taskIds;
    }

    private int occupiedByGroup(String runId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status in ('DISPATCHED','RUNNING')
                """, Integer.class, TEST_GROUP, runId + "%");
        return count == null ? 0 : count;
    }

    private int runningByUser(String runId, String userId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and user_id = ?
                   and biz_key like ?
                   and status in ('DISPATCHED','RUNNING')
                """, Integer.class, TEST_GROUP, userId, runId + "%");
        return count == null ? 0 : count;
    }

    private int successCount(String runId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status = 'SUCCESS'
                """, Integer.class, TEST_GROUP, runId + "%");
        return count == null ? 0 : count;
    }

    private int terminalCount(String runId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                   and status in ('SUCCESS','FAILED','CANCELLED')
                """, Integer.class, TEST_GROUP, runId + "%");
        return count == null ? 0 : count;
    }

    private int activeOrPendingCount(String runId, String userId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(1)
                  from scheduler_task
                 where group_code = ?
                   and user_id = ?
                   and biz_key like ?
                   and status in ('PENDING','RUNNABLE','DISPATCHED','RUNNING','WAIT_RETRY')
                """, Integer.class, TEST_GROUP, userId, runId + "%");
        return count == null ? 0 : count;
    }

    private int executionStarted(String runId, String userId) {
        Integer count = jdbcTemplate.queryForObject("""
                select count(distinct e.task_id)
                  from scheduler_task_execution e
                  join scheduler_task t on t.id = e.task_id
                 where t.group_code = ?
                   and t.user_id = ?
                   and t.biz_key like ?
                """, Integer.class, TEST_GROUP, userId, runId + "%");
        return count == null ? 0 : count;
    }

    private void cleanupTasks() {
        List<Long> taskIds = jdbcTemplate.query("""
                select id
                  from scheduler_task
                 where group_code = ?
                   and biz_key like ?
                """, (rs, rowNum) -> rs.getLong(1), TEST_GROUP, PREFIX + "%");
        if (taskIds.isEmpty()) {
            return;
        }
        String ids = taskIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        jdbcTemplate.update("delete from scheduler_task_execution where task_id in (" + ids + ")");
        jdbcTemplate.update("delete from scheduler_task_dependency where task_id in (" + ids + ") or depends_on_task_id in (" + ids + ")");
        jdbcTemplate.update("delete from scheduler_task where id in (" + ids + ")");
    }

    private void clearRedis() {
        redisTemplate.delete(RedisKeys.timeQueue(TEST_GROUP));
        redisTemplate.delete(RedisKeys.readyQueue(TEST_GROUP));
        redisTemplate.delete(RedisKeys.activeUsers(TEST_GROUP, null));
        redisTemplate.delete(redisTemplate.keys(RedisKeys.userRunningPattern(TEST_GROUP)));
        redisTemplate.delete(redisTemplate.keys("sched:ready:user:" + TEST_GROUP + ":*"));
        redisTemplate.delete(redisTemplate.keys("sched:active-user-lock:" + TEST_GROUP + ":*"));
        redisTemplate.delete(RedisKeys.groupRunning(TEST_GROUP));
        redisTemplate.delete(RedisKeys.groupReconcileThrottle(TEST_GROUP));
    }

    @TestConfiguration
    static class PriorityFairSchedulingConfig {
        @Bean
        @Primary
        GroupConfigRepository priorityFairGroupConfigRepository() {
            return new GroupConfigRepository() {
                @Override
                public List<GroupConfig> listEnabled() {
                    ScenarioConfig config = ScenarioConfig.fromSystemProperties();
                    GroupConfig group = new GroupConfig();
                    group.setGroupCode(TEST_GROUP);
                    group.setEnabled(true);
                    group.setMaxConcurrency(config.groupConcurrency());
                    group.setUserBaseConcurrency(config.userConcurrency());
                    group.setDynamicUserLimitEnabled(false);
                    group.setDispatchBatchSize(config.dispatchBatchSize());
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
        TaskHandler priorityFairTaskHandler() {
            return new TaskHandler() {
                @Override
                public List<String> bizTypes() {
                    return List.of(BIZ_TYPE);
                }

                @Override
                public org.dong.scheduler.core.model.TaskExecuteResult execute(
                        org.dong.scheduler.core.model.SchedulerTask task) throws Exception {
                    FirstWaveHoldController.awaitIfNeeded(task);
                    Thread.sleep(ScenarioConfig.fromSystemProperties().taskSleepMs());
                    return org.dong.scheduler.core.model.TaskExecuteResult.success();
                }
            };
        }
    }

    private record PriorityBatch(int priority, int count) {
    }

    private record UserScenario(String userId, List<PriorityBatch> batches) {
        int totalTaskCount() {
            return batches.stream().mapToInt(PriorityBatch::count).sum();
        }
    }

    static final class FirstWaveHoldController {
        private static volatile HoldState state;

        private FirstWaveHoldController() {
        }

        static void install(String runId, int target) {
            state = new HoldState(runId, target);
        }

        static void awaitIfNeeded(org.dong.scheduler.core.model.SchedulerTask task) throws InterruptedException {
            HoldState current = state;
            if (current == null || task.getBizKey() == null || !task.getBizKey().startsWith(current.runId())) {
                return;
            }
            int order = current.startedCount().incrementAndGet();
            if (order <= current.target()) {
                current.firstWaveReached().countDown();
                current.releaseLatch().await(60, TimeUnit.SECONDS);
            }
        }

        static void release(String runId) {
            HoldState current = state;
            if (current != null && current.runId().equals(runId)) {
                while (current.firstWaveReached().getCount() > 0) {
                    current.firstWaveReached().countDown();
                }
                current.releaseLatch().countDown();
            }
        }

        static void clear() {
            HoldState current = state;
            if (current != null) {
                current.releaseLatch().countDown();
            }
            state = null;
        }

        private record HoldState(String runId,
                                 int target,
                                 AtomicInteger startedCount,
                                 CountDownLatch firstWaveReached,
                                 CountDownLatch releaseLatch) {
            private HoldState(String runId, int target) {
                this(runId, target, new AtomicInteger(), new CountDownLatch(target), new CountDownLatch(1));
            }
        }
    }

    private record ScenarioConfig(List<UserScenario> users,
                                  String heavyUserId,
                                  int groupConcurrency,
                                  int userConcurrency,
                                  int dispatchBatchSize,
                                  long taskSleepMs,
                                  long pollIntervalMs,
                                  long warmupTimeoutMs,
                                  long testTimeoutMs,
                                  long otherUserStartDeadlineMs) {
        static ScenarioConfig fromSystemProperties() {
            List<UserScenario> users = parseUserSpecs(System.getProperty(
                    "fair.test.userSpecs",
                    "heavy-user=0x8,10x8,20x8;user-b=0x4,10x4,20x4;user-c=0x4,10x4,20x4"
            ));
            String heavyUserId = System.getProperty("fair.test.heavyUser", users.get(0).userId());
            int userConcurrency = Integer.getInteger("fair.test.userConcurrency", 2);
            int dispatchBatchSize = Integer.getInteger("fair.test.dispatchBatchSize", 100);
            long taskSleepMs = Long.getLong("fair.test.taskSleepMs", 5_000L);
            long pollIntervalMs = Long.getLong("fair.test.pollIntervalMs", 200L);
            long warmupTimeoutMs = Long.getLong("fair.test.warmupTimeoutMs", 8_000L);
            long otherUserStartDeadlineMs = Long.getLong("fair.test.otherUserStartDeadlineMs", 8_000L);
            int defaultGroupConcurrency = userConcurrency * users.size();
            int groupConcurrency = Integer.getInteger("fair.test.groupConcurrency", defaultGroupConcurrency);
            long defaultTestTimeoutMs = Long.getLong("fair.test.testTimeoutMs",
                    estimateTimeoutMs(users, userConcurrency, groupConcurrency, taskSleepMs, warmupTimeoutMs));
            ScenarioConfig config = new ScenarioConfig(
                    users,
                    heavyUserId,
                    groupConcurrency,
                    userConcurrency,
                    dispatchBatchSize,
                    taskSleepMs,
                    pollIntervalMs,
                    warmupTimeoutMs,
                    defaultTestTimeoutMs,
                    otherUserStartDeadlineMs
            );
            config.validate();
            return config;
        }

        private static List<UserScenario> parseUserSpecs(String raw) {
            List<UserScenario> users = new ArrayList<>();
            for (String userPart : raw.split(";")) {
                String trimmedUserPart = userPart.trim();
                if (trimmedUserPart.isEmpty()) {
                    continue;
                }
                String[] segments = trimmedUserPart.split("=", 2);
                if (segments.length != 2) {
                    throw new IllegalArgumentException("invalid user spec: " + trimmedUserPart);
                }
                String userId = segments[0].trim();
                List<PriorityBatch> batches = new ArrayList<>();
                for (String batchPart : segments[1].split(",")) {
                    String trimmedBatch = batchPart.trim();
                    if (trimmedBatch.isEmpty()) {
                        continue;
                    }
                    String[] pair = trimmedBatch.split("x", 2);
                    if (pair.length != 2) {
                        throw new IllegalArgumentException("invalid priority batch: " + trimmedBatch);
                    }
                    int priority = Integer.parseInt(pair[0].trim());
                    int count = Integer.parseInt(pair[1].trim());
                    batches.add(new PriorityBatch(priority, count));
                }
                batches.sort(Comparator.comparingInt(PriorityBatch::priority));
                users.add(new UserScenario(userId, List.copyOf(batches)));
            }
            if (users.size() < 2) {
                throw new IllegalArgumentException("fair test requires at least 2 users");
            }
            return List.copyOf(users);
        }

        private static long estimateTimeoutMs(List<UserScenario> users,
                                              int userConcurrency,
                                              int groupConcurrency,
                                              long taskSleepMs,
                                              long warmupTimeoutMs) {
            int totalTasks = users.stream().mapToInt(UserScenario::totalTaskCount).sum();
            int effectiveParallelism = Math.max(1, Math.min(groupConcurrency, userConcurrency * users.size()));
            long rounds = (totalTasks + effectiveParallelism - 1L) / effectiveParallelism;
            return warmupTimeoutMs + rounds * taskSleepMs + Duration.ofSeconds(20).toMillis();
        }

        void validate() {
            assertFalse(users.isEmpty(), "users must not be empty");
            boolean heavyExists = users.stream().anyMatch(user -> user.userId().equals(heavyUserId));
            assertTrue(heavyExists, "heavy user not found in fair.test.userSpecs: " + heavyUserId);
            assertTrue(groupConcurrency >= userConcurrency * users.size(),
                    "groupConcurrency should be >= userConcurrency * userCount so each user can hit user limit");
            for (UserScenario user : users) {
                assertTrue(user.totalTaskCount() >= userConcurrency,
                        "each user needs at least userConcurrency tasks: " + user.userId());
                for (PriorityBatch batch : user.batches()) {
                    assertTrue(batch.priority() >= 0 && batch.priority() <= 99,
                            "priority out of range for user " + user.userId() + ": " + batch.priority());
                    assertTrue(batch.count() > 0, "task count must be positive for user " + user.userId());
                }
            }
            assertTrue(heavyUser().totalTaskCount() > nonHeavyUsers().stream()
                    .mapToInt(UserScenario::totalTaskCount)
                    .max()
                    .orElse(0), "heavy user should have the largest backlog");
        }

        UserScenario heavyUser() {
            return users.stream()
                    .filter(user -> user.userId().equals(heavyUserId))
                    .findFirst()
                    .orElseThrow();
        }

        List<UserScenario> nonHeavyUsers() {
            return users.stream()
                    .filter(user -> !user.userId().equals(heavyUserId))
                    .toList();
        }

        int totalTaskCount() {
            return users.stream().mapToInt(UserScenario::totalTaskCount).sum();
        }
    }
}
