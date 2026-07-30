package org.dong.scheduler.core.redis;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.model.SchedulerTask;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class QueueRedisService {
    private static final long SCORE_MULTIPLIER = 10_000_000_000_000L;
    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 99;
    private static final ZoneId SYSTEM_ZONE = ZoneId.systemDefault();

    private final StringRedisTemplate redisTemplate;
    private final SchedulerProperties properties;

    public QueueRedisService(StringRedisTemplate redisTemplate, SchedulerProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public void enqueue(SchedulerTask task) {
        long executeAt = toEpochMillis(task.getExecuteAt());
        redisTemplate.opsForZSet().add(RedisKeys.timeQueue(task.getGroupCode(), task.getDispatchRoute()),
                task.getId().toString(), executeAt);
    }

    public List<Long> promoteDueTasks(String groupCode, String dispatchRoute, long nowEpochMillis, int limit) {
        Set<String> due = redisTemplate.opsForZSet()
                .rangeByScore(RedisKeys.timeQueue(groupCode, dispatchRoute), Double.NEGATIVE_INFINITY, nowEpochMillis, 0, limit);
        if (due == null || due.isEmpty()) {
            return List.of();
        }

        List<Long> moved = new ArrayList<>(due.size());
        for (String taskIdStr : due) {
            Long taskId = Long.valueOf(taskIdStr);
            moved.add(taskId);
            redisTemplate.opsForZSet().remove(RedisKeys.timeQueue(groupCode, dispatchRoute), taskIdStr);
        }
        return moved;
    }

    public void enqueueReady(SchedulerTask task) {
        String queueKey = RedisKeys.userReadyQueue(task.getGroupCode(), task.getDispatchRoute(), task.getUserId());
        redisTemplate.opsForZSet().add(queueKey, task.getId().toString(), taskScore(task));
        refreshActiveUser(task.getGroupCode(), task.getDispatchRoute(), task.getUserId(), false);
    }

    public boolean existsInReadyQueue(SchedulerTask task) {
        Double score = redisTemplate.opsForZSet().score(
                RedisKeys.userReadyQueue(task.getGroupCode(), task.getDispatchRoute(), task.getUserId()),
                String.valueOf(task.getId())
        );
        return score != null;
    }

    public void removeFromReadyQueue(SchedulerTask task) {
        removeFromReadyQueue(task.getGroupCode(), task.getDispatchRoute(), task.getUserId(), task.getId());
    }

    public void removeFromReadyQueue(String queueGroupCode, String dispatchRoute, String userId, long taskId) {
        redisTemplate.opsForZSet().remove(
                RedisKeys.userReadyQueue(queueGroupCode, dispatchRoute, userId),
                String.valueOf(taskId)
        );
    }

    public void removeQueueReferences(SchedulerTask oldSnapshot) {
        removeFromTime(oldSnapshot.getGroupCode(), oldSnapshot.getDispatchRoute(), oldSnapshot.getId());
        removeFromReadyQueue(oldSnapshot);
        rebalanceActiveUser(oldSnapshot.getGroupCode(), oldSnapshot.getDispatchRoute(), oldSnapshot.getUserId());
    }

    public String peekNextActiveUser(String groupCode, String dispatchRoute) {
        Set<String> values = redisTemplate.opsForZSet().range(RedisKeys.activeUsers(groupCode, dispatchRoute), 0, 0);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.iterator().next();
    }

    public String tryAcquireActiveUserLock(String groupCode, String dispatchRoute, String userId, long ttlMillis) {
        String token = UUID.randomUUID().toString().replace("-", "");
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                RedisKeys.activeUserLock(groupCode, dispatchRoute, userId),
                token,
                Duration.ofMillis(ttlMillis)
        );
        return Boolean.TRUE.equals(acquired) ? token : null;
    }

    public void releaseActiveUserLock(String groupCode, String dispatchRoute, String userId, String token) {
        String key = RedisKeys.activeUserLock(groupCode, dispatchRoute, userId);
        String current = redisTemplate.opsForValue().get(key);
        if (token != null && token.equals(current)) {
            redisTemplate.delete(key);
        }
    }

    public Integer peekReadyHeadPriority(String groupCode, String dispatchRoute, String userId) {
        Set<String> values = redisTemplate.opsForZSet().range(
                RedisKeys.userReadyQueue(groupCode, dispatchRoute, userId), 0, 0
        );
        if (values == null || values.isEmpty()) {
            return null;
        }
        String taskId = values.iterator().next();
        Double score = redisTemplate.opsForZSet().score(
                RedisKeys.userReadyQueue(groupCode, dispatchRoute, userId),
                taskId
        );
        if (score == null) {
            return null;
        }
        return (int) (((long) Math.floor(score)) / SCORE_MULTIPLIER);
    }

    public List<Long> peekReadyTasksByPriority(String groupCode, String dispatchRoute, String userId, int priority, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        long normalizedPriority = normalizePriority(priority);
        double minScore = normalizedPriority * (double) SCORE_MULTIPLIER;
        double maxScore = minScore + SCORE_MULTIPLIER - 1;
        Set<String> values = redisTemplate.opsForZSet().rangeByScore(
                RedisKeys.userReadyQueue(groupCode, dispatchRoute, userId),
                minScore,
                maxScore,
                0,
                limit
        );
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        return values.stream().map(Long::valueOf).toList();
    }

    public void rebalanceActiveUser(String groupCode, String dispatchRoute, String userId) {
        refreshActiveUser(groupCode, dispatchRoute, userId, true);
    }

    public boolean existsInTime(String groupCode, String dispatchRoute, long taskId) {
        Double score = redisTemplate.opsForZSet().score(RedisKeys.timeQueue(groupCode, dispatchRoute), String.valueOf(taskId));
        return score != null;
    }

    public void removeFromTime(String groupCode, String dispatchRoute, long taskId) {
        redisTemplate.opsForZSet().remove(RedisKeys.timeQueue(groupCode, dispatchRoute), String.valueOf(taskId));
    }

    private void refreshActiveUser(String groupCode, String dispatchRoute, String userId, boolean moveToTail) {
        String activeKey = RedisKeys.activeUsers(groupCode, dispatchRoute);
        Integer headPriority = peekReadyHeadPriority(groupCode, dispatchRoute, userId);
        if (headPriority == null) {
            redisTemplate.opsForZSet().remove(activeKey, userId);
            return;
        }
        Double existing = redisTemplate.opsForZSet().score(activeKey, userId);
        long fairScore = moveToTail || existing == null
                ? relativeMillis()
                : ((long) Math.floor(existing)) % SCORE_MULTIPLIER;
        redisTemplate.opsForZSet().add(activeKey, userId, activeScore(headPriority, fairScore));
    }

    private long taskScore(SchedulerTask task) {
        long priority = normalizePriority(task.getPriority());
        return priority * SCORE_MULTIPLIER + toEpochMillis(task.getCreateTime());
    }

    private double activeScore(int headPriority, long fairScore) {
        return normalizePriority(headPriority) * (double) SCORE_MULTIPLIER + fairScore;
    }

    private long relativeMillis() {
        LocalDateTime base = properties.getPriorityBaseEpoch();
        long baseMillis = base.atZone(SYSTEM_ZONE).toInstant().toEpochMilli();
        return Math.max(0L, System.currentTimeMillis() - baseMillis);
    }

    private long toEpochMillis(LocalDateTime time) {
        return time.atZone(SYSTEM_ZONE).toInstant().toEpochMilli();
    }

    private long normalizePriority(int priority) {
        if (priority < MIN_PRIORITY) {
            return MIN_PRIORITY;
        }
        if (priority > MAX_PRIORITY) {
            return MAX_PRIORITY;
        }
        return priority;
    }
}
