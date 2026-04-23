package org.dong.scheduler.core.redis;

import org.dong.scheduler.core.model.SchedulerTask;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class QueueRedisService {
    private static final long SCORE_MULTIPLIER = 10_000_000_000_000L;
    private static final int MAX_PRIORITY = 99_999;

    private final StringRedisTemplate redisTemplate;

    public QueueRedisService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void enqueue(SchedulerTask task) {
        long executeAt = task.getExecuteAt().toInstant(ZoneOffset.UTC).toEpochMilli();
        redisTemplate.opsForZSet().add(RedisKeys.timeQueue(task.getGroupCode()), task.getId().toString(), executeAt);
    }

    public List<Long> promoteDueTasks(String groupCode, long nowEpochMillis, int limit) {
        Set<String> due = redisTemplate.opsForZSet()
                .rangeByScore(RedisKeys.timeQueue(groupCode), Double.NEGATIVE_INFINITY, nowEpochMillis, 0, limit);
        if (due == null || due.isEmpty()) {
            return List.of();
        }

        List<Long> moved = new ArrayList<>(due.size());
        for (String taskIdStr : due) {
            Long taskId = Long.valueOf(taskIdStr);
            moved.add(taskId);
            redisTemplate.opsForZSet().remove(RedisKeys.timeQueue(groupCode), taskIdStr);
        }
        return moved;
    }

    public void addToReady(SchedulerTask task) {
        long createTs = task.getCreateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        long score = ((long) (MAX_PRIORITY - task.getPriority())) * SCORE_MULTIPLIER + createTs;
        redisTemplate.opsForZSet().add(RedisKeys.readyQueue(task.getGroupCode()), task.getId().toString(), score);
    }

    public List<Long> peekReady(String groupCode, int limit) {
        Set<String> values = redisTemplate.opsForZSet().range(RedisKeys.readyQueue(groupCode), 0, limit - 1);
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        return values.stream().map(Long::valueOf).toList();
    }

    public void removeFromReady(String groupCode, long taskId) {
        redisTemplate.opsForZSet().remove(RedisKeys.readyQueue(groupCode), String.valueOf(taskId));
    }

    public boolean existsInReady(String groupCode, long taskId) {
        Double score = redisTemplate.opsForZSet().score(RedisKeys.readyQueue(groupCode), String.valueOf(taskId));
        return score != null;
    }

    public boolean existsInTime(String groupCode, long taskId) {
        Double score = redisTemplate.opsForZSet().score(RedisKeys.timeQueue(groupCode), String.valueOf(taskId));
        return score != null;
    }

    public void removeFromTime(String groupCode, long taskId) {
        redisTemplate.opsForZSet().remove(RedisKeys.timeQueue(groupCode), String.valueOf(taskId));
    }
}
