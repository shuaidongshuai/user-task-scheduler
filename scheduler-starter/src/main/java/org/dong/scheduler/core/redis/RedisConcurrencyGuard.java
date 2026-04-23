package org.dong.scheduler.core.redis;

import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisConcurrencyGuard implements ConcurrencyGuard {
    private static final String ACQUIRE_SCRIPT = """
            local groupRunning = tonumber(redis.call('GET', KEYS[1]) or '0')
            if groupRunning >= tonumber(ARGV[1]) then
                return 0
            end
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            if userRunning >= tonumber(ARGV[2]) then
                return 0
            end
            if redis.call('SET', KEYS[3], ARGV[3], 'NX', 'EX', ARGV[4]) == false then
                return 0
            end
            redis.call('INCR', KEYS[1])
            redis.call('INCR', KEYS[2])
            return 1
            """;

    private static final String RELEASE_SCRIPT = """
            local lease = redis.call('GET', KEYS[3])
            if lease ~= ARGV[1] then
                return 0
            end
            redis.call('DEL', KEYS[3])
            local groupRunning = tonumber(redis.call('GET', KEYS[1]) or '0')
            if groupRunning > 0 then
                local groupAfter = redis.call('DECR', KEYS[1])
                if tonumber(groupAfter) <= 0 then redis.call('DEL', KEYS[1]) end
            end
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            if userRunning > 0 then
                local userAfter = redis.call('DECR', KEYS[2])
                if tonumber(userAfter) <= 0 then redis.call('DEL', KEYS[2]) end
            end
            return 1
            """;
    private static final String RENEW_SCRIPT = """
            local lease = redis.call('GET', KEYS[1])
            if lease ~= ARGV[1] then
                return 0
            end
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return 1
            """;
    private static final String REPAIR_RELEASE_SCRIPT = """
            local groupRunning = tonumber(redis.call('GET', KEYS[1]) or '0')
            if groupRunning > 0 then
                local groupAfter = redis.call('DECR', KEYS[1])
                if tonumber(groupAfter) <= 0 then redis.call('DEL', KEYS[1]) end
            end
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            if userRunning > 0 then
                local userAfter = redis.call('DECR', KEYS[2])
                if tonumber(userAfter) <= 0 then redis.call('DEL', KEYS[2]) end
            end
            return 1
            """;
    private static final String REDUCE_USER_AND_GROUP_SCRIPT = """
            local groupRunning = tonumber(redis.call('GET', KEYS[1]) or '0')
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            local delta = tonumber(ARGV[1]) or 0
            if delta <= 0 then
                return 0
            end
            local reduce = delta
            if reduce > groupRunning then
                reduce = groupRunning
            end
            if reduce > userRunning then
                reduce = userRunning
            end
            if reduce <= 0 then
                return 0
            end
            local groupAfter = groupRunning - reduce
            local userAfter = userRunning - reduce
            if groupAfter <= 0 then
                redis.call('DEL', KEYS[1])
            else
                redis.call('SET', KEYS[1], groupAfter)
            end
            if userAfter <= 0 then
                redis.call('DEL', KEYS[2])
            else
                redis.call('SET', KEYS[2], userAfter)
            end
            return reduce
            """;
    private static final String RELEASE_RECONCILE_LOCK_SCRIPT = """
            local v = redis.call('GET', KEYS[1])
            if v ~= ARGV[1] then
                return 0
            end
            redis.call('DEL', KEYS[1])
            return 1
            """;

    private final StringRedisTemplate redisTemplate;

    public RedisConcurrencyGuard(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean tryAcquire(String groupCode, String userId, long taskId,
                              int groupMax, int userLimit, int leaseSec,
                              String executeNo) {
        Long result = redisTemplate.execute(connection -> (Long) connection.scriptingCommands().eval(
                        ACQUIRE_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 3,
                        RedisKeys.groupRunning(groupCode).getBytes(StandardCharsets.UTF_8),
                        RedisKeys.userRunning(groupCode, userId).getBytes(StandardCharsets.UTF_8),
                        RedisKeys.taskLease(taskId).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(groupMax).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(userLimit).getBytes(StandardCharsets.UTF_8),
                        executeNo.getBytes(StandardCharsets.UTF_8),
                        String.valueOf(leaseSec).getBytes(StandardCharsets.UTF_8)
                ), true);
        return result != null && result == 1L;
    }

    @Override
    public boolean release(String groupCode, String userId, long taskId, String executeNo) {
        Long result = redisTemplate.execute(connection -> (Long) connection.scriptingCommands().eval(
                RELEASE_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 3,
                RedisKeys.groupRunning(groupCode).getBytes(StandardCharsets.UTF_8),
                RedisKeys.userRunning(groupCode, userId).getBytes(StandardCharsets.UTF_8),
                RedisKeys.taskLease(taskId).getBytes(StandardCharsets.UTF_8),
                executeNo.getBytes(StandardCharsets.UTF_8)
        ), true);
        return result != null && result == 1L;
    }

    @Override
    public boolean renewLease(long taskId, String executeNo, int leaseSec) {
        Long result = redisTemplate.execute(connection -> (Long) connection.scriptingCommands().eval(
                RENEW_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 1,
                RedisKeys.taskLease(taskId).getBytes(StandardCharsets.UTF_8),
                executeNo.getBytes(StandardCharsets.UTF_8),
                String.valueOf(leaseSec).getBytes(StandardCharsets.UTF_8)
        ), true);
        return result != null && result == 1L;
    }

    @Override
    public void repairRelease(String groupCode, String userId) {
        redisTemplate.execute(connection -> connection.scriptingCommands().eval(
                REPAIR_RELEASE_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 2,
                RedisKeys.groupRunning(groupCode).getBytes(StandardCharsets.UTF_8),
                RedisKeys.userRunning(groupCode, userId).getBytes(StandardCharsets.UTF_8)
        ), true);
    }

    @Override
    public long groupRunning(String groupCode) {
        String value = redisTemplate.opsForValue().get(RedisKeys.groupRunning(groupCode));
        return value == null ? 0L : Long.parseLong(value);
    }

    @Override
    public long userRunning(String groupCode, String userId) {
        String value = redisTemplate.opsForValue().get(RedisKeys.userRunning(groupCode, userId));
        return value == null ? 0L : Long.parseLong(value);
    }

    @Override
    public Map<String, Long> listUserRunning(String groupCode) {
        Set<String> keys = redisTemplate.keys(RedisKeys.userRunningPattern(groupCode));
        if (keys == null || keys.isEmpty()) {
            return Map.of();
        }
        Map<String, Long> result = new HashMap<>();
        String prefix = RedisKeys.userRunningPrefix(groupCode);
        for (String key : keys) {
            String userId = key.startsWith(prefix) ? key.substring(prefix.length()) : null;
            if (userId == null || userId.isBlank()) {
                continue;
            }
            String value = redisTemplate.opsForValue().get(key);
            long running = value == null ? 0L : Long.parseLong(value);
            result.put(userId, running);
        }
        return result;
    }

    @Override
    public void setGroupRunning(String groupCode, long running) {
        if (running <= 0) {
            redisTemplate.delete(RedisKeys.groupRunning(groupCode));
            return;
        }
        redisTemplate.opsForValue().set(RedisKeys.groupRunning(groupCode), String.valueOf(running));
    }

    @Override
    public void setUserRunning(String groupCode, String userId, long running) {
        String key = RedisKeys.userRunning(groupCode, userId);
        if (running <= 0) {
            redisTemplate.delete(key);
            return;
        }
        redisTemplate.opsForValue().set(key, String.valueOf(running));
    }

    @Override
    public long reduceUserAndGroupRunning(String groupCode, String userId, long delta) {
        Long reduced = redisTemplate.execute(connection -> (Long) connection.scriptingCommands().eval(
                REDUCE_USER_AND_GROUP_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 2,
                RedisKeys.groupRunning(groupCode).getBytes(StandardCharsets.UTF_8),
                RedisKeys.userRunning(groupCode, userId).getBytes(StandardCharsets.UTF_8),
                String.valueOf(delta).getBytes(StandardCharsets.UTF_8)
        ), true);
        return reduced == null ? 0L : reduced;
    }

    @Override
    public boolean tryAcquireReconcileLock(String owner, int lockSec) {
        Boolean ok = redisTemplate.opsForValue().setIfAbsent(
                RedisKeys.reconcileLock(), owner, Duration.ofSeconds(Math.max(1, lockSec))
        );
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void releaseReconcileLock(String owner) {
        redisTemplate.execute(connection -> connection.scriptingCommands().eval(
                RELEASE_RECONCILE_LOCK_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 1,
                RedisKeys.reconcileLock().getBytes(StandardCharsets.UTF_8),
                owner.getBytes(StandardCharsets.UTF_8)
        ), true);
    }

    @Override
    public void syncRunningCounters(String groupCode, long groupRunning, Map<String, Long> userRunningByUserId) {
        setGroupRunning(groupCode, groupRunning);

        Map<String, Long> existing = listUserRunning(groupCode);
        for (Map.Entry<String, Long> entry : existing.entrySet()) {
            if (!userRunningByUserId.containsKey(entry.getKey())) {
                redisTemplate.delete(RedisKeys.userRunning(groupCode, entry.getKey()));
            }
        }
        for (Map.Entry<String, Long> entry : userRunningByUserId.entrySet()) {
            if (entry.getValue() <= 0) {
                setUserRunning(groupCode, entry.getKey(), 0L);
                continue;
            }
            setUserRunning(groupCode, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String leaseValue(long taskId) {
        return redisTemplate.opsForValue().get(RedisKeys.taskLease(taskId));
    }
}
