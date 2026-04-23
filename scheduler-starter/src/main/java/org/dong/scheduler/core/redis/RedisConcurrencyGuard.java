package org.dong.scheduler.core.redis;

import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;

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
            if groupRunning > 0 then redis.call('DECR', KEYS[1]) end
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            if userRunning > 0 then redis.call('DECR', KEYS[2]) end
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
            if groupRunning > 0 then redis.call('DECR', KEYS[1]) end
            local userRunning = tonumber(redis.call('GET', KEYS[2]) or '0')
            if userRunning > 0 then redis.call('DECR', KEYS[2]) end
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
    public void release(String groupCode, String userId, long taskId, String executeNo) {
        redisTemplate.execute(connection -> connection.scriptingCommands().eval(
                RELEASE_SCRIPT.getBytes(StandardCharsets.UTF_8), ReturnType.INTEGER, 3,
                RedisKeys.groupRunning(groupCode).getBytes(StandardCharsets.UTF_8),
                RedisKeys.userRunning(groupCode, userId).getBytes(StandardCharsets.UTF_8),
                RedisKeys.taskLease(taskId).getBytes(StandardCharsets.UTF_8),
                executeNo.getBytes(StandardCharsets.UTF_8)
        ), true);
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
    public String leaseValue(long taskId) {
        return redisTemplate.opsForValue().get(RedisKeys.taskLease(taskId));
    }
}
