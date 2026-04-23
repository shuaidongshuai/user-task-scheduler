package org.dong.scheduler.core.redis;

public final class RedisKeys {
    private RedisKeys() {
    }

    public static String timeQueue(String group) {
        return "sched:queue:time:" + group;
    }

    public static String readyQueue(String group) {
        return "sched:queue:ready:" + group;
    }

    public static String groupRunning(String group) {
        return "sched:group:running:" + group;
    }

    public static String userRunning(String group, String userId) {
        return "sched:user:running:" + group + ":" + userId;
    }

    public static String taskLease(long taskId) {
        return "sched:task:lease:" + taskId;
    }
}
