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

    public static String userRunningPrefix(String group) {
        return "sched:user:running:" + group + ":";
    }

    public static String userRunningPattern(String group) {
        return userRunningPrefix(group) + "*";
    }

    public static String taskLease(long taskId) {
        return "sched:task:lease:" + taskId;
    }

    public static String reconcileLock() {
        return "sched:reconcile:lock";
    }

    public static String groupReconcileThrottle(String group) {
        return "sched:reconcile:throttle:" + group;
    }
}
