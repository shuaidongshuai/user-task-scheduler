package org.dong.scheduler.core.redis;

public final class RedisKeys {
    private RedisKeys() {
    }

    public static String timeQueue(String group) {
        return "sched:queue:time:" + group;
    }

    public static String timeQueue(String group, String route) {
        if (route == null || route.isBlank()) {
            return timeQueue(group);
        }
        return "sched:queue:time:" + group + ":" + route;
    }

    public static String readyQueue(String group) {
        return "sched:queue:ready:" + group;
    }

    public static String readyQueue(String group, String route) {
        if (route == null || route.isBlank()) {
            return readyQueue(group);
        }
        return "sched:queue:ready:" + group + ":" + route;
    }

    public static String activeUsers(String group, String route) {
        if (route == null || route.isBlank()) {
            return "sched:active-users:" + group;
        }
        return "sched:active-users:" + group + ":" + route;
    }

    public static String userReadyQueue(String group, String route, String userId) {
        if (route == null || route.isBlank()) {
            return "sched:ready:user:" + group + ":" + userId;
        }
        return "sched:ready:user:" + group + ":" + route + ":" + userId;
    }

    public static String activeUserLock(String group, String route, String userId) {
        if (route == null || route.isBlank()) {
            return "sched:active-user-lock:" + group + ":" + userId;
        }
        return "sched:active-user-lock:" + group + ":" + route + ":" + userId;
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

    public static String jobLock(String jobName) {
        return "sched:job:lock:" + jobName;
    }

    public static String groupReconcileThrottle(String group) {
        return "sched:reconcile:throttle:" + group;
    }
}
