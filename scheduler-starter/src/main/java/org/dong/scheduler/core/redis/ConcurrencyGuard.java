package org.dong.scheduler.core.redis;

public interface ConcurrencyGuard {
    boolean tryAcquire(String groupCode, String userId, long taskId,
                       int groupMax, int userLimit, int leaseSec,
                       String executeNo);

    void release(String groupCode, String userId, long taskId, String executeNo);

    boolean renewLease(long taskId, String executeNo, int leaseSec);

    void repairRelease(String groupCode, String userId);

    long groupRunning(String groupCode);

    String leaseValue(long taskId);
}
