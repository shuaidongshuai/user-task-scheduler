package org.dong.scheduler.core.redis;

import java.util.Map;

public interface ConcurrencyGuard {
    boolean tryAcquire(String groupCode, String userId, long taskId,
                       int groupMax, int userLimit, int leaseSec,
                       String executeNo);

    boolean release(String groupCode, String userId, long taskId, String executeNo);

    boolean renewLease(long taskId, String executeNo, int leaseSec);

    void repairRelease(String groupCode, String userId);

    long groupRunning(String groupCode);

    long userRunning(String groupCode, String userId);

    Map<String, Long> listUserRunning(String groupCode);

    void setGroupRunning(String groupCode, long running);

    void setUserRunning(String groupCode, String userId, long running);

    long reduceUserAndGroupRunning(String groupCode, String userId, long delta);

    boolean tryAcquireReconcileLock(String owner, int lockSec);

    boolean tryAcquireGroupReconcileThrottle(String groupCode, int throttleSec);

    void releaseReconcileLock(String owner);

    void syncRunningCounters(String groupCode, long groupRunning, Map<String, Long> userRunningByUserId);

    String leaseValue(long taskId);
}
