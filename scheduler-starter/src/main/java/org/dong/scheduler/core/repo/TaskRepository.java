package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TaskRepository {
    long insert(String taskNo, TaskSubmitRequest request, String extInfo, TaskStatus status);

    Optional<SchedulerTask> findById(Long id);

    Optional<SchedulerTask> findByIdForUpdate(Long id);

    Map<Long, SchedulerTask> findByIds(List<Long> ids);

    Optional<SchedulerTask> findByTaskNo(String taskNo);

    List<Long> findExistingTaskIds(List<Long> taskIds);

    boolean casToRunning(Long id, String expectedGroupCode, int expectedVersion,
                         String instanceId, String threadName, LocalDateTime now);

    boolean casWaitHoldToRunning(Long id, String instanceId, String threadName, LocalDateTime now);

    boolean markSuccess(Long id, LocalDateTime now);

    boolean markFailed(Long id, String errorCode, String errorMsg, LocalDateTime now);

    boolean markFailedByWaitDeadline(SchedulerTask snapshot, String errorCode, String errorMsg, LocalDateTime now);

    boolean markFailedPendingByDependency(Long id, String errorCode, String errorMsg, LocalDateTime now);

    boolean markWaitRetry(Long id, LocalDateTime nextRetryAt, String errorCode, String errorMsg, LocalDateTime now);

    boolean markWaitRetryOnGroup(Long id, LocalDateTime nextRetryAt, String errorCode, String errorMsg,
                                 String sourceGroupCode, String targetGroupCode, LocalDateTime now);

    boolean markWaitHold(Long id, LocalDateTime nextExecuteAt, String extInfo, LocalDateTime now);

    boolean rollbackToWaitHold(Long id, LocalDateTime nextExecuteAt, LocalDateTime now);

    boolean rescheduleToRunnable(Long id, LocalDateTime nextExecuteAt, String errorCode, String errorMsg, LocalDateTime now);

    boolean markCancelledByTaskNo(String taskNo, LocalDateTime now);

    boolean heartbeat(Long id, LocalDateTime now);

    void updateExtInfo(Long id, String extInfo, LocalDateTime now);

    List<SchedulerTask> findRunningHeartbeatTimeout(String groupCode, LocalDateTime cutoff, int limit);

    List<SchedulerTask> findRunnableForQueueRefill(String dispatchRoute, LocalDateTime now, int limit);

    List<SchedulerTask> findPendingForTimeQueueRefill(String dispatchRoute, LocalDateTime now, int limit);

    List<Long> findWaitingTimeoutTaskIds(LocalDateTime now, int limit);

    List<Long> findFallbackDueTaskIds(String dispatchRoute, LocalDateTime now, int limit);

    boolean casRouteFallback(SchedulerTask snapshot, String targetGroupCode,
                             LocalDateTime nextCheckAt, LocalDateTime now);

    boolean casUpdateFallbackCheck(SchedulerTask snapshot, LocalDateTime nextCheckAt, LocalDateTime now,
                                   boolean incrementPolicyCount);

    boolean casFallbackWaitingToFailed(SchedulerTask snapshot, String errorCode, String errorMsg,
                                       LocalDateTime now, boolean incrementPolicyCount);

    void insertGroupFallbackLog(SchedulerTask snapshot, String targetGroupCode,
                                LocalDateTime nextCheckAt, int fallbackCount);

    void insertExecutionGroupSwitchLog(SchedulerTask snapshot, String targetGroupCode, int fallbackCount);

    void promotePendingToRunnable(String dispatchRoute, LocalDateTime now, int limit);

    boolean markRunnableIfPending(Long id, LocalDateTime now);

    boolean markTerminalByBusinessState(Long id, TaskStatus status, LocalDateTime now);

    void insertExecutionStart(SchedulerTask task, String executeNo, String dispatcherInstance, String workerInstance, LocalDateTime now);

    void finishExecution(String executeNo, TaskStatus status, String errorCode, String errorMsg, LocalDateTime now);

    long countRunningByGroup(String groupCode);

    long countRunningByUserInGroup(String groupCode, String userId);

    Map<String, Long> countRunningByUserInGroup(String groupCode);
}
