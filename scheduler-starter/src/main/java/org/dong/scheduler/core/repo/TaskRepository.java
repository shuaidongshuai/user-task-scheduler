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

    Optional<SchedulerTask> findByTaskNo(String taskNo);

    Optional<SchedulerTask> findByBizTypeAndBizKey(String bizType, String bizKey);

    boolean casToRunning(Long id, String instanceId, String threadName, LocalDateTime now);

    boolean markSuccess(Long id, LocalDateTime now);

    boolean markFailed(Long id, String errorCode, String errorMsg, LocalDateTime now);

    boolean markWaitRetry(Long id, LocalDateTime nextRetryAt, String errorCode, String errorMsg, LocalDateTime now);

    boolean rescheduleToRunnable(Long id, LocalDateTime nextExecuteAt, String errorCode, String errorMsg, LocalDateTime now);

    boolean markCancelledByTaskNo(String taskNo, LocalDateTime now);

    boolean heartbeat(Long id, LocalDateTime now);

    void updateExtInfo(Long id, String extInfo, LocalDateTime now);

    List<SchedulerTask> findRunningHeartbeatTimeout(String groupCode, LocalDateTime cutoff, int limit);

    List<SchedulerTask> findRunnableForQueueRefill(LocalDateTime now, int limit);

    void promotePendingToRunnable(LocalDateTime now, int limit);

    boolean markRunnableIfPending(Long id, LocalDateTime now);

    void markTerminalByBusinessState(Long id, TaskStatus status, LocalDateTime now);

    void insertExecutionStart(SchedulerTask task, String executeNo, String dispatcherInstance, String workerInstance, LocalDateTime now);

    void finishExecution(String executeNo, TaskStatus status, String errorCode, String errorMsg, LocalDateTime now);

    long countRunningByGroup(String groupCode);

    Map<String, Long> countRunningByUserInGroup(String groupCode);
}
