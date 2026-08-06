package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class JdbcTaskRepository implements TaskRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcTaskRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public long insert(String taskNo, TaskSubmitRequest request, String extInfo, TaskStatus status) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    insert into scheduler_task(
                        task_no, group_code, dispatch_route, user_id, biz_type, biz_key,
                        status, priority, execute_at, next_retry_at,
                        retry_count, max_retry_count, hold_round_count, hold_max_rounds, hold_retry_delay_sec,
                        execute_timeout_sec, retry_delay_sec, max_wait_sec, wait_deadline_at, fallback_check_at,
                        fallback_policy_count, group_fallback_count, version, ext_info, create_time, update_time
                    ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),now())
                    """, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, taskNo);
            ps.setString(2, request.getGroupCode());
            if (request.getDispatchRoute() == null || request.getDispatchRoute().isBlank()) {
                ps.setObject(3, null);
            } else {
                ps.setString(3, request.getDispatchRoute());
            }
            ps.setString(4, request.getUserId());
            ps.setString(5, request.getBizType());
            ps.setString(6, request.getBizKey());
            ps.setString(7, status.name());
            ps.setInt(8, request.getPriority());
            ps.setTimestamp(9, Timestamp.valueOf(request.getExecuteAt()));
            ps.setTimestamp(10, null);
            ps.setInt(11, 0);
            ps.setInt(12, request.getMaxRetryCount());
            ps.setInt(13, 0);
            ps.setInt(14, request.getHoldMaxRounds());
            ps.setInt(15, request.getHoldRetryDelaySec());
            if (request.getExecuteTimeoutSec() == null) {
                ps.setObject(16, null);
            } else {
                ps.setInt(16, request.getExecuteTimeoutSec());
            }
            if (request.getRetryDelaySec() == null) {
                ps.setObject(17, null);
            } else {
                ps.setInt(17, request.getRetryDelaySec());
            }
            if (request.getMaxWaitSec() == null) {
                ps.setObject(18, null);
            } else {
                ps.setInt(18, request.getMaxWaitSec());
            }
            if (request.getWaitDeadlineAt() == null) {
                ps.setTimestamp(19, null);
            } else {
                ps.setTimestamp(19, Timestamp.valueOf(request.getWaitDeadlineAt()));
            }
            ps.setTimestamp(20, timestamp(request.getFallbackCheckAt()));
            ps.setInt(21, 0);
            ps.setInt(22, 0);
            ps.setInt(23, 0);
            ps.setString(24, extInfo);
            return ps;
        }, keyHolder);
        return Objects.requireNonNull(keyHolder.getKey()).longValue();
    }

    @Override
    public Optional<SchedulerTask> findById(Long id) {
        List<SchedulerTask> list = jdbcTemplate.query("select * from scheduler_task where id = ?", this::mapTask, id);
        return list.stream().findFirst();
    }

    @Override
    public Optional<SchedulerTask> findByIdForUpdate(Long id) {
        List<SchedulerTask> list = jdbcTemplate.query(
                "select * from scheduler_task where id = ? for update", this::mapTask, id);
        return list.stream().findFirst();
    }

    @Override
    public Map<Long, SchedulerTask> findByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Map.of();
        }
        String placeholders = ids.stream().map(id -> "?").collect(Collectors.joining(","));
        List<SchedulerTask> tasks = jdbcTemplate.query(
                "select * from scheduler_task where id in (" + placeholders + ")",
                this::mapTask,
                ids.toArray()
        );
        return tasks.stream().collect(Collectors.toMap(SchedulerTask::getId, task -> task));
    }

    @Override
    public Optional<SchedulerTask> findByTaskNo(String taskNo) {
        List<SchedulerTask> list = jdbcTemplate.query("select * from scheduler_task where task_no = ?", this::mapTask, taskNo);
        return list.stream().findFirst();
    }

    @Override
    public List<Long> findExistingTaskIds(List<Long> taskIds) {
        if (taskIds == null || taskIds.isEmpty()) {
            return List.of();
        }
        String placeholders = taskIds.stream().map(id -> "?").collect(Collectors.joining(","));
        return jdbcTemplate.query(
                "select id from scheduler_task where id in (" + placeholders + ")",
                (rs, rowNum) -> rs.getLong(1),
                taskIds.toArray()
        );
    }

    @Override
    public boolean casToRunning(Long id, String expectedGroupCode, int expectedVersion,
                                String instanceId, String threadName, LocalDateTime now) {
        int updated = jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNING', dispatcher_instance=?, worker_instance=?, worker_thread=?,
                       start_time=?, heartbeat_time=?, update_time=now(), version=version+1
                 where id=? and status in ('RUNNABLE','WAIT_RETRY') and group_code=? and version=?
                """, instanceId, instanceId, threadName, Timestamp.valueOf(now), Timestamp.valueOf(now),
                id, expectedGroupCode, expectedVersion);
        return updated > 0;
    }

    @Override
    public boolean casWaitHoldToRunning(Long id, String instanceId, String threadName, LocalDateTime now) {
        int updated = jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNING', dispatcher_instance=?, worker_instance=?, worker_thread=?,
                       start_time=?, heartbeat_time=?, update_time=now(), version=version+1
                 where id=? and status='WAIT_HOLD'
                """, instanceId, instanceId, threadName, Timestamp.valueOf(now), Timestamp.valueOf(now), id);
        return updated > 0;
    }

    @Override
    public boolean markSuccess(Long id, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='SUCCESS', finish_time=?, fallback_check_at=null, update_time=now(), version=version+1
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(now), id) > 0;
    }

    @Override
    public boolean markFailed(Long id, String errorCode, String errorMsg, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='FAILED', finish_time=?, error_code=?, error_msg=?, fallback_check_at=null,
                       update_time=now(), version=version+1
                 where id=? and status in ('RUNNING','WAIT_HOLD','WAIT_RETRY','RUNNABLE')
                """, Timestamp.valueOf(now), errorCode, errorMsg, id) > 0;
    }

    @Override
    public boolean markFailedByWaitDeadline(SchedulerTask snapshot, String errorCode, String errorMsg,
                                            LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='FAILED', finish_time=?, error_code=?, error_msg=?, fallback_check_at=null,
                       update_time=now(), version=version+1
                 where id=? and status in ('PENDING','RUNNABLE','WAIT_RETRY')
                   and status=? and version=?
                   and wait_deadline_at is not null and wait_deadline_at <= ?
                """, Timestamp.valueOf(now), errorCode, errorMsg, snapshot.getId(), snapshot.getStatus().name(),
                snapshot.getVersion(), Timestamp.valueOf(now)) > 0;
    }

    @Override
    public boolean markFailedPendingByDependency(Long id, String errorCode, String errorMsg, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='FAILED', finish_time=?, error_code=?, error_msg=?, fallback_check_at=null,
                       update_time=now(), version=version+1
                 where id=? and status='PENDING'
                """, Timestamp.valueOf(now), errorCode, errorMsg, id) > 0;
    }

    @Override
    public boolean markWaitRetry(Long id, LocalDateTime nextRetryAt, String errorCode, String errorMsg, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='WAIT_RETRY', retry_count=retry_count+1, next_retry_at=?, execute_at=?,
                       error_code=?, error_msg=?, update_time=now(), version=version+1
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(nextRetryAt), Timestamp.valueOf(nextRetryAt), errorCode, errorMsg, id) > 0;
    }

    @Override
    public boolean markWaitRetryOnGroup(Long id, LocalDateTime nextRetryAt, String errorCode, String errorMsg,
                                        String sourceGroupCode, String targetGroupCode, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='WAIT_RETRY', group_code=?, retry_count=retry_count+1, next_retry_at=?, execute_at=?,
                       error_code=?, error_msg=?, group_fallback_count=group_fallback_count+1,
                       update_time=now(), version=version+1
                 where id=? and status='RUNNING' and group_code=?
                """, targetGroupCode, Timestamp.valueOf(nextRetryAt), Timestamp.valueOf(nextRetryAt),
                errorCode, errorMsg, id, sourceGroupCode) > 0;
    }

    @Override
    public boolean markWaitHold(Long id, LocalDateTime nextExecuteAt, String extInfo, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='WAIT_HOLD', hold_round_count=hold_round_count+1, execute_at=?, next_retry_at=null,
                       error_code=null, error_msg=null, ext_info=?, worker_instance=null, worker_thread=null,
                       heartbeat_time=null, update_time=now(), version=version+1
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(nextExecuteAt), extInfo, id) > 0;
    }

    @Override
    public boolean rollbackToWaitHold(Long id, LocalDateTime nextExecuteAt, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='WAIT_HOLD', execute_at=?, next_retry_at=null,
                       worker_instance=null, worker_thread=null, heartbeat_time=null,
                       update_time=now(), version=version+1
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(nextExecuteAt), id) > 0;
    }

    @Override
    public boolean rescheduleToRunnable(Long id, LocalDateTime nextExecuteAt, String errorCode, String errorMsg, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNABLE', execute_at=?, next_retry_at=?,
                       error_code=?, error_msg=?, worker_instance=null, worker_thread=null,
                       heartbeat_time=null, start_time=null, update_time=now(), version=version+1
                 where id=? and status in ('RUNNING','RUNNABLE','WAIT_RETRY')
                """, Timestamp.valueOf(nextExecuteAt), Timestamp.valueOf(nextExecuteAt), errorCode, errorMsg, id) > 0;
    }

    @Override
    public boolean markCancelledByTaskNo(String taskNo, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='CANCELLED', finish_time=?, fallback_check_at=null, update_time=now(), version=version+1
                 where task_no=? and status in ('PENDING','RUNNABLE','WAIT_RETRY','WAIT_HOLD')
                """, Timestamp.valueOf(now), taskNo) > 0;
    }

    @Override
    public boolean heartbeat(Long id, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set heartbeat_time=?, update_time=now()
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(now), id) > 0;
    }

    @Override
    public void updateExtInfo(Long id, String extInfo, LocalDateTime now) {
        jdbcTemplate.update("""
                update scheduler_task
                   set ext_info=?, update_time=?, version=version+1
                 where id=?
                """, extInfo, Timestamp.valueOf(now), id);
    }

    @Override
    public List<SchedulerTask> findRunningHeartbeatTimeout(String groupCode, LocalDateTime cutoff, int limit) {
        return jdbcTemplate.query("""
                select * from scheduler_task
                 where group_code=? and status='RUNNING' and heartbeat_time < ?
                 order by heartbeat_time asc limit ?
                """, this::mapTask, groupCode, Timestamp.valueOf(cutoff), limit);
    }

    @Override
    public List<SchedulerTask> findRunnableForQueueRefill(String dispatchRoute, LocalDateTime now, int limit) {
        if (dispatchRoute == null || dispatchRoute.isBlank()) {
            return jdbcTemplate.query("""
                    select * from scheduler_task
                     where dispatch_route is null
                       and status in ('RUNNABLE','WAIT_RETRY','WAIT_HOLD')
                     order by execute_at asc limit ?
                    """, this::mapTask, limit);
        }
        return jdbcTemplate.query("""
                select * from scheduler_task
                 where dispatch_route = ?
                   and status in ('RUNNABLE','WAIT_RETRY','WAIT_HOLD')
                 order by execute_at asc limit ?
                """, this::mapTask, dispatchRoute, limit);
    }

    @Override
    public List<SchedulerTask> findPendingForTimeQueueRefill(String dispatchRoute, LocalDateTime now, int limit) {
        if (dispatchRoute == null || dispatchRoute.isBlank()) {
            return jdbcTemplate.query("""
                    select * from scheduler_task
                     where dispatch_route is null
                       and status = 'PENDING'
                       and execute_at > ?
                     order by execute_at asc
                     limit ?
                    """, this::mapTask, Timestamp.valueOf(now), limit);
        }
        return jdbcTemplate.query("""
                select * from scheduler_task
                 where dispatch_route = ?
                   and status = 'PENDING'
                   and execute_at > ?
                 order by execute_at asc
                 limit ?
                """, this::mapTask, dispatchRoute, Timestamp.valueOf(now), limit);
    }

    @Override
    public List<Long> findWaitingTimeoutTaskIds(LocalDateTime now, int limit) {
        return jdbcTemplate.query("""
                select id from scheduler_task
                 where status in ('PENDING','RUNNABLE','WAIT_RETRY')
                   and wait_deadline_at is not null
                   and wait_deadline_at <= ?
                 order by wait_deadline_at asc
                 limit ?
                """, (rs, rowNum) -> rs.getLong(1), Timestamp.valueOf(now), limit);
    }

    @Override
    public void promotePendingToRunnable(String dispatchRoute, LocalDateTime now, int limit) {
        if (dispatchRoute == null || dispatchRoute.isBlank()) {
            jdbcTemplate.update("""
                    update scheduler_task
                       set status='RUNNABLE', update_time=now(), version=version+1
                     where id in (
                        select id from (
                            select id from scheduler_task
                             where dispatch_route is null
                               and status='PENDING'
                               and execute_at <= ?
                               and not exists (
                                   select 1 from scheduler_task_dependency d
                                    where d.task_id = scheduler_task.id
                                      and d.status in ('WAITING', 'IMPOSSIBLE')
                               )
                             order by execute_at asc
                             limit ?
                        ) t
                     )
                    """, Timestamp.valueOf(now), limit);
            return;
        }
        jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNABLE', update_time=now(), version=version+1
                 where id in (
                    select id from (
                        select id from scheduler_task
                         where dispatch_route = ?
                           and status='PENDING'
                           and execute_at <= ?
                           and not exists (
                               select 1 from scheduler_task_dependency d
                                where d.task_id = scheduler_task.id
                                  and d.status in ('WAITING', 'IMPOSSIBLE')
                           )
                         order by execute_at asc
                         limit ?
                    ) t
                 )
                """, dispatchRoute, Timestamp.valueOf(now), limit);
    }

    @Override
    public List<Long> findFallbackDueTaskIds(String dispatchRoute, LocalDateTime now, int limit) {
        if (dispatchRoute == null || dispatchRoute.isBlank()) {
            return jdbcTemplate.query("""
                    select id from scheduler_task
                     where dispatch_route is null
                       and status in ('PENDING','RUNNABLE','WAIT_RETRY')
                       and fallback_check_at is not null and fallback_check_at <= ?
                     order by fallback_check_at asc, id asc limit ?
                    """, (rs, rowNum) -> rs.getLong(1), Timestamp.valueOf(now), limit);
        }
        return jdbcTemplate.query("""
                select id from scheduler_task
                 where dispatch_route = ?
                   and status in ('PENDING','RUNNABLE','WAIT_RETRY')
                   and fallback_check_at is not null and fallback_check_at <= ?
                 order by fallback_check_at asc, id asc limit ?
                """, (rs, rowNum) -> rs.getLong(1), dispatchRoute, Timestamp.valueOf(now), limit);
    }

    @Override
    public boolean casRouteFallback(SchedulerTask snapshot, String targetGroupCode,
                                    LocalDateTime nextCheckAt, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set group_code=?, fallback_check_at=?, fallback_policy_count=fallback_policy_count+1,
                       group_fallback_count=group_fallback_count+1, update_time=?, version=version+1
                 where id=? and status in ('PENDING','RUNNABLE','WAIT_RETRY')
                   and status=? and group_code=? and fallback_check_at=? and version=?
                   and (wait_deadline_at is null or wait_deadline_at > ?)
                """, targetGroupCode, timestamp(nextCheckAt), Timestamp.valueOf(now), snapshot.getId(),
                snapshot.getStatus().name(), snapshot.getGroupCode(), timestamp(snapshot.getFallbackCheckAt()),
                snapshot.getVersion(), Timestamp.valueOf(now)) > 0;
    }

    @Override
    public boolean casUpdateFallbackCheck(SchedulerTask snapshot, LocalDateTime nextCheckAt, LocalDateTime now,
                                          boolean incrementPolicyCount) {
        String countUpdate = incrementPolicyCount
                ? "fallback_policy_count=fallback_policy_count+1," : "";
        return jdbcTemplate.update("update scheduler_task set fallback_check_at=?, " + countUpdate
                        + " update_time=?, version=version+1"
                        + " where id=? and status in ('PENDING','RUNNABLE','WAIT_RETRY')"
                        + " and status=? and group_code=? and fallback_check_at=? and version=?"
                        + " and (wait_deadline_at is null or wait_deadline_at > ?)",
                timestamp(nextCheckAt), Timestamp.valueOf(now), snapshot.getId(), snapshot.getStatus().name(),
                snapshot.getGroupCode(), timestamp(snapshot.getFallbackCheckAt()), snapshot.getVersion(),
                Timestamp.valueOf(now)) > 0;
    }

    @Override
    public boolean casFallbackWaitingToFailed(SchedulerTask snapshot, String errorCode, String errorMsg,
                                              LocalDateTime now, boolean incrementPolicyCount) {
        String countUpdate = incrementPolicyCount
                ? "fallback_policy_count=fallback_policy_count+1," : "";
        return jdbcTemplate.update("update scheduler_task set status='FAILED', finish_time=?, error_code=?,"
                        + " error_msg=?, fallback_check_at=null, " + countUpdate
                        + " update_time=?, version=version+1"
                        + " where id=? and status in ('PENDING','RUNNABLE','WAIT_RETRY')"
                        + " and status=? and group_code=? and fallback_check_at=? and version=?"
                        + " and (wait_deadline_at is null or wait_deadline_at > ?)",
                Timestamp.valueOf(now), errorCode, errorMsg, Timestamp.valueOf(now), snapshot.getId(),
                snapshot.getStatus().name(), snapshot.getGroupCode(), timestamp(snapshot.getFallbackCheckAt()),
                snapshot.getVersion(), Timestamp.valueOf(now)) > 0;
    }

    @Override
    public void insertGroupFallbackLog(SchedulerTask snapshot, String targetGroupCode,
                                       LocalDateTime nextCheckAt, int fallbackCount) {
        jdbcTemplate.update("""
                insert into scheduler_task_group_fallback_log(
                    task_id, task_no, source_group_code, target_group_code,
                    previous_fallback_check_at, next_fallback_check_at, task_status, switch_source, fallback_count
                ) values(?,?,?,?,?,?,?,?,?)
                """, snapshot.getId(), snapshot.getTaskNo(), snapshot.getGroupCode(), targetGroupCode,
                timestamp(snapshot.getFallbackCheckAt()), timestamp(nextCheckAt), snapshot.getStatus().name(),
                "NON_RUNNING_FALLBACK", fallbackCount);
    }

    @Override
    public void insertExecutionGroupSwitchLog(SchedulerTask snapshot, String targetGroupCode, int fallbackCount) {
        jdbcTemplate.update("""
                insert into scheduler_task_group_fallback_log(
                    task_id, task_no, source_group_code, target_group_code,
                    previous_fallback_check_at, next_fallback_check_at, task_status, switch_source, fallback_count
                ) values(?,?,?,?,?,?,?,?,?)
                """, snapshot.getId(), snapshot.getTaskNo(), snapshot.getGroupCode(), targetGroupCode,
                null, null, snapshot.getStatus().name(), "EXECUTION_RETRY", fallbackCount);
    }

    @Override
    public boolean markRunnableIfPending(Long id, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNABLE', update_time=now(), version=version+1
                 where id=? and status='PENDING' and execute_at <= ?
                   and not exists (
                       select 1 from scheduler_task_dependency d
                        where d.task_id = scheduler_task.id
                          and d.status in ('WAITING', 'IMPOSSIBLE')
                   )
                """, id, Timestamp.valueOf(now)) > 0;
    }

    @Override
    public boolean markTerminalByBusinessState(Long id, TaskStatus status, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status=?, finish_time=?, fallback_check_at=null, update_time=now(), version=version+1
                 where id=? and status in ('RUNNABLE','WAIT_RETRY','WAIT_HOLD','RUNNING','PENDING')
                """, status.name(), Timestamp.valueOf(now), id) > 0;
    }

    @Override
    public void insertExecutionStart(SchedulerTask task, String executeNo, String dispatcherInstance, String workerInstance, LocalDateTime now) {
        jdbcTemplate.update("""
                insert into scheduler_task_execution(
                    task_id, task_no, group_code, user_id, execute_no, status,
                    dispatcher_instance, worker_instance, start_time, create_time, update_time
                ) values(?,?,?,?,?,'RUNNING',?,?,?,now(),now())
                """, task.getId(), task.getTaskNo(), task.getGroupCode(), task.getUserId(), executeNo,
                dispatcherInstance, workerInstance, Timestamp.valueOf(now));
    }

    @Override
    public void finishExecution(String executeNo, TaskStatus status, String errorCode, String errorMsg, LocalDateTime now) {
        jdbcTemplate.update("""
                update scheduler_task_execution
                   set status=?, finish_time=?, duration_ms=timestampdiff(microsecond, start_time, ?)/1000,
                       error_code=?, error_msg=?, update_time=now()
                 where execute_no=?
                """, status.name(), Timestamp.valueOf(now), Timestamp.valueOf(now), errorCode, errorMsg, executeNo);
    }

    @Override
    public long countRunningByGroup(String groupCode) {
        Long count = jdbcTemplate.queryForObject("""
                select count(1) from scheduler_task where group_code=? and status in ('RUNNING','WAIT_HOLD')
                """, Long.class, groupCode);
        return count == null ? 0L : count;
    }

    @Override
    public long countRunningByUserInGroup(String groupCode, String userId) {
        Long count = jdbcTemplate.queryForObject("""
                select count(1) from scheduler_task where group_code=? and user_id=? and status in ('RUNNING','WAIT_HOLD')
                """, Long.class, groupCode, userId);
        return count == null ? 0L : count;
    }

    @Override
    public Map<String, Long> countRunningByUserInGroup(String groupCode) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                select user_id, count(1) as cnt
                  from scheduler_task
                 where group_code=? and status in ('RUNNING','WAIT_HOLD')
                 group by user_id
                """, groupCode);
        Map<String, Long> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String userId = (String) row.get("user_id");
            Number cnt = (Number) row.get("cnt");
            result.put(userId, cnt == null ? 0L : cnt.longValue());
        }
        return result;
    }

    private SchedulerTask mapTask(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        SchedulerTask task = new SchedulerTask();
        task.setId(rs.getLong("id"));
        task.setTaskNo(rs.getString("task_no"));
        task.setGroupCode(rs.getString("group_code"));
        task.setDispatchRoute(rs.getString("dispatch_route"));
        task.setUserId(rs.getString("user_id"));
        task.setBizType(rs.getString("biz_type"));
        task.setBizKey(rs.getString("biz_key"));
        task.setStatus(TaskStatus.valueOf(rs.getString("status")));
        task.setPriority(rs.getInt("priority"));
        task.setExecuteAt(tsToLdt(rs.getTimestamp("execute_at")));
        task.setNextRetryAt(tsToLdt(rs.getTimestamp("next_retry_at")));
        task.setRetryCount(rs.getInt("retry_count"));
        task.setMaxRetryCount(rs.getInt("max_retry_count"));
        task.setHoldRoundCount(rs.getInt("hold_round_count"));
        task.setHoldMaxRounds(rs.getInt("hold_max_rounds"));
        task.setHoldRetryDelaySec(rs.getInt("hold_retry_delay_sec"));
        task.setExecuteTimeoutSec((Integer) rs.getObject("execute_timeout_sec"));
        task.setRetryDelaySec((Integer) rs.getObject("retry_delay_sec"));
        task.setMaxWaitSec((Integer) rs.getObject("max_wait_sec"));
        task.setWaitDeadlineAt(tsToLdt(rs.getTimestamp("wait_deadline_at")));
        task.setFallbackCheckAt(tsToLdt(rs.getTimestamp("fallback_check_at")));
        task.setFallbackPolicyCount(rs.getInt("fallback_policy_count"));
        task.setGroupFallbackCount(rs.getInt("group_fallback_count"));
        task.setDispatcherInstance(rs.getString("dispatcher_instance"));
        task.setWorkerInstance(rs.getString("worker_instance"));
        task.setWorkerThread(rs.getString("worker_thread"));
        task.setHeartbeatTime(tsToLdt(rs.getTimestamp("heartbeat_time")));
        task.setStartTime(tsToLdt(rs.getTimestamp("start_time")));
        task.setFinishTime(tsToLdt(rs.getTimestamp("finish_time")));
        task.setVersion(rs.getInt("version"));
        task.setErrorCode(rs.getString("error_code"));
        task.setErrorMsg(rs.getString("error_msg"));
        task.setExtInfo(rs.getString("ext_info"));
        task.setCreateTime(tsToLdt(rs.getTimestamp("create_time")));
        task.setUpdateTime(tsToLdt(rs.getTimestamp("update_time")));
        return task;
    }

    private LocalDateTime tsToLdt(Timestamp ts) {
        return ts == null ? null : ts.toLocalDateTime();
    }

    private Timestamp timestamp(LocalDateTime value) {
        return value == null ? null : Timestamp.valueOf(value);
    }
}
