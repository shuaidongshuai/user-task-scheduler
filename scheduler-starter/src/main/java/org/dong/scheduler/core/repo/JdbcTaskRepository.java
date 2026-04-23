package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class JdbcTaskRepository implements TaskRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcTaskRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public long insert(TaskSubmitRequest request, String extJson, TaskStatus status) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement("""
                        insert into scheduler_task(
                            task_no, group_code, user_id, biz_type, biz_key,
                            status, priority, execute_at, next_retry_at,
                            retry_count, max_retry_count, execute_timeout_sec, retry_delay_sec,
                            version, ext_json, create_time, update_time
                        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),now())
                        """, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, request.getTaskNo());
                ps.setString(2, request.getGroupCode());
                ps.setString(3, request.getUserId());
                ps.setString(4, request.getBizType());
                ps.setString(5, request.getBizKey());
                ps.setString(6, status.name());
                ps.setInt(7, request.getPriority());
                ps.setTimestamp(8, Timestamp.valueOf(request.getExecuteAt()));
                ps.setTimestamp(9, null);
                ps.setInt(10, 0);
                ps.setInt(11, request.getMaxRetryCount());
                if (request.getExecuteTimeoutSec() == null) {
                    ps.setObject(12, null);
                } else {
                    ps.setInt(12, request.getExecuteTimeoutSec());
                }
                if (request.getRetryDelaySec() == null) {
                    ps.setObject(13, null);
                } else {
                    ps.setInt(13, request.getRetryDelaySec());
                }
                ps.setInt(14, 0);
                ps.setString(15, extJson);
                return ps;
            }, keyHolder);
        } catch (DuplicateKeyException e) {
            return findByTaskNo(request.getTaskNo()).map(SchedulerTask::getId)
                    .orElseThrow(() -> e);
        }
        return Objects.requireNonNull(keyHolder.getKey()).longValue();
    }

    @Override
    public Optional<SchedulerTask> findById(Long id) {
        List<SchedulerTask> list = jdbcTemplate.query("select * from scheduler_task where id = ?", this::mapTask, id);
        return list.stream().findFirst();
    }

    @Override
    public Optional<SchedulerTask> findByTaskNo(String taskNo) {
        List<SchedulerTask> list = jdbcTemplate.query("select * from scheduler_task where task_no = ?", this::mapTask, taskNo);
        return list.stream().findFirst();
    }

    @Override
    public boolean casToRunning(Long id, String instanceId, String threadName, LocalDateTime now) {
        int updated = jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNING', dispatcher_instance=?, worker_instance=?, worker_thread=?,
                       start_time=?, heartbeat_time=?, update_time=now(), version=version+1
                 where id=? and status in ('RUNNABLE','WAIT_RETRY')
                """, instanceId, instanceId, threadName, Timestamp.valueOf(now), Timestamp.valueOf(now), id);
        return updated > 0;
    }

    @Override
    public boolean markSuccess(Long id, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='SUCCESS', finish_time=?, update_time=now(), version=version+1
                 where id=? and status='RUNNING'
                """, Timestamp.valueOf(now), id) > 0;
    }

    @Override
    public boolean markFailed(Long id, String errorCode, String errorMsg, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='FAILED', finish_time=?, error_code=?, error_msg=?, update_time=now(), version=version+1
                 where id=? and status in ('RUNNING','WAIT_RETRY','RUNNABLE')
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
                   set status='CANCELLED', finish_time=?, update_time=now(), version=version+1
                 where task_no=? and status in ('PENDING','RUNNABLE','WAIT_RETRY')
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
    public List<SchedulerTask> findRunningHeartbeatTimeout(String groupCode, LocalDateTime cutoff, int limit) {
        return jdbcTemplate.query("""
                select * from scheduler_task
                 where group_code=? and status='RUNNING' and heartbeat_time < ?
                 order by heartbeat_time asc limit ?
                """, this::mapTask, groupCode, Timestamp.valueOf(cutoff), limit);
    }

    @Override
    public List<SchedulerTask> findRunnableForQueueRefill(LocalDateTime now, int limit) {
        return jdbcTemplate.query("""
                select * from scheduler_task
                 where status in ('RUNNABLE','WAIT_RETRY')
                 order by execute_at asc limit ?
                """, this::mapTask, limit);
    }

    @Override
    public void promotePendingToRunnable(LocalDateTime now, int limit) {
        jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNABLE', update_time=now(), version=version+1
                 where id in (
                    select id from (
                        select id from scheduler_task
                         where status='PENDING' and execute_at <= ?
                         order by execute_at asc
                         limit ?
                    ) t
                 )
                """, Timestamp.valueOf(now), limit);
    }

    @Override
    public boolean markRunnableIfPending(Long id, LocalDateTime now) {
        return jdbcTemplate.update("""
                update scheduler_task
                   set status='RUNNABLE', update_time=now(), version=version+1
                 where id=? and status='PENDING' and execute_at <= ?
                """, id, Timestamp.valueOf(now)) > 0;
    }

    @Override
    public void markTerminalByBusinessState(Long id, TaskStatus status, LocalDateTime now) {
        jdbcTemplate.update("""
                update scheduler_task
                   set status=?, finish_time=?, update_time=now(), version=version+1
                 where id=? and status in ('RUNNABLE','WAIT_RETRY','RUNNING','PENDING')
                """, status.name(), Timestamp.valueOf(now), id);
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

    private SchedulerTask mapTask(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        SchedulerTask task = new SchedulerTask();
        task.setId(rs.getLong("id"));
        task.setTaskNo(rs.getString("task_no"));
        task.setGroupCode(rs.getString("group_code"));
        task.setUserId(rs.getString("user_id"));
        task.setBizType(rs.getString("biz_type"));
        task.setBizKey(rs.getString("biz_key"));
        task.setStatus(TaskStatus.valueOf(rs.getString("status")));
        task.setPriority(rs.getInt("priority"));
        task.setExecuteAt(tsToLdt(rs.getTimestamp("execute_at")));
        task.setNextRetryAt(tsToLdt(rs.getTimestamp("next_retry_at")));
        task.setRetryCount(rs.getInt("retry_count"));
        task.setMaxRetryCount(rs.getInt("max_retry_count"));
        task.setExecuteTimeoutSec((Integer) rs.getObject("execute_timeout_sec"));
        task.setRetryDelaySec((Integer) rs.getObject("retry_delay_sec"));
        task.setDispatcherInstance(rs.getString("dispatcher_instance"));
        task.setWorkerInstance(rs.getString("worker_instance"));
        task.setWorkerThread(rs.getString("worker_thread"));
        task.setHeartbeatTime(tsToLdt(rs.getTimestamp("heartbeat_time")));
        task.setStartTime(tsToLdt(rs.getTimestamp("start_time")));
        task.setFinishTime(tsToLdt(rs.getTimestamp("finish_time")));
        task.setVersion(rs.getInt("version"));
        task.setErrorCode(rs.getString("error_code"));
        task.setErrorMsg(rs.getString("error_msg"));
        task.setExtJson(rs.getString("ext_json"));
        task.setCreateTime(tsToLdt(rs.getTimestamp("create_time")));
        task.setUpdateTime(tsToLdt(rs.getTimestamp("update_time")));
        return task;
    }

    private LocalDateTime tsToLdt(Timestamp ts) {
        return ts == null ? null : ts.toLocalDateTime();
    }
}
