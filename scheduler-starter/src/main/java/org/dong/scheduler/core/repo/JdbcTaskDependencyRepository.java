package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskDependencySummary;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

public class JdbcTaskDependencyRepository implements TaskDependencyRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcTaskDependencyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void batchInsert(Long taskId, List<TaskDependencyRequest> dependencies, LocalDateTime now) {
        if (dependencies == null || dependencies.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate("""
                insert into scheduler_task_dependency(
                    task_id, depends_on_task_id, target_state, status, create_time, update_time
                ) values(?,?,?,?,?,?)
                """, dependencies, dependencies.size(), (ps, dep) -> {
            ps.setLong(1, taskId);
            ps.setLong(2, dep.getTaskId());
            ps.setString(3, dep.getTargetState().name());
            ps.setString(4, "WAITING");
            ps.setTimestamp(5, Timestamp.valueOf(now));
            ps.setTimestamp(6, Timestamp.valueOf(now));
        });
    }

    @Override
    public void refreshByTaskId(Long taskId, LocalDateTime now) {
        Timestamp nowTs = Timestamp.valueOf(now);
        jdbcTemplate.update("""
                update scheduler_task_dependency d
                   set status = case
                       when exists (
                           select 1
                             from scheduler_task t
                            where t.id = d.depends_on_task_id
                              and t.status = 'SUCCESS'
                              and d.target_state in ('SUCCESS', 'TERMINAL')
                       ) then 'SATISFIED'
                       when exists (
                           select 1
                             from scheduler_task t
                            where t.id = d.depends_on_task_id
                              and t.status = 'FAILED'
                              and d.target_state in ('FAILED', 'TERMINAL')
                       ) then 'SATISFIED'
                       when exists (
                           select 1
                             from scheduler_task t
                            where t.id = d.depends_on_task_id
                              and t.status = 'CANCELLED'
                       ) then 'IMPOSSIBLE'
                       when exists (
                           select 1
                             from scheduler_task t
                            where t.id = d.depends_on_task_id
                              and t.status = 'SUCCESS'
                              and d.target_state = 'FAILED'
                       ) then 'IMPOSSIBLE'
                       when exists (
                           select 1
                             from scheduler_task t
                            where t.id = d.depends_on_task_id
                              and t.status = 'FAILED'
                              and d.target_state = 'SUCCESS'
                       ) then 'IMPOSSIBLE'
                       else status
                   end,
                       update_time = ?
                 where d.task_id = ?
                   and d.status = 'WAITING'
                   and exists (
                       select 1
                         from scheduler_task t
                        where t.id = d.depends_on_task_id
                          and t.status in ('SUCCESS', 'FAILED', 'CANCELLED')
                   )
                """, nowTs, taskId);

        jdbcTemplate.update("""
                update scheduler_task_dependency d
                   set d.status = 'IMPOSSIBLE',
                       d.update_time = ?
                 where d.task_id = ?
                   and d.status = 'WAITING'
                   and not exists (
                       select 1 from scheduler_task t where t.id = d.depends_on_task_id
                   )
                """, nowTs, taskId);
    }

    @Override
    public List<Long> findDependentTaskIds(Long dependsOnTaskId) {
        return jdbcTemplate.query("""
                select distinct task_id
                  from scheduler_task_dependency
                 where depends_on_task_id = ?
                """, (rs, rowNum) -> rs.getLong(1), dependsOnTaskId);
    }

    @Override
    public void updateByUpstreamTerminal(Long dependsOnTaskId, TaskStatus actualStatus, LocalDateTime now) {
        Timestamp nowTs = Timestamp.valueOf(now);
        switch (actualStatus) {
            case SUCCESS -> {
                jdbcTemplate.update("""
                        update scheduler_task_dependency
                           set status = 'SATISFIED', update_time = ?
                         where depends_on_task_id = ?
                           and status = 'WAITING'
                           and target_state in ('SUCCESS', 'TERMINAL')
                        """, nowTs, dependsOnTaskId);
                jdbcTemplate.update("""
                        update scheduler_task_dependency
                           set status = 'IMPOSSIBLE', update_time = ?
                         where depends_on_task_id = ?
                           and status = 'WAITING'
                           and target_state = 'FAILED'
                        """, nowTs, dependsOnTaskId);
            }
            case FAILED -> {
                jdbcTemplate.update("""
                        update scheduler_task_dependency
                           set status = 'SATISFIED', update_time = ?
                         where depends_on_task_id = ?
                           and status = 'WAITING'
                           and target_state in ('FAILED', 'TERMINAL')
                        """, nowTs, dependsOnTaskId);
                jdbcTemplate.update("""
                        update scheduler_task_dependency
                           set status = 'IMPOSSIBLE', update_time = ?
                         where depends_on_task_id = ?
                           and status = 'WAITING'
                           and target_state = 'SUCCESS'
                        """, nowTs, dependsOnTaskId);
            }
            case CANCELLED -> jdbcTemplate.update("""
                    update scheduler_task_dependency
                       set status = 'IMPOSSIBLE', update_time = ?
                     where depends_on_task_id = ?
                       and status = 'WAITING'
                    """, nowTs, dependsOnTaskId);
            default -> {
                // non-terminal statuses never propagate dependency resolution
            }
        }
    }

    @Override
    public TaskDependencySummary summarize(Long taskId) {
        List<TaskDependencySummary> list = jdbcTemplate.query("""
                select count(1) as total_count,
                       sum(case when status = 'SATISFIED' then 1 else 0 end) as satisfied_count,
                       sum(case when status = 'IMPOSSIBLE' then 1 else 0 end) as impossible_count
                  from scheduler_task_dependency
                 where task_id = ?
                """, (rs, rowNum) -> new TaskDependencySummary(
                rs.getInt("total_count"),
                rs.getInt("satisfied_count"),
                rs.getInt("impossible_count")
        ), taskId);
        if (list.isEmpty()) {
            return new TaskDependencySummary(0, 0, 0);
        }
        return list.getFirst();
    }
}
