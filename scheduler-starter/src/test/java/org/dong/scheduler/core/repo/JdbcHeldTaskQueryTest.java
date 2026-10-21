package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.SchedulerTask;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcHeldTaskQueryTest {
    @Test
    void shouldSelectDueHeldTasksAcrossUsersAndRespectRouteGroupCursorAndLimit() {
        EmbeddedDatabase database = new EmbeddedDatabaseBuilder().generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2).build();
        try {
            JdbcTemplate jdbc = new JdbcTemplate(database);
            jdbc.execute("""
                    create table scheduler_task (
                        id bigint primary key, user_id varchar(64), group_code varchar(64),
                        dispatch_route varchar(64), status varchar(32), execute_at timestamp)
                    """);
            LocalDateTime now = LocalDateTime.of(2026, 10, 21, 12, 0);
            insert(jdbc, 1, "a", "g", "api", "WAIT_HOLD", now.minusSeconds(2));
            insert(jdbc, 2, "b", "g", "api", "WAIT_HOLD", now.minusSeconds(1));
            insert(jdbc, 3, "c", "g", "api", "WAIT_HOLD", now.minusSeconds(1));
            insert(jdbc, 4, "a", "g", "api", "WAIT_HOLD", now.plusSeconds(1));
            insert(jdbc, 5, "a", "g", "api", "RUNNABLE", now.minusMinutes(1));
            insert(jdbc, 6, "a", "g", "server", "WAIT_HOLD", now.minusMinutes(1));
            insert(jdbc, 7, "a", "other", "api", "WAIT_HOLD", now.minusMinutes(1));
            insert(jdbc, 8, "a", "g", null, "WAIT_HOLD", now.minusMinutes(1));
            insert(jdbc, 9, "b", "g", "", "WAIT_HOLD", now.minusMinutes(1));
            insert(jdbc, 10, "c", "g", "api", "WAIT_HOLD", now);
            JdbcTaskRepository repository = new JdbcTaskRepository(jdbc);

            assertThat(repository.findDueHeldTasks("g", "api", now, null, null, 10))
                    .extracting(SchedulerTask::getId).containsExactly(1L, 2L, 3L, 10L);
            assertThat(repository.findDueHeldTasks("g", "api", now, null, null, 1))
                    .extracting(SchedulerTask::getId).containsExactly(1L);
            assertThat(repository.findDueHeldTasks("g", "api", now, now.minusSeconds(1), 2L, 10))
                    .extracting(SchedulerTask::getId).containsExactly(3L, 10L);
            assertThat(repository.findDueHeldTasks("g", "api", now, now, 10L, 10)).isEmpty();
            for (String route : new String[]{null, "", "  "}) {
                assertThat(repository.findDueHeldTasks("g", route, now, null, null, 10))
                        .extracting(SchedulerTask::getId).containsExactly(8L, 9L);
            }
            assertThat(repository.findDueHeldTasks("g", "api", now, null, null, 0)).isEmpty();
        } finally {
            database.shutdown();
        }
    }

    private void insert(JdbcTemplate jdbc, long id, String user, String group, String route,
                        String status, LocalDateTime executeAt) {
        jdbc.update("insert into scheduler_task values (?,?,?,?,?,?)", id, user, group, route, status, executeAt);
    }
}
