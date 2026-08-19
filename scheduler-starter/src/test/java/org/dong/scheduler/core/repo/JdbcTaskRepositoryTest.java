package org.dong.scheduler.core.repo;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcTaskRepositoryTest {

    @Test
    void shouldPreserveFirstStartTimeWhenWaitHoldResumes() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JdbcTaskRepository repository = new JdbcTaskRepository(jdbcTemplate);
        LocalDateTime resumeTime = LocalDateTime.of(2026, 8, 18, 16, 30);
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);

        boolean updated = repository.casWaitHoldToRunning(101L, "instance-a", "worker-1", resumeTime);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), argsCaptor.capture());
        assertTrue(updated);
        assertFalse(sqlCaptor.getValue().contains("start_time"));
        assertArrayEquals(
                new Object[]{"instance-a", "instance-a", "worker-1", Timestamp.valueOf(resumeTime), 101L},
                argsCaptor.getValue()
        );
    }
}
