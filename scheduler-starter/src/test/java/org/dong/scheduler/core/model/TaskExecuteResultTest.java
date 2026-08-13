package org.dong.scheduler.core.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskExecuteResultTest {

    @Test
    void shouldCreateRetryableTargetGroupResultWithNextFallbackCheckAt() {
        LocalDateTime nextFallbackCheckAt = LocalDateTime.of(2026, 8, 12, 10, 30);

        TaskExecuteResult result = TaskExecuteResult.retryableOnGroup("backup-group", nextFallbackCheckAt);

        assertTrue(result.isRetryable());
        assertEquals("backup-group", result.getNextGroupCode());
        assertEquals(nextFallbackCheckAt, result.getNextFallbackCheckAt());
    }
}
