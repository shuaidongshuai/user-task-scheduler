package org.dong.scheduler.core.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskExecuteResultTest {

    @Test
    void shouldCreateRetryableTargetGroupResultWithoutErrorReason() {
        TaskExecuteResult result = TaskExecuteResult.retryableOnGroup("backup-group");

        assertTrue(result.isRetryable());
        assertEquals("backup-group", result.getNextGroupCode());
        assertNull(result.getErrorCode());
        assertNull(result.getErrorMsg());
    }
}
