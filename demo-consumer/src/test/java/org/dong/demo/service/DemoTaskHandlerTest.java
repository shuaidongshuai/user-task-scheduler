package org.dong.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.model.GroupFallbackAction;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DemoTaskHandlerTest {
    private final DemoTaskHandler handler = new DemoTaskHandler(mock(DemoBizTaskRepository.class), new ObjectMapper());

    @Test
    void shouldRouteOnlyWhenFallbackTargetGroupIsProvidedInExtInfo() {
        SchedulerTask task = new SchedulerTask();
        task.setExtInfo("{\"fallbackTargetGroup\":\"demo-backup-group\"}");

        GroupFallbackDecision decision = handler.onGroupWaitTimeout(task);

        assertEquals(GroupFallbackAction.ROUTE, decision.action());
        assertEquals("demo-backup-group", decision.targetGroupCode());
    }

    @Test
    void shouldKeepExistingDefaultWhenNoFallbackTargetGroupIsProvided() {
        SchedulerTask task = new SchedulerTask();
        task.setExtInfo("{\"failBeforeSuccess\":0}");

        GroupFallbackDecision decision = handler.onGroupWaitTimeout(task);

        assertEquals(GroupFallbackAction.STOP_CHECKING, decision.action());
    }

    @Test
    void shouldRequestRetryOnTargetGroupWhenConfiguredInExecutionOptions() throws Exception {
        SchedulerTask task = new SchedulerTask();
        task.setId(1L);
        task.setBizKey("biz-execution-route");
        task.setRetryCount(0);
        task.setExtInfo("{\"failBeforeSuccess\":1,\"executeRetryTargetGroup\":\"demo-backup-group\"}");

        TaskExecuteResult result = handler.execute(task);

        assertTrue(result.isRetryable());
        assertEquals("demo-backup-group", result.getNextGroupCode());
    }
}
