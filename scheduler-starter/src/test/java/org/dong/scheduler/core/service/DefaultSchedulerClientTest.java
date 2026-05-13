package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.DependencyTargetState;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitDependencyRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitRequest;
import org.dong.scheduler.core.model.batch.BatchSubmitResultItem;
import org.dong.scheduler.core.model.batch.BatchSubmitTaskRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultSchedulerClientTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private TaskStateService taskStateService;

    private DefaultSchedulerClient schedulerClient;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDefaultGroupCode("default-group");
        schedulerClient = new DefaultSchedulerClient(taskRepository, queueRedisService, properties, taskStateService);
    }

    @Test
    void shouldRejectBatchDependencyCycle() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "B", DependencyTargetState.SUCCESS))),
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("B")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-b")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)))
        ));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("in-batch dependency cycle detected", ex.getMessage());
    }

    @Test
    void shouldRejectDependencyWhenIdAndRefAreBothProvided() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(1L, "B", DependencyTargetState.SUCCESS)))
        ));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("dependency requires exactly one of dependsOnTaskId/dependsOnClientTaskRef", ex.getMessage());
    }

    @Test
    void shouldRejectSingleSubmitWhenDependencyTaskIdDoesNotExist() {
        TaskSubmitRequest request = new TaskSubmitRequest()
                .setUserId("u1")
                .setBizType("demo.biz")
                .setBizKey("biz-a")
                .setExecuteAt(LocalDateTime.now())
                .setDependencies(List.of(new TaskDependencyRequest(123L, DependencyTargetState.SUCCESS)));
        when(taskRepository.findExistingTaskIds(List.of(123L))).thenReturn(List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submit(request));

        assertEquals("dependency taskId not found: 123", ex.getMessage());
    }

    @Test
    void shouldRejectBatchSubmitWhenDependencyTaskIdDoesNotExist() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(123L, null, DependencyTargetState.SUCCESS)))
        ));
        when(taskRepository.findExistingTaskIds(List.of(123L))).thenReturn(List.of());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> schedulerClient.submitBatch(request));

        assertEquals("dependency taskId not found: 123", ex.getMessage());
    }

    @Test
    void shouldNormalizeBatchTasksBeforeDelegating() {
        BatchSubmitRequest request = new BatchSubmitRequest(List.of(
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("A")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-a")
                        .setPriority(-5)
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of()),
                new BatchSubmitTaskRequest()
                        .setClientTaskRef("B")
                        .setGroupCode("g1")
                        .setUserId("u1")
                        .setBizType("demo.biz")
                        .setBizKey("biz-b")
                        .setMaxRetryCount(-1)
                        .setRetryDelaySec(-2)
                        .setExecuteAt(LocalDateTime.now())
                        .setDependencies(List.of(new BatchSubmitDependencyRequest(null, "A", DependencyTargetState.SUCCESS)))
        ));
        when(taskStateService.submitBatch(anyList())).thenReturn(List.of(
                new BatchSubmitResultItem("A", 101L, "task-a"),
                new BatchSubmitResultItem("B", 102L, "task-b")
        ));

        List<BatchSubmitResultItem> result = schedulerClient.submitBatch(request);

        assertEquals(2, result.size());
        ArgumentCaptor<List<TaskStateService.BatchSubmitCommand>> captor = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(taskStateService).submitBatch(captor.capture());
        List<TaskStateService.BatchSubmitCommand> commands = captor.getValue();
        assertEquals(2, commands.size());
        assertEquals("default-group", commands.getFirst().request().getGroupCode());
        assertEquals(0, commands.getFirst().request().getPriority());
        assertEquals("g1", commands.get(1).request().getGroupCode());
        assertEquals(0, commands.get(1).request().getMaxRetryCount());
        assertEquals(0, commands.get(1).request().getRetryDelaySec());
    }
}
