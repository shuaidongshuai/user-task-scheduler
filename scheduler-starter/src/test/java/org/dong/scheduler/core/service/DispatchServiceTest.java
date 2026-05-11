package org.dong.scheduler.core.service;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DispatchServiceTest {

    @Mock
    private GroupConfigRepository groupConfigRepository;
    @Mock
    private TaskRepository taskRepository;
    @Mock
    private QueueRedisService queueRedisService;
    @Mock
    private ConcurrencyGuard concurrencyGuard;
    @Mock
    private DynamicUserLimitService dynamicUserLimitService;
    @Mock
    private WorkerService workerService;
    @Mock
    private RecoveryService recoveryService;
    @Mock
    private TaskStateService taskStateService;

    private DispatchService dispatchService;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        BusinessTaskStateProviderRegistry providerRegistry = new BusinessTaskStateProviderRegistry(List.of());
        dispatchService = new DispatchService(
                properties,
                groupConfigRepository,
                taskRepository,
                queueRedisService,
                concurrencyGuard,
                dynamicUserLimitService,
                workerService,
                recoveryService,
                providerRegistry,
                taskStateService
        );
    }

    @Test
    void shouldAddPromotedRunnableTaskToReadyWithoutRecheckingDueFlag() {
        GroupConfig group = new GroupConfig();
        group.setGroupCode("g1");
        group.setEnabled(true);
        group.setMaxConcurrency(10);
        group.setDispatchBatchSize(100);

        SchedulerTask task = new SchedulerTask();
        task.setId(401L);
        task.setTaskNo("task-401");
        task.setGroupCode("g1");
        task.setUserId("u1");
        task.setStatus(TaskStatus.RUNNABLE);
        task.setExecuteAt(LocalDateTime.now().plusMinutes(1));

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(group));
        when(queueRedisService.promoteDueTasks(eq("g1"), anyLong(), eq(100)))
                .thenReturn(List.of(401L));
        when(taskRepository.findById(401L)).thenReturn(Optional.of(task));
        when(concurrencyGuard.groupRunning("g1")).thenReturn(10L);

        dispatchService.dispatchOnce();

        verify(queueRedisService).addToReady(task);
        verify(queueRedisService, never()).enqueue(task);
    }
}
