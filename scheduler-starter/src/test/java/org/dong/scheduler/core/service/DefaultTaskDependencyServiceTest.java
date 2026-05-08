package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencySummary;
import org.dong.scheduler.core.repo.TaskDependencyRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultTaskDependencyServiceTest {

    @Mock
    private TaskRepository taskRepository;
    @Mock
    private TaskDependencyRepository taskDependencyRepository;

    @Test
    void shouldAddTaskToReadyWhenAllDependenciesSatisfiedAndTaskIsDue() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        SchedulerTask task = new SchedulerTask();
        task.setId(100L);
        task.setStatus(TaskStatus.PENDING);
        task.setGroupCode("g1");
        task.setExecuteAt(LocalDateTime.now().minusSeconds(1));

        when(taskRepository.findById(100L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(100L)).thenReturn(new TaskDependencySummary(2, 2, 0));
        when(taskRepository.markRunnableIfPending(eq(100L), any(LocalDateTime.class))).thenReturn(true);

        SchedulerTask queueTask = service.refreshTaskAfterSubmit(100L, LocalDateTime.now());

        assertNotNull(queueTask);
        assertEquals(TaskStatus.RUNNABLE, task.getStatus());
    }

    @Test
    void shouldFailPendingTaskImmediatelyWhenAnyDependencyIsImpossible() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        SchedulerTask task = new SchedulerTask();
        task.setId(101L);
        task.setStatus(TaskStatus.PENDING);
        task.setExecuteAt(LocalDateTime.now().minusSeconds(1));

        when(taskRepository.findById(101L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(101L)).thenReturn(new TaskDependencySummary(2, 1, 1));
        when(taskRepository.markFailedPendingByDependency(eq(101L), eq("DEPENDENCY_NOT_SATISFIED"), any(), any(LocalDateTime.class)))
                .thenReturn(true);

        SchedulerTask queueTask = service.refreshTaskAfterSubmit(101L, LocalDateTime.now());

        assertTrue(queueTask == null);
        verify(taskRepository).markFailedPendingByDependency(eq(101L), eq("DEPENDENCY_NOT_SATISFIED"), any(), any(LocalDateTime.class));
    }
}
