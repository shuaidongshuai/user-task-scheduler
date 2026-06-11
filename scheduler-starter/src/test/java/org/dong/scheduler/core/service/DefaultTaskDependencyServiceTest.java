package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskDependencySummary;
import org.dong.scheduler.core.repo.TaskDependencyRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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

    @Test
    void shouldKeepTaskPendingWhenDependenciesSatisfiedButExecuteAtNotDue() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        SchedulerTask task = new SchedulerTask();
        task.setId(102L);
        task.setStatus(TaskStatus.PENDING);
        task.setExecuteAt(LocalDateTime.now().plusMinutes(5));

        when(taskRepository.findById(102L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(102L)).thenReturn(new TaskDependencySummary(2, 2, 0));

        SchedulerTask queueTask = service.refreshTaskAfterSubmit(102L, LocalDateTime.now());

        assertNotNull(queueTask);
        assertEquals(TaskStatus.PENDING, task.getStatus());
        verify(taskRepository, never()).markRunnableIfPending(eq(102L), any(LocalDateTime.class));
    }

    @Test
    void shouldKeepTaskWaitingWhenOnlyPartOfDependenciesSatisfied() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        SchedulerTask task = new SchedulerTask();
        task.setId(103L);
        task.setStatus(TaskStatus.PENDING);
        task.setExecuteAt(LocalDateTime.now().minusMinutes(1));

        when(taskRepository.findById(103L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(103L)).thenReturn(new TaskDependencySummary(3, 2, 0));

        SchedulerTask queueTask = service.refreshTaskAfterSubmit(103L, LocalDateTime.now());

        assertNull(queueTask);
        verify(taskRepository, never()).markRunnableIfPending(eq(103L), any(LocalDateTime.class));
        verify(taskRepository, never()).markFailedPendingByDependency(eq(103L), any(), any(), any(LocalDateTime.class));
    }

    @Test
    void shouldPromoteDependentTaskWhenUpstreamSuccessSatisfiesAllDependencies() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask task = new SchedulerTask();
        task.setId(200L);
        task.setStatus(TaskStatus.PENDING);
        task.setExecuteAt(now.minusSeconds(1));

        when(taskDependencyRepository.findDependentTaskIds(10L)).thenReturn(List.of(200L));
        when(taskRepository.findById(200L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(200L)).thenReturn(new TaskDependencySummary(2, 2, 0));
        when(taskRepository.markRunnableIfPending(eq(200L), any(LocalDateTime.class))).thenReturn(true);

        List<SchedulerTask> queueTasks = service.onUpstreamTaskTerminal(10L, TaskStatus.SUCCESS, now);

        assertEquals(1, queueTasks.size());
        assertEquals(200L, queueTasks.getFirst().getId());
        assertEquals(TaskStatus.RUNNABLE, queueTasks.getFirst().getStatus());
        verify(taskDependencyRepository).updateByUpstreamTerminal(10L, TaskStatus.SUCCESS, now);
    }

    @Test
    void shouldFailDependentTaskImmediatelyWhenUpstreamStatusMakesDependencyImpossible() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        LocalDateTime now = LocalDateTime.now();
        SchedulerTask task = new SchedulerTask();
        task.setId(201L);
        task.setStatus(TaskStatus.PENDING);
        task.setExecuteAt(now.minusSeconds(1));

        when(taskDependencyRepository.findDependentTaskIds(11L)).thenReturn(List.of(201L));
        when(taskRepository.findById(201L)).thenReturn(Optional.of(task));
        when(taskDependencyRepository.summarize(201L)).thenReturn(new TaskDependencySummary(1, 0, 1));
        when(taskRepository.markFailedPendingByDependency(eq(201L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now)))
                .thenReturn(true);

        List<SchedulerTask> queueTasks = service.onUpstreamTaskTerminal(11L, TaskStatus.FAILED, now);

        assertTrue(queueTasks.isEmpty());
        verify(taskRepository).markFailedPendingByDependency(eq(201L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now));
        verify(taskRepository, never()).markRunnableIfPending(eq(201L), any(LocalDateTime.class));
    }

    @Test
    void shouldPropagateDependencyFailureWhenDependentTaskIsFailedByDependency() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        LocalDateTime now = LocalDateTime.now();

        SchedulerTask middleTask = new SchedulerTask();
        middleTask.setId(201L);
        middleTask.setStatus(TaskStatus.PENDING);
        middleTask.setExecuteAt(now.minusSeconds(1));

        SchedulerTask leafTask = new SchedulerTask();
        leafTask.setId(202L);
        leafTask.setStatus(TaskStatus.PENDING);
        leafTask.setExecuteAt(now.minusSeconds(1));

        when(taskDependencyRepository.findDependentTaskIds(11L)).thenReturn(List.of(201L));
        when(taskRepository.findById(201L)).thenReturn(Optional.of(middleTask));
        when(taskDependencyRepository.summarize(201L)).thenReturn(new TaskDependencySummary(1, 0, 1));
        when(taskDependencyRepository.findFirstImpossibleDependsOnTaskId(201L)).thenReturn(Optional.of(11L));
        when(taskRepository.markFailedPendingByDependency(eq(201L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now)))
                .thenReturn(true);

        when(taskDependencyRepository.findDependentTaskIds(201L)).thenReturn(List.of(202L));
        when(taskRepository.findById(202L)).thenReturn(Optional.of(leafTask));
        when(taskDependencyRepository.summarize(202L)).thenReturn(new TaskDependencySummary(1, 0, 1));
        when(taskDependencyRepository.findFirstImpossibleDependsOnTaskId(202L)).thenReturn(Optional.of(201L));
        when(taskRepository.markFailedPendingByDependency(eq(202L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now)))
                .thenReturn(true);

        List<SchedulerTask> queueTasks = service.onUpstreamTaskTerminal(11L, TaskStatus.FAILED, now);

        assertTrue(queueTasks.isEmpty());
        ArgumentCaptor<String> errorMsgCaptor = ArgumentCaptor.forClass(String.class);
        verify(taskDependencyRepository).updateByUpstreamTerminal(11L, TaskStatus.FAILED, now);
        verify(taskDependencyRepository).updateByUpstreamTerminal(201L, TaskStatus.FAILED, now);
        verify(taskRepository).markFailedPendingByDependency(eq(201L), eq("DEPENDENCY_NOT_SATISFIED"), errorMsgCaptor.capture(), eq(now));
        verify(taskRepository).markFailedPendingByDependency(eq(202L), eq("DEPENDENCY_NOT_SATISFIED"), errorMsgCaptor.capture(), eq(now));
        assertEquals(List.of(
                "dependency task status not satisfied: taskId=11",
                "dependency task status not satisfied: taskId=201"
        ), errorMsgCaptor.getAllValues());
    }

    @Test
    void shouldPromoteDependentTaskWhenItAcceptsDependencyFailure() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        LocalDateTime now = LocalDateTime.now();

        SchedulerTask middleTask = new SchedulerTask();
        middleTask.setId(211L);
        middleTask.setStatus(TaskStatus.PENDING);
        middleTask.setExecuteAt(now.minusSeconds(1));

        SchedulerTask leafTask = new SchedulerTask();
        leafTask.setId(212L);
        leafTask.setStatus(TaskStatus.PENDING);
        leafTask.setExecuteAt(now.minusSeconds(1));

        when(taskDependencyRepository.findDependentTaskIds(21L)).thenReturn(List.of(211L));
        when(taskRepository.findById(211L)).thenReturn(Optional.of(middleTask));
        when(taskDependencyRepository.summarize(211L)).thenReturn(new TaskDependencySummary(1, 0, 1));
        when(taskRepository.markFailedPendingByDependency(eq(211L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now)))
                .thenReturn(true);

        when(taskDependencyRepository.findDependentTaskIds(211L)).thenReturn(List.of(212L));
        when(taskRepository.findById(212L)).thenReturn(Optional.of(leafTask));
        when(taskDependencyRepository.summarize(212L)).thenReturn(new TaskDependencySummary(1, 1, 0));
        when(taskRepository.markRunnableIfPending(eq(212L), eq(now))).thenReturn(true);

        List<SchedulerTask> queueTasks = service.onUpstreamTaskTerminal(21L, TaskStatus.FAILED, now);

        assertEquals(1, queueTasks.size());
        assertEquals(212L, queueTasks.getFirst().getId());
        assertEquals(TaskStatus.RUNNABLE, queueTasks.getFirst().getStatus());
        verify(taskDependencyRepository).updateByUpstreamTerminal(211L, TaskStatus.FAILED, now);
        verify(taskRepository).markFailedPendingByDependency(eq(211L), eq("DEPENDENCY_NOT_SATISFIED"), any(), eq(now));
        verify(taskRepository).markRunnableIfPending(eq(212L), eq(now));
        verify(taskRepository, never()).markFailedPendingByDependency(eq(212L), any(), any(), any(LocalDateTime.class));
    }

    @Test
    void shouldSupportMultiLayerDependencyPropagationAcrossDifferentUpstreams() {
        DefaultTaskDependencyService service = new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
        LocalDateTime now = LocalDateTime.now();

        SchedulerTask middleTask = new SchedulerTask();
        middleTask.setId(300L);
        middleTask.setStatus(TaskStatus.PENDING);
        middleTask.setExecuteAt(now.minusSeconds(1));

        SchedulerTask leafTask = new SchedulerTask();
        leafTask.setId(301L);
        leafTask.setStatus(TaskStatus.PENDING);
        leafTask.setExecuteAt(now.minusSeconds(1));

        when(taskDependencyRepository.findDependentTaskIds(20L)).thenReturn(List.of(300L));
        when(taskRepository.findById(300L)).thenReturn(Optional.of(middleTask));
        when(taskDependencyRepository.summarize(300L)).thenReturn(new TaskDependencySummary(1, 1, 0));
        when(taskRepository.markRunnableIfPending(eq(300L), eq(now))).thenReturn(true);

        List<SchedulerTask> middleQueueTasks = service.onUpstreamTaskTerminal(20L, TaskStatus.SUCCESS, now);

        assertEquals(1, middleQueueTasks.size());
        assertEquals(300L, middleQueueTasks.getFirst().getId());
        assertEquals(TaskStatus.RUNNABLE, middleQueueTasks.getFirst().getStatus());

        when(taskDependencyRepository.findDependentTaskIds(300L)).thenReturn(List.of(301L));
        when(taskRepository.findById(301L)).thenReturn(Optional.of(leafTask));
        when(taskDependencyRepository.summarize(301L)).thenReturn(new TaskDependencySummary(1, 1, 0));
        when(taskRepository.markRunnableIfPending(eq(301L), eq(now))).thenReturn(true);

        List<SchedulerTask> leafQueueTasks = service.onUpstreamTaskTerminal(300L, TaskStatus.SUCCESS, now);

        assertEquals(1, leafQueueTasks.size());
        assertEquals(301L, leafQueueTasks.getFirst().getId());
        assertEquals(TaskStatus.RUNNABLE, leafQueueTasks.getFirst().getStatus());
        assertFalse(leafQueueTasks.isEmpty());
    }
}
