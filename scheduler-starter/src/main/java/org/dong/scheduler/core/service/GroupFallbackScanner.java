package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.spi.TaskHandler;
import org.dong.scheduler.core.util.ThreadContextUtil;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class GroupFallbackScanner {
    private static final long POLICY_EXCEPTION_RETRY_DELAY_MULTIPLIER = 10L;

    private final SchedulerProperties properties;
    private final TaskRepository taskRepository;
    private final TaskHandlerRegistry handlerRegistry;
    private final TaskStateService taskStateService;
    private final GroupFallbackService fallbackService;
    private final ThreadPoolExecutor callbackExecutor;

    public GroupFallbackScanner(SchedulerProperties properties,
                                TaskRepository taskRepository,
                                TaskHandlerRegistry handlerRegistry,
                                TaskStateService taskStateService,
                                GroupFallbackService fallbackService,
                                ThreadPoolExecutor callbackExecutor) {
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.handlerRegistry = handlerRegistry;
        this.taskStateService = taskStateService;
        this.fallbackService = fallbackService;
        this.callbackExecutor = callbackExecutor;
    }

    public int scanOnce() {
        LocalDateTime scanNow = LocalDateTime.now();
        List<Long> queried = taskRepository.findFallbackDueTaskIds(
                properties.getDispatchRoute(), scanNow, properties.getFallbackScanLimit());
        List<Long> taskIds = new ArrayList<>(new LinkedHashSet<>(queried));
        List<CallbackSlot> inFlight = new ArrayList<>();
        int cursor = 0;
        int changed = 0;
        while (cursor < taskIds.size() || !inFlight.isEmpty()) {
            while (cursor < taskIds.size() && inFlight.size() < properties.getFallbackCallbackThreads()) {
                Long taskId = taskIds.get(cursor++);
                SchedulerTask snapshot = taskRepository.findById(taskId).orElse(null);
                if (!eligible(snapshot, LocalDateTime.now())) {
                    continue;
                }
                if (snapshot.waitingTimedOut(LocalDateTime.now())) {
                    if (taskStateService.markFailedByWaitDeadline(snapshot.getId(), LocalDateTime.now())) {
                        changed++;
                    }
                    continue;
                }
                TaskHandler handler = handlerRegistry.find(snapshot.getBizType());
                if (handler == null) {
                    if (fallbackService.stopWaitingFallback(snapshot, GroupFallbackService.HANDLER_NOT_FOUND,
                            "TaskHandler not found for bizType=" + snapshot.getBizType(),
                            LocalDateTime.now(), false).changed()) {
                        changed++;
                    }
                    continue;
                }
                AtomicLong startedNanos = new AtomicLong(0L);
                try {
                    Future<GroupFallbackDecision> future = callbackExecutor.submit(ThreadContextUtil.addContext(() -> {
                        startedNanos.set(System.nanoTime());
                        return handler.onGroupWaitTimeout(snapshot);
                    }));
                    inFlight.add(new CallbackSlot(snapshot, future, startedNanos));
                } catch (RejectedExecutionException ex) {
                    fallbackService.deferAfterExecutorReject(snapshot, LocalDateTime.now());
                    log.warn("fallback callback executor rejected task, taskId={}, taskNo={}",
                            snapshot.getId(), snapshot.getTaskNo());
                }
            }
            boolean progressed = false;
            for (int index = inFlight.size() - 1; index >= 0; index--) {
                CallbackSlot slot = inFlight.get(index);
                if (slot.future().isDone()) {
                    changed += complete(slot);
                    inFlight.remove(index);
                    progressed = true;
                    continue;
                }
                long startedAt = slot.startedNanos().get();
                long elapsedMs = startedAt == 0L ? 0L : (System.nanoTime() - startedAt) / 1_000_000L;
                if (startedAt != 0L && elapsedMs >= properties.getFallbackPolicyTimeoutMs()) {
                    slot.future().cancel(true);
                    if (fallbackService.stopWaitingFallback(slot.snapshot(), GroupFallbackService.POLICY_TIMEOUT,
                            "onGroupWaitTimeout exceeded " + properties.getFallbackPolicyTimeoutMs() + "ms",
                            LocalDateTime.now(), true).changed()) {
                        changed++;
                    }
                    inFlight.remove(index);
                    progressed = true;
                }
            }
            if (!progressed && !inFlight.isEmpty()) {
                LockSupport.parkNanos(10_000_000L);
            }
        }
        return changed;
    }

    private int complete(CallbackSlot slot) {
        GroupFallbackDecision decision;
        try {
            decision = slot.future().get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception ex) {
            Throwable cause = ex.getCause() == null ? ex : ex.getCause();
            long delayMs = properties.getFallbackMinNextCheckDelayMs() * POLICY_EXCEPTION_RETRY_DELAY_MULTIPLIER;
            LocalDateTime nextCheckAt = LocalDateTime.now().plusNanos(delayMs * 1_000_000L);
            log.warn("onGroupWaitTimeout failed; retry fallback check later, taskId={}, taskNo={}, nextCheckAt={}, cause={}",
                    slot.snapshot().getId(), slot.snapshot().getTaskNo(),
                    nextCheckAt,
                    cause.getClass().getSimpleName() + ":" + cause.getMessage());
            return fallbackService.applyWaitingDecision(slot.snapshot(), GroupFallbackDecision.keepCurrent(nextCheckAt),
                    LocalDateTime.now()).changed() ? 1 : 0;
        }
        return fallbackService.applyWaitingDecision(
                slot.snapshot(), decision, LocalDateTime.now()).changed() ? 1 : 0;
    }

    private boolean eligible(SchedulerTask task, LocalDateTime now) {
        if (task == null || task.getFallbackCheckAt() == null || task.getFallbackCheckAt().isAfter(now)) {
            return false;
        }
        if (!sameRoute(task.getDispatchRoute(), properties.getDispatchRoute())) {
            return false;
        }
        Set<TaskStatus> statuses = Set.of(TaskStatus.PENDING, TaskStatus.RUNNABLE, TaskStatus.WAIT_RETRY);
        return statuses.contains(task.getStatus());
    }

    private boolean sameRoute(String left, String right) {
        String normalizedLeft = left == null || left.isBlank() ? null : left;
        String normalizedRight = right == null || right.isBlank() ? null : right;
        return java.util.Objects.equals(normalizedLeft, normalizedRight);
    }

    private record CallbackSlot(
            SchedulerTask snapshot,
            Future<GroupFallbackDecision> future,
            AtomicLong startedNanos
    ) {
    }
}
