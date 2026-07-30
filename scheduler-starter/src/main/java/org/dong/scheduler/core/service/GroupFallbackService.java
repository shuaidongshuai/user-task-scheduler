package org.dong.scheduler.core.service;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupFallbackAction;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
public class GroupFallbackService {
    public static final String HANDLER_NOT_FOUND = "FALLBACK_HANDLER_NOT_FOUND";
    public static final String DECISION_INVALID = "FALLBACK_DECISION_INVALID";
    public static final String TARGET_INVALID = "FALLBACK_TARGET_INVALID";
    public static final String TARGET_DISABLED = "FALLBACK_TARGET_DISABLED";
    public static final String POLICY_EXCEPTION = "FALLBACK_POLICY_EXCEPTION";
    public static final String POLICY_TIMEOUT = "FALLBACK_POLICY_TIMEOUT";

    private static final int MAX_GROUP_CODE_LENGTH = 64;
    private static final int MAX_ERROR_CODE_LENGTH = 64;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 1024;

    private final SchedulerProperties properties;
    private final TaskRepository taskRepository;
    private final GroupConfigRepository groupConfigRepository;
    private final TaskDependencyService taskDependencyService;
    private final QueueRedisService queueRedisService;
    private final TransactionTemplate transactionTemplate;

    public GroupFallbackService(SchedulerProperties properties,
                                TaskRepository taskRepository,
                                GroupConfigRepository groupConfigRepository,
                                TaskDependencyService taskDependencyService,
                                QueueRedisService queueRedisService,
                                TransactionTemplate transactionTemplate) {
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.groupConfigRepository = groupConfigRepository;
        this.taskDependencyService = taskDependencyService;
        this.queueRedisService = queueRedisService;
        this.transactionTemplate = transactionTemplate;
    }

    public FallbackApplyResult applyWaitingDecision(SchedulerTask snapshot,
                                                     GroupFallbackDecision decision,
                                                     LocalDateTime now) {
        Validation validation = validate(snapshot, decision, now);
        if (!validation.valid()) {
            return failWaiting(snapshot, validation.errorCode(), validation.errorMessage(), now, true);
        }
        GroupFallbackAction action = decision.action();
        if (action == GroupFallbackAction.FAIL) {
            return failWaiting(snapshot, normalizeErrorCode(decision.errorCode()),
                    truncate(decision.errorMessage(), MAX_ERROR_MESSAGE_LENGTH), now, true);
        }
        if (action == GroupFallbackAction.ROUTE) {
            String targetGroupCode = decision.targetGroupCode().trim();
            Boolean changed = transactionTemplate.execute(tx -> {
                boolean applied = taskRepository.casRouteFallback(
                        snapshot, targetGroupCode, decision.nextFallbackCheckAt(), now);
                if (applied) {
                    taskRepository.insertGroupFallbackLog(
                            snapshot, targetGroupCode, decision.nextFallbackCheckAt(),
                            snapshot.getGroupFallbackCount() + 1);
                }
                return applied;
            });
            if (!Boolean.TRUE.equals(changed)) {
                return FallbackApplyResult.casMiss(action);
            }
            removeQueueReferencesSafely(snapshot);
            taskRepository.findById(snapshot.getId()).ifPresent(this::enqueueWaitingTaskSafely);
            log.info("task group fallback applied, taskId={}, taskNo={}, sourceGroup={}, targetGroup={}, status={}",
                    snapshot.getId(), snapshot.getTaskNo(), snapshot.getGroupCode(), targetGroupCode,
                    snapshot.getStatus());
            return FallbackApplyResult.applied(action);
        }
        boolean changed = taskRepository.casUpdateFallbackCheck(
                snapshot, decision.nextFallbackCheckAt(), now, true);
        return changed ? FallbackApplyResult.applied(action) : FallbackApplyResult.casMiss(action);
    }

    public FallbackApplyResult failWaiting(SchedulerTask snapshot, String errorCode, String errorMessage,
                                           LocalDateTime now, boolean incrementPolicyCount) {
        TerminalResult result = transactionTemplate.execute(tx -> {
            boolean changed = taskRepository.casFallbackWaitingToFailed(
                    snapshot, normalizeErrorCode(errorCode), truncate(errorMessage, MAX_ERROR_MESSAGE_LENGTH),
                    now, incrementPolicyCount);
            if (!changed) {
                return new TerminalResult(false, List.of());
            }
            List<SchedulerTask> downstream = taskDependencyService.onUpstreamTaskTerminal(
                    snapshot.getId(), TaskStatus.FAILED, now);
            return new TerminalResult(true, downstream);
        });
        if (result == null || !result.changed()) {
            return FallbackApplyResult.casMiss(GroupFallbackAction.FAIL);
        }
        removeQueueReferencesSafely(snapshot);
        result.downstream().forEach(this::enqueueWaitingTaskSafely);
        log.warn("task fallback policy failed task, taskId={}, taskNo={}, group={}, errorCode={}",
                snapshot.getId(), snapshot.getTaskNo(), snapshot.getGroupCode(), errorCode);
        return FallbackApplyResult.applied(GroupFallbackAction.FAIL);
    }

    public FallbackApplyResult deferAfterExecutorReject(SchedulerTask snapshot, LocalDateTime now) {
        LocalDateTime deferredAt = now.plusNanos(properties.getFallbackExecutorRejectBackoffMs() * 1_000_000L);
        if (snapshot.getWaitDeadlineAt() != null && !deferredAt.isBefore(snapshot.getWaitDeadlineAt())) {
            deferredAt = null;
        }
        boolean changed = taskRepository.casUpdateFallbackCheck(snapshot, deferredAt, now, false);
        return changed ? FallbackApplyResult.deferredResult() : FallbackApplyResult.casMiss(null);
    }

    private Validation validate(SchedulerTask snapshot, GroupFallbackDecision decision, LocalDateTime now) {
        if (decision == null || decision.action() == null) {
            return Validation.error(DECISION_INVALID, "fallback decision or action is null");
        }
        LocalDateTime nextCheckAt = decision.nextFallbackCheckAt();
        if (nextCheckAt != null) {
            LocalDateTime minimum = now.plusNanos(properties.getFallbackMinNextCheckDelayMs() * 1_000_000L);
            if (nextCheckAt.isBefore(minimum)) {
                return Validation.error(DECISION_INVALID, "nextFallbackCheckAt is earlier than minimum delay");
            }
            if (snapshot.getWaitDeadlineAt() != null && !nextCheckAt.isBefore(snapshot.getWaitDeadlineAt())) {
                return Validation.error(DECISION_INVALID, "nextFallbackCheckAt must be before waitDeadlineAt");
            }
        }
        return switch (decision.action()) {
            case ROUTE -> validateRoute(snapshot, decision);
            case KEEP_CURRENT -> nextCheckAt == null || decision.targetGroupCode() != null
                    || decision.errorCode() != null || decision.errorMessage() != null
                    ? Validation.error(DECISION_INVALID, "KEEP_CURRENT fields are invalid")
                    : Validation.ok();
            case STOP_CHECKING -> hasUnexpectedFields(decision)
                    ? Validation.error(DECISION_INVALID, "STOP_CHECKING contains unexpected fields")
                    : Validation.ok();
            case FAIL -> validateFail(decision);
        };
    }

    private Validation validateRoute(SchedulerTask snapshot, GroupFallbackDecision decision) {
        if (decision.targetGroupCode() == null || decision.targetGroupCode().isBlank()) {
            return Validation.error(TARGET_INVALID, "fallback target group is blank");
        }
        String target = decision.targetGroupCode().trim();
        if (target.length() > MAX_GROUP_CODE_LENGTH || target.equals(snapshot.getGroupCode())) {
            return Validation.error(TARGET_INVALID, "fallback target group is invalid");
        }
        if (decision.errorCode() != null || decision.errorMessage() != null) {
            return Validation.error(DECISION_INVALID, "ROUTE contains error fields");
        }
        if (groupConfigRepository.findEnabledByGroupCode(target).isEmpty()) {
            return Validation.error(TARGET_DISABLED, "fallback target group is missing or disabled");
        }
        return Validation.ok();
    }

    private Validation validateFail(GroupFallbackDecision decision) {
        if (decision.targetGroupCode() != null || decision.nextFallbackCheckAt() != null
                || decision.errorCode() == null || decision.errorCode().isBlank()
                || decision.errorCode().trim().length() > MAX_ERROR_CODE_LENGTH) {
            return Validation.error(DECISION_INVALID, "FAIL decision fields are invalid");
        }
        return Validation.ok();
    }

    private boolean hasUnexpectedFields(GroupFallbackDecision decision) {
        return decision.targetGroupCode() != null || decision.nextFallbackCheckAt() != null
                || decision.errorCode() != null || decision.errorMessage() != null;
    }

    private void enqueueWaitingTask(SchedulerTask task) {
        if (task.getStatus() == TaskStatus.PENDING) {
            if (!taskDependencyService.hasUnsatisfiedDependencies(task.getId())) {
                queueRedisService.enqueue(task);
            }
            return;
        }
        if (task.getStatus() == TaskStatus.RUNNABLE || task.getStatus() == TaskStatus.WAIT_RETRY) {
            if (task.due(LocalDateTime.now())) {
                queueRedisService.enqueueReady(task);
            } else {
                queueRedisService.enqueue(task);
            }
        }
    }

    private void enqueueWaitingTaskSafely(SchedulerTask task) {
        try {
            enqueueWaitingTask(task);
        } catch (RuntimeException ex) {
            log.error("failed to enqueue fallback result, taskId={}, taskNo={}, group={}, route={}, status={}",
                    task.getId(), task.getTaskNo(), task.getGroupCode(), task.getDispatchRoute(), task.getStatus(), ex);
        }
    }

    private void removeQueueReferencesSafely(SchedulerTask snapshot) {
        try {
            queueRedisService.removeQueueReferences(snapshot);
        } catch (RuntimeException ex) {
            log.error("failed to clean fallback queue references, taskId={}, taskNo={}, group={}, route={}",
                    snapshot.getId(), snapshot.getTaskNo(), snapshot.getGroupCode(), snapshot.getDispatchRoute(), ex);
        }
    }

    private String normalizeErrorCode(String errorCode) {
        if (errorCode == null || errorCode.isBlank()) {
            return DECISION_INVALID;
        }
        String value = errorCode.trim();
        return value.length() <= MAX_ERROR_CODE_LENGTH ? value : value.substring(0, MAX_ERROR_CODE_LENGTH);
    }

    private String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    public record FallbackApplyResult(boolean changed, boolean deferred, GroupFallbackAction action) {
        public static FallbackApplyResult applied(GroupFallbackAction action) {
            return new FallbackApplyResult(true, false, action);
        }

        public static FallbackApplyResult deferredResult() {
            return new FallbackApplyResult(true, true, null);
        }

        public static FallbackApplyResult casMiss(GroupFallbackAction action) {
            return new FallbackApplyResult(false, false, action);
        }
    }

    private record Validation(boolean valid, String errorCode, String errorMessage) {
        private static Validation ok() {
            return new Validation(true, null, null);
        }

        private static Validation error(String code, String message) {
            return new Validation(false, code, message);
        }
    }

    private record TerminalResult(boolean changed, List<SchedulerTask> downstream) {
    }
}
