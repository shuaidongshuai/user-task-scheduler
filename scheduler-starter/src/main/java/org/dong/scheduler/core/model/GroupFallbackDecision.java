package org.dong.scheduler.core.model;

import java.time.LocalDateTime;

public record GroupFallbackDecision(
        GroupFallbackAction action,
        String targetGroupCode,
        LocalDateTime nextFallbackCheckAt,
        String errorCode,
        String errorMessage
) {
    public static GroupFallbackDecision routeTo(String groupCode, LocalDateTime nextCheckAt) {
        return new GroupFallbackDecision(GroupFallbackAction.ROUTE, groupCode, nextCheckAt, null, null);
    }

    public static GroupFallbackDecision keepCurrent(LocalDateTime nextCheckAt) {
        return new GroupFallbackDecision(GroupFallbackAction.KEEP_CURRENT, null, nextCheckAt, null, null);
    }

    public static GroupFallbackDecision stopChecking() {
        return new GroupFallbackDecision(GroupFallbackAction.STOP_CHECKING, null, null, null, null);
    }

    public static GroupFallbackDecision fail(String code, String message) {
        return new GroupFallbackDecision(GroupFallbackAction.FAIL, null, null, code, message);
    }
}
