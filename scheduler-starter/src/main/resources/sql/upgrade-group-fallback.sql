·ALTER TABLE scheduler_task
    ADD COLUMN fallback_check_at DATETIME NULL
        COMMENT '下次调用等待降级策略的绝对时间' AFTER wait_deadline_at,
    ADD COLUMN fallback_policy_count INT NOT NULL DEFAULT 0
        COMMENT '降级策略成功落库次数' AFTER fallback_check_at,
    ADD COLUMN group_fallback_count INT NOT NULL DEFAULT 0
        COMMENT '实际切换Group次数' AFTER fallback_policy_count,
    ADD INDEX idx_route_status_fallback(dispatch_route, status, fallback_check_at, id);
