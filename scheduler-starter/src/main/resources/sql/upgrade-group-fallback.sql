·ALTER TABLE scheduler_task
    ADD COLUMN fallback_check_at DATETIME NULL
        COMMENT '下次调用等待降级策略的绝对时间' AFTER wait_deadline_at,
    ADD COLUMN fallback_policy_count INT NOT NULL DEFAULT 0
        COMMENT '降级策略成功落库次数' AFTER fallback_check_at,
    ADD COLUMN group_fallback_count INT NOT NULL DEFAULT 0
        COMMENT '实际切换Group次数' AFTER fallback_policy_count,
    ADD INDEX idx_route_status_fallback(dispatch_route, status, fallback_check_at, id);

CREATE TABLE scheduler_task_group_fallback_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    task_id BIGINT NOT NULL COMMENT '调度任务ID',
    task_no VARCHAR(64) NOT NULL COMMENT '任务唯一号',
    source_group_code VARCHAR(64) NOT NULL COMMENT '切组前Group',
    target_group_code VARCHAR(64) NOT NULL COMMENT '切组后Group',
    previous_fallback_check_at DATETIME NOT NULL COMMENT '本次触发时间',
    next_fallback_check_at DATETIME NULL COMMENT '下次检查时间',
    task_status VARCHAR(32) NOT NULL COMMENT '切组时任务状态',
    fallback_count INT NOT NULL COMMENT '累计实际切组次数',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_task_time(task_id, create_time),
    INDEX idx_source_target_time(source_group_code, target_group_code, create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非运行任务Group切换审计日志';
