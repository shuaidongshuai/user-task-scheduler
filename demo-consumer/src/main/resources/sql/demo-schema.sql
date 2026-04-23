DROP TABLE IF EXISTS demo_biz_task;

CREATE TABLE IF NOT EXISTS demo_biz_task (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    biz_key VARCHAR(128) NOT NULL UNIQUE COMMENT '业务键（与调度任务biz_key对应）',
    status VARCHAR(32) NOT NULL COMMENT 'RUNNING/SUCCESS/FAILED',
    payload TEXT DEFAULT NULL COMMENT '示例业务负载',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Demo业务任务表示例';

INSERT INTO scheduler_group_config(
    group_code, enabled, max_concurrency, user_base_concurrency,
    dynamic_user_limit_enabled, load_strategy_json,
    dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec,
    description
) VALUES (
    'demo-group', 1, 20, 2,
    1,
    JSON_OBJECT(
        'enabled', true,
        'rounding', 'FLOOR',
        'minLimit', 1,
        'maxLimit', 20,
        'rules', JSON_ARRAY(
            JSON_OBJECT('loadLt', 0.25, 'factor', 4.0),
            JSON_OBJECT('loadLt', 0.50, 'factor', 2.0),
            JSON_OBJECT('loadLt', 0.75, 'factor', 1.0),
            JSON_OBJECT('loadLt', 1.01, 'factor', 0.5)
        )
    ),
    100, 30, 60,
    'demo group'
)
ON DUPLICATE KEY UPDATE
    enabled=VALUES(enabled),
    max_concurrency=VALUES(max_concurrency),
    user_base_concurrency=VALUES(user_base_concurrency),
    dynamic_user_limit_enabled=VALUES(dynamic_user_limit_enabled),
    load_strategy_json=VALUES(load_strategy_json),
    dispatch_batch_size=VALUES(dispatch_batch_size),
    heartbeat_timeout_sec=VALUES(heartbeat_timeout_sec),
    lock_expire_sec=VALUES(lock_expire_sec),
    description=VALUES(description);
