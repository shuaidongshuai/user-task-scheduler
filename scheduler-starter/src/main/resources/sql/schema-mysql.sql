DROP TABLE IF EXISTS scheduler_task_execution;
DROP TABLE IF EXISTS scheduler_task_dependency;
DROP TABLE IF EXISTS scheduler_task_group_fallback_log;
DROP TABLE IF EXISTS scheduler_group_config;
DROP TABLE IF EXISTS scheduler_task;

CREATE TABLE IF NOT EXISTS scheduler_task (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    task_no VARCHAR(64) NOT NULL UNIQUE COMMENT '任务唯一号（调度内部唯一键）',
    group_code VARCHAR(64) NOT NULL COMMENT '任务组编码',
    dispatch_route VARCHAR(64) DEFAULT NULL COMMENT '调度路由（决定由哪类服务消费，为空表示兼容旧队列）',
    user_id VARCHAR(64) NOT NULL COMMENT '用户ID',
    biz_type VARCHAR(64) NOT NULL COMMENT '业务类型（匹配TaskHandler）',
    biz_key VARCHAR(128) NOT NULL COMMENT '业务键（可重复，用于业务侧关联）',

    status VARCHAR(32) NOT NULL COMMENT '任务状态：PENDING/RUNNABLE/RUNNING/WAIT_HOLD/WAIT_RETRY/SUCCESS/FAILED/CANCELLED',
    priority INT NOT NULL DEFAULT 0 COMMENT '优先级，值越大优先级越高',

    execute_at DATETIME NOT NULL COMMENT '计划执行时间',
    next_retry_at DATETIME DEFAULT NULL COMMENT '下次重试时间',

    retry_count INT NOT NULL DEFAULT 0 COMMENT '当前重试次数',
    max_retry_count INT NOT NULL DEFAULT 0 COMMENT '最大重试次数',
    hold_round_count INT NOT NULL DEFAULT 0 COMMENT 'WAIT_HOLD 已经进入的轮次数',
    hold_max_rounds INT NOT NULL DEFAULT 1000 COMMENT 'WAIT_HOLD 最多允许轮次数',
    hold_retry_delay_sec INT NOT NULL DEFAULT 3 COMMENT 'WAIT_HOLD 每轮等待时间（秒）',
    execute_timeout_sec INT DEFAULT NULL COMMENT '任务执行超时时间（秒）',
    retry_delay_sec INT DEFAULT NULL COMMENT '单任务重试间隔（秒，为空则使用全局配置）',
    max_wait_sec INT DEFAULT NULL COMMENT '任务最长等待调度时间（秒，为空表示不限时）',
    wait_deadline_at DATETIME DEFAULT NULL COMMENT '等待调度超时截止时间，为空表示不限时',
    fallback_check_at DATETIME DEFAULT NULL COMMENT '下次调用等待降级策略的绝对时间',
    fallback_policy_count INT NOT NULL DEFAULT 0 COMMENT '降级策略成功落库次数',
    group_fallback_count INT NOT NULL DEFAULT 0 COMMENT '实际切换Group次数',

    dispatcher_instance VARCHAR(128) DEFAULT NULL COMMENT '调度实例标识',
    worker_instance VARCHAR(128) DEFAULT NULL COMMENT '执行实例标识',
    worker_thread VARCHAR(128) DEFAULT NULL COMMENT '执行线程标识',

    heartbeat_time DATETIME DEFAULT NULL COMMENT '最近心跳时间（DB真相）',
    start_time DATETIME DEFAULT NULL COMMENT '开始执行时间',
    finish_time DATETIME DEFAULT NULL COMMENT '执行完成时间',

    version INT NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',

    error_code VARCHAR(64) DEFAULT NULL COMMENT '错误码',
    error_msg VARCHAR(1024) DEFAULT NULL COMMENT '错误信息',

    ext_info TEXT DEFAULT NULL COMMENT '扩展信息（字符串）',

    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    INDEX idx_group_route_status_time (group_code, dispatch_route, status, execute_at),
    INDEX idx_route_status_time (dispatch_route, status, execute_at),
    INDEX idx_status_time (status, execute_at),
    INDEX idx_status_wait_deadline (status, wait_deadline_at),
    INDEX idx_route_status_fallback (dispatch_route, status, fallback_check_at, id),
    INDEX idx_user_group_status (user_id, group_code, status),
    INDEX idx_heartbeat (status, heartbeat_time),
    INDEX idx_biz_type_biz_key (biz_type, biz_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='调度任务主表';

CREATE TABLE IF NOT EXISTS scheduler_task_group_fallback_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    task_id BIGINT NOT NULL COMMENT '调度任务ID',
    task_no VARCHAR(64) NOT NULL COMMENT '任务唯一号',
    source_group_code VARCHAR(64) NOT NULL COMMENT '切组前Group',
    target_group_code VARCHAR(64) NOT NULL COMMENT '切组后Group',
    previous_fallback_check_at DATETIME NOT NULL COMMENT '本次触发时间',
    next_fallback_check_at DATETIME DEFAULT NULL COMMENT '下次检查时间',
    task_status VARCHAR(32) NOT NULL COMMENT '切组时任务状态',
    fallback_count INT NOT NULL COMMENT '累计实际切组次数',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_task_time (task_id, create_time),
    INDEX idx_source_target_time (source_group_code, target_group_code, create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非运行任务Group切换审计日志';

CREATE TABLE IF NOT EXISTS scheduler_task_dependency (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    task_id BIGINT NOT NULL COMMENT '当前任务ID',
    depends_on_task_id BIGINT NOT NULL COMMENT '依赖的上游任务ID',
    target_state VARCHAR(32) NOT NULL COMMENT '期望上游状态：SUCCESS/FAILED/TERMINAL',
    status VARCHAR(32) NOT NULL COMMENT '依赖状态：WAITING/SATISFIED/IMPOSSIBLE',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    UNIQUE KEY uk_task_dependency (task_id, depends_on_task_id),
    INDEX idx_depends_on_status (depends_on_task_id, status),
    INDEX idx_task_status (task_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务依赖关系表';

CREATE TABLE IF NOT EXISTS scheduler_group_config (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    group_code VARCHAR(64) NOT NULL UNIQUE COMMENT '任务组编码',

    enabled TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用：1启用，0禁用',
    max_concurrency INT NOT NULL COMMENT 'group最大并发',
    user_base_concurrency INT NOT NULL COMMENT 'user基础并发',

    dynamic_user_limit_enabled TINYINT NOT NULL DEFAULT 0 COMMENT '是否启用动态user并发策略',
    load_strategy_json JSON DEFAULT NULL COMMENT '动态策略配置JSON',

    dispatch_batch_size INT NOT NULL DEFAULT 20 COMMENT '每轮调度批大小',
    heartbeat_timeout_sec INT NOT NULL DEFAULT 90 COMMENT '心跳超时阈值（秒）',
    lock_expire_sec INT NOT NULL DEFAULT 120 COMMENT '任务租约过期时间（秒）',

    description VARCHAR(255) DEFAULT NULL COMMENT '配置描述',

    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='调度组配置表';

CREATE TABLE IF NOT EXISTS scheduler_task_execution (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    task_id BIGINT NOT NULL COMMENT '任务ID',
    task_no VARCHAR(64) NOT NULL COMMENT '任务唯一号',
    group_code VARCHAR(64) NOT NULL COMMENT '任务组编码',
    user_id VARCHAR(64) NOT NULL COMMENT '用户ID',

    execute_no VARCHAR(64) NOT NULL COMMENT '执行实例号（一次执行一次号）',
    status VARCHAR(32) NOT NULL COMMENT '执行状态：RUNNING/WAIT_HOLD/SUCCESS/FAILED/WAIT_RETRY',

    dispatcher_instance VARCHAR(128) DEFAULT NULL COMMENT '调度实例标识',
    worker_instance VARCHAR(128) DEFAULT NULL COMMENT '执行实例标识',

    start_time DATETIME NOT NULL COMMENT '开始时间',
    finish_time DATETIME DEFAULT NULL COMMENT '结束时间',
    duration_ms BIGINT DEFAULT NULL COMMENT '执行耗时（毫秒）',

    error_code VARCHAR(64) DEFAULT NULL COMMENT '错误码',
    error_msg VARCHAR(1024) DEFAULT NULL COMMENT '错误信息',

    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    UNIQUE KEY uk_execute_no (execute_no),
    INDEX idx_task_id (task_id),
    INDEX idx_group_time (group_code, start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行记录表';
