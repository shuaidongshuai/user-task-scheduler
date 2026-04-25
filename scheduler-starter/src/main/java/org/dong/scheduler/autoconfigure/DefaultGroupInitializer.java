package org.dong.scheduler.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.config.SchedulerProperties;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class DefaultGroupInitializer implements ApplicationRunner {
    private final SchedulerProperties properties;
    private final JdbcTemplate jdbcTemplate;

    public DefaultGroupInitializer(SchedulerProperties properties, JdbcTemplate jdbcTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isAutoInitDefaultGroup()) {
            return;
        }
        String defaultGroupCode = properties.getDefaultGroupCode();
        if (defaultGroupCode == null || defaultGroupCode.isBlank()) {
            throw new IllegalArgumentException("scheduler.default-group-code must not be blank");
        }
        jdbcTemplate.update("""
                insert into scheduler_group_config(
                    group_code, enabled, max_concurrency, user_base_concurrency,
                    dynamic_user_limit_enabled, load_strategy_json,
                    dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec,
                    description, create_time, update_time
                ) values (?, 1, ?, ?, 0, null, ?, ?, ?, ?, now(), now())
                on duplicate key update group_code = group_code
                """,
                defaultGroupCode,
                properties.getDefaultGroupMaxConcurrency(),
                properties.getDefaultGroupUserBaseConcurrency(),
                properties.getDefaultGroupDispatchBatchSize(),
                properties.getDefaultGroupHeartbeatTimeoutSec(),
                properties.getDefaultGroupLockExpireSec(),
                properties.getDefaultGroupDescription());

        log.info("default scheduler group ensured, groupCode={}", defaultGroupCode);
    }
}
