package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.GroupConfig;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class JdbcGroupConfigRepository implements GroupConfigRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcGroupConfigRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<GroupConfig> listEnabled() {
        return jdbcTemplate.query("""
                        select group_code, enabled, max_concurrency, user_base_concurrency,
                               dynamic_user_limit_enabled, load_strategy_json,
                               dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec
                        from scheduler_group_config
                        where enabled = 1
                        """,
                (rs, rowNum) -> {
                    GroupConfig c = new GroupConfig();
                    c.setGroupCode(rs.getString("group_code"));
                    c.setEnabled(rs.getInt("enabled") == 1);
                    c.setMaxConcurrency(rs.getInt("max_concurrency"));
                    c.setUserBaseConcurrency(rs.getInt("user_base_concurrency"));
                    c.setDynamicUserLimitEnabled(rs.getInt("dynamic_user_limit_enabled") == 1);
                    c.setLoadStrategyJson(rs.getString("load_strategy_json"));
                    c.setDispatchBatchSize(rs.getInt("dispatch_batch_size"));
                    c.setHeartbeatTimeoutSec(rs.getInt("heartbeat_timeout_sec"));
                    c.setLockExpireSec(rs.getInt("lock_expire_sec"));
                    return c;
                });
    }
}
