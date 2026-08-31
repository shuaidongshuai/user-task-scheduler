package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.UserConcurrencyConfig;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Optional;

public class JdbcUserConcurrencyConfigRepository implements UserConcurrencyConfigRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcUserConcurrencyConfigRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Optional<UserConcurrencyConfig> findByUserIdAndGroupCode(String userId, String groupCode) {
        List<UserConcurrencyConfig> configs = jdbcTemplate.query("""
                        select user_id, group_code, user_base_concurrency,
                               dynamic_user_limit_enabled, load_strategy_json
                          from scheduler_user_concurrency_config
                         where user_id = ? and group_code = ?
                        """,
                (rs, rowNum) -> {
                    UserConcurrencyConfig config = new UserConcurrencyConfig();
                    config.setUserId(rs.getString("user_id"));
                    config.setGroupCode(rs.getString("group_code"));
                    config.setUserBaseConcurrency(rs.getInt("user_base_concurrency"));
                    config.setDynamicUserLimitEnabled(rs.getInt("dynamic_user_limit_enabled") == 1);
                    config.setLoadStrategyJson(rs.getString("load_strategy_json"));
                    return config;
                },
                userId, groupCode);
        return configs.stream().findFirst();
    }
}
