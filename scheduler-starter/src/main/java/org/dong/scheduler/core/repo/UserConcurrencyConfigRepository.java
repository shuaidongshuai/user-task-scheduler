package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.UserConcurrencyConfig;

import java.util.Optional;

public interface UserConcurrencyConfigRepository {
    Optional<UserConcurrencyConfig> findByUserIdAndGroupCode(String userId, String groupCode);
}
