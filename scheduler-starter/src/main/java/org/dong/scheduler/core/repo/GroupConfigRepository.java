package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.GroupConfig;

import java.util.List;
import java.util.Optional;

public interface GroupConfigRepository {
    List<GroupConfig> listEnabled();

    Optional<GroupConfig> findEnabledByGroupCode(String groupCode);
}
