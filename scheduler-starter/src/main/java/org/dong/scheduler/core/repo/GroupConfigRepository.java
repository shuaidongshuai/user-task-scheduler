package org.dong.scheduler.core.repo;

import org.dong.scheduler.core.model.GroupConfig;

import java.util.List;

public interface GroupConfigRepository {
    List<GroupConfig> listEnabled();
}
