package org.dong.scheduler.core.spi;

import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.model.SchedulerTask;

public interface BusinessTaskStateProvider {
    String bizType();

    BusinessTaskState query(SchedulerTask task);
}
