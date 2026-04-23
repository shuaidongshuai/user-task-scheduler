package org.dong.scheduler.core.model;

import lombok.Data;

@Data
public class GroupConfig {
    private String groupCode;
    private boolean enabled;
    private int maxConcurrency;
    private int userBaseConcurrency;
    private boolean dynamicUserLimitEnabled;
    private String loadStrategyJson;
    private int dispatchBatchSize;
    private int heartbeatTimeoutSec;
    private int lockExpireSec;
}
