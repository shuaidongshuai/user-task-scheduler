package org.dong.scheduler.core.model;

import lombok.Data;

@Data
public class UserConcurrencyConfig {
    private String userId;
    private String groupCode;
    private int userBaseConcurrency;
    private boolean dynamicUserLimitEnabled;
    private String loadStrategyJson;
}
