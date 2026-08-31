package org.dong.scheduler.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.LoadStrategy;
import org.dong.scheduler.core.model.UserConcurrencyConfig;

public class DynamicUserLimitService {
    private final ObjectMapper objectMapper;

    public DynamicUserLimitService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public int calculate(GroupConfig config, long groupRunning) {
        return calculate(config, null, groupRunning);
    }

    public int calculate(GroupConfig groupConfig, UserConcurrencyConfig userConfig, long groupRunning) {
        int base = userConfig == null
                ? groupConfig.getUserBaseConcurrency()
                : userConfig.getUserBaseConcurrency();
        String loadStrategyJson = userConfig == null
                ? groupConfig.getLoadStrategyJson()
                : userConfig.getLoadStrategyJson();
        boolean dynamicUserLimitEnabled = userConfig == null
                ? groupConfig.isDynamicUserLimitEnabled()
                : userConfig.isDynamicUserLimitEnabled();
        if (!dynamicUserLimitEnabled || loadStrategyJson == null || loadStrategyJson.isBlank()) {
            return Math.max(base, 1);
        }

        try {
            LoadStrategy strategy = objectMapper.readValue(loadStrategyJson, LoadStrategy.class);
            if (!strategy.isEnabled() || strategy.getRules().isEmpty()) {
                return Math.max(base, 1);
            }

            double load = groupConfig.getMaxConcurrency() <= 0
                    ? 1.0
                    : (double) groupRunning / groupConfig.getMaxConcurrency();
            double factor = 1.0;
            for (LoadStrategy.Rule r : strategy.getRules()) {
                if (load < r.getLoadLt()) {
                    factor = r.getFactor();
                    break;
                }
            }

            double raw = base * factor;
            int rounded = switch (strategy.getRounding().toUpperCase()) {
                case "CEIL" -> (int) Math.ceil(raw);
                case "ROUND" -> (int) Math.round(raw);
                default -> (int) Math.floor(raw);
            };
            int limited = Math.max(strategy.getMinLimit(), rounded);
            return Math.max(1, Math.min(strategy.getMaxLimit(), limited));
        } catch (Exception e) {
            return Math.max(base, 1);
        }
    }
}
