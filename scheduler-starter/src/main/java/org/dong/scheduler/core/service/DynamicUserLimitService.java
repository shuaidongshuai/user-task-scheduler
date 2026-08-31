package org.dong.scheduler.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.LoadStrategy;
import org.dong.scheduler.core.model.UserConcurrencyConfig;

@Slf4j
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
        if (base <= 0) {
            return 0;
        }
        if (!dynamicUserLimitEnabled || loadStrategyJson == null || loadStrategyJson.isBlank()) {
            return base;
        }

        try {
            LoadStrategy strategy = objectMapper.readValue(loadStrategyJson, LoadStrategy.class);
            if (!strategy.isEnabled() || strategy.getRules().isEmpty()) {
                return base;
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
            return Math.max(0, Math.min(strategy.getMaxLimit(), limited));
        } catch (Exception e) {
            log.warn("failed to calculate dynamic user limit, group={}, user={}; fall back to base concurrency",
                    groupConfig.getGroupCode(), userConfig == null ? null : userConfig.getUserId(), e);
            return base;
        }
    }
}
