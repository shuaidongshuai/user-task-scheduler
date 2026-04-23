package org.dong.scheduler.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.LoadStrategy;

public class DynamicUserLimitService {
    private final ObjectMapper objectMapper;

    public DynamicUserLimitService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public int calculate(GroupConfig config, long groupRunning) {
        int base = config.getUserBaseConcurrency();
        if (!config.isDynamicUserLimitEnabled() || config.getLoadStrategyJson() == null || config.getLoadStrategyJson().isBlank()) {
            return Math.max(base, 1);
        }

        try {
            LoadStrategy strategy = objectMapper.readValue(config.getLoadStrategyJson(), LoadStrategy.class);
            if (!strategy.isEnabled() || strategy.getRules().isEmpty()) {
                return Math.max(base, 1);
            }

            double load = config.getMaxConcurrency() <= 0 ? 1.0 : (double) groupRunning / config.getMaxConcurrency();
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
