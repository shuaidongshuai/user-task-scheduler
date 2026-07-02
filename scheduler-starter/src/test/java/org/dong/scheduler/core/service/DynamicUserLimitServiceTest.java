package org.dong.scheduler.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.dong.scheduler.core.model.GroupConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DynamicUserLimitServiceTest {

    @Test
    void shouldDeserializeCamelCaseStrategyWhenObjectMapperUsesSnakeCase() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        DynamicUserLimitService service = new DynamicUserLimitService(objectMapper);

        GroupConfig config = new GroupConfig();
        config.setMaxConcurrency(10);
        config.setUserBaseConcurrency(2);
        config.setDynamicUserLimitEnabled(true);
        config.setLoadStrategyJson("""
                {
                  "rules": [
                    {"factor": 2.5, "loadLt": 0.5},
                    {"factor": 1.5, "loadLt": 0.7},
                    {"factor": 1.0, "loadLt": 0.9},
                    {"factor": 0.5, "loadLt": 999.0}
                  ],
                  "enabled": true,
                  "maxLimit": 50,
                  "minLimit": 1,
                  "rounding": "FLOOR"
                }
                """);

        assertEquals(5, service.calculate(config, 0));

        config.setUserBaseConcurrency(100);
        assertEquals(50, service.calculate(config, 0));
    }

    @Test
    void shouldDeserializeSnakeCaseStrategyWhenObjectMapperUsesDefaultNaming() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig config = new GroupConfig();
        config.setMaxConcurrency(10);
        config.setUserBaseConcurrency(2);
        config.setDynamicUserLimitEnabled(true);
        config.setLoadStrategyJson("""
                {
                  "rules": [
                    {"factor": 2.5, "load_lt": 0.5}
                  ],
                  "enabled": true,
                  "max_limit": 50,
                  "min_limit": 1,
                  "rounding": "FLOOR"
                }
                """);

        assertEquals(5, service.calculate(config, 0));
    }
}
