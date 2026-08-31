package org.dong.scheduler.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.UserConcurrencyConfig;
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

    @Test
    void shouldUseUserBaseConcurrencyAndLoadStrategyWhenConfigured() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setMaxConcurrency(10);
        groupConfig.setUserBaseConcurrency(2);
        groupConfig.setDynamicUserLimitEnabled(true);
        groupConfig.setLoadStrategyJson("""
                {"enabled":true,"maxLimit":20,"minLimit":1,"rounding":"FLOOR",
                 "rules":[{"factor":2.0,"loadLt":1.0}]}
                """);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserBaseConcurrency(3);
        userConfig.setDynamicUserLimitEnabled(true);
        userConfig.setLoadStrategyJson("""
                {"enabled":true,"maxLimit":20,"minLimit":1,"rounding":"FLOOR",
                 "rules":[{"factor":3.0,"loadLt":1.0}]}
                """);

        assertEquals(9, service.calculate(groupConfig, userConfig, 0));
    }

    @Test
    void shouldUseUserDynamicSwitchWhenUserConfigExists() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setMaxConcurrency(10);
        groupConfig.setDynamicUserLimitEnabled(false);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserBaseConcurrency(3);
        userConfig.setDynamicUserLimitEnabled(true);
        userConfig.setLoadStrategyJson("""
                {"enabled":true,"maxLimit":20,"minLimit":1,"rounding":"FLOOR",
                 "rules":[{"factor":3.0,"loadLt":1.0}]}
                """);

        assertEquals(9, service.calculate(groupConfig, userConfig, 0));
    }

    @Test
    void shouldDisableDynamicLimitWhenUserSwitchIsDisabled() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setMaxConcurrency(10);
        groupConfig.setDynamicUserLimitEnabled(true);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserBaseConcurrency(3);
        userConfig.setDynamicUserLimitEnabled(false);
        userConfig.setLoadStrategyJson("""
                {"enabled":true,"maxLimit":20,"minLimit":1,"rounding":"FLOOR",
                 "rules":[{"factor":3.0,"loadLt":1.0}]}
                """);

        assertEquals(3, service.calculate(groupConfig, userConfig, 0));
    }

    @Test
    void shouldKeepZeroUserLimitEvenWhenDynamicStrategyMinimumIsPositive() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setMaxConcurrency(10);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserBaseConcurrency(0);
        userConfig.setDynamicUserLimitEnabled(true);
        userConfig.setLoadStrategyJson("""
                {"enabled":true,"maxLimit":20,"minLimit":1,"rounding":"FLOOR",
                 "rules":[{"factor":3.0,"loadLt":1.0}]}
                """);

        assertEquals(0, service.calculate(groupConfig, userConfig, 0));
    }

    @Test
    void shouldFallBackToUserBaseConcurrencyWhenUserStrategyIsInvalid() {
        DynamicUserLimitService service = new DynamicUserLimitService(new ObjectMapper());

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setMaxConcurrency(10);
        groupConfig.setDynamicUserLimitEnabled(true);
        UserConcurrencyConfig userConfig = new UserConcurrencyConfig();
        userConfig.setUserBaseConcurrency(3);
        userConfig.setDynamicUserLimitEnabled(true);
        userConfig.setLoadStrategyJson("invalid-json");

        assertEquals(3, service.calculate(groupConfig, userConfig, 0));
    }
}
