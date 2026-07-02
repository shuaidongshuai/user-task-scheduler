package org.dong.scheduler.core.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LoadStrategy {
    private boolean enabled;
    private String rounding = "FLOOR";
    @JsonAlias({"minLimit", "min_limit"})
    private int minLimit = 1;
    @JsonAlias({"maxLimit", "max_limit"})
    private int maxLimit = 100;
    private List<Rule> rules = new ArrayList<>();

    @Data
    public static class Rule {
        @JsonAlias({"loadLt", "load_lt"})
        private double loadLt;
        private double factor;
    }
}
