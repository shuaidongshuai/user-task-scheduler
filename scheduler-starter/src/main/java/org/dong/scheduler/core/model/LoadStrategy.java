package org.dong.scheduler.core.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LoadStrategy {
    private boolean enabled;
    private String rounding = "FLOOR";
    private int minLimit = 1;
    private int maxLimit = 100;
    private List<Rule> rules = new ArrayList<>();

    @Data
    public static class Rule {
        private double loadLt;
        private double factor;
    }
}
