package com.here.xyz.util.db.metrics;

import java.util.Map;

public class Metric {

    private final Map<String, String> dimensions;

    private String name;

    private final Long value;

    public Metric(Map<String, String> dimensions, String name, Long value) {
        this.dimensions = dimensions;
        this.name = name;
        this.value = value;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public Long getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
