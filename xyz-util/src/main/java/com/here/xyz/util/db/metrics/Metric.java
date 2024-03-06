package com.here.xyz.util.db.metrics;

import java.util.Map;
import java.util.Objects;

public class Metric {

    private final Map<String, String> dimensions;

    private final Long value;

    public Metric(Map<String, String> dimensions, Long value) {
        this.dimensions = dimensions;
        this.value = value;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public Long getValue() {
        return value;
    }

    public int generateHash() {
        // 17 and 31 are prime numbers commonly used to make hash collisions less likely
        int result = 17;

        for (Map.Entry<String, String> entry : dimensions.entrySet()) {
            String dimension = entry.getKey();
            String value = entry.getValue();
            result = 31 * result + Objects.hash(dimension, value);
        }
        return result;
    }

}
