package com.here.xyz.util.db.metrics;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class StorageMetric {

    private final Map<String, String> dimensions;

    private final LongAdder value;

    public StorageMetric(Map<String, String> dimensions, LongAdder value) {
        this.dimensions = dimensions;
        this.value = value;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void addToValue(long valueToAdd) {
        value.add(valueToAdd);
    }

    public long getValue() {
        return value.sum();
    }
}
