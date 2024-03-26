package com.here.xyz.util.db.metrics;

import java.util.Map;

public record Metric(String type, Map<String, String> dimensions, Long value) {
}
