package com.here.xyz.util.db.metrics;

import java.util.Map;

public record Metric(Map<String, String> dimensions, String type, Long value) {
}
