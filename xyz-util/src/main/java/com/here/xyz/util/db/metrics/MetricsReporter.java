package com.here.xyz.util.db.metrics;

public interface MetricsReporter {

    void reportMetric(Metric metric);

    void flush();

}
