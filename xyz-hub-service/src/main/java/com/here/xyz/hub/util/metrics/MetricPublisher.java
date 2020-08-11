package com.here.xyz.hub.util.metrics;

import java.util.Collection;

public abstract class MetricPublisher {

  private final String metricName;

  protected MetricPublisher(String metricName) {
    this.metricName = metricName;
  }

  abstract void publishValues(Collection<Double> values);

  public String getMetricName() {
    return metricName;
  }

}
