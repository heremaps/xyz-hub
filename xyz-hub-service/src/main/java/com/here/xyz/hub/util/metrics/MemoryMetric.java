package com.here.xyz.hub.util.metrics;

import com.here.xyz.hub.Service;
import java.util.Collection;
import java.util.Collections;

public class MemoryMetric extends Metric {

  public MemoryMetric(MetricPublisher publisher) {
    super(publisher, 30);
  }

  @Override
  Collection<Double> gatherValues() {
    return Collections.singleton((double) Service.getUsedMemoryPercent());
  }
}
