package com.here.xyz.hub.util.metrics;

import com.here.xyz.hub.task.FeatureTaskHandler;
import java.util.Collection;
import java.util.Collections;

public class GlobalInflightRequestMemory extends Metric {

  public GlobalInflightRequestMemory(MetricPublisher publisher) {
    super(publisher, 30);
  }

  @Override
  Collection<Double> gatherValues() {
    return Collections.singleton((double) FeatureTaskHandler.getGlobalInflightRequestMemory());
  }
}
