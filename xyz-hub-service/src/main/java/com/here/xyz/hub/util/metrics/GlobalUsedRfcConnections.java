package com.here.xyz.hub.util.metrics;

import com.here.xyz.hub.connectors.RemoteFunctionClient;
import java.util.Collection;
import java.util.Collections;

public class GlobalUsedRfcConnections extends Metric {

  public GlobalUsedRfcConnections(MetricPublisher publisher) {
    super(publisher, 30);
  }

  @Override
  Collection<Double> gatherValues() {
    return Collections.singleton((double) RemoteFunctionClient.getGlobalUsedConnectionsPercentage());
  }
}
