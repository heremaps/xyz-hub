package com.here.xyz.hub.util.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.here.xyz.hub.Service;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudWatchMetricPublisher extends MetricPublisher {

  private static final Logger logger = LogManager.getLogger();

  private static String region;
  private static AmazonCloudWatch _client;

  String namespace;
  Dimension dimension;
  StandardUnit unit = StandardUnit.None;

  public CloudWatchMetricPublisher(String namespace, String metricName, String dimensionName, String dimensionValue, StandardUnit unit) {
    this(namespace, metricName, dimensionName, dimensionValue);
    this.unit = unit;
    region = Service.configuration != null && Service.configuration.AWS_REGION != null ? Service.configuration.AWS_REGION : "none";
  }

  public CloudWatchMetricPublisher(String namespace, String metricName, String dimensionName, String dimensionValue) {
    super(metricName);
    this.namespace = namespace;
    dimension = new Dimension()
        .withName(dimensionName)
        .withValue(dimensionValue);
  }

  @Override
  void publishValues(Collection<Double> values) {
    MetricDatum datum = new MetricDatum()
        .withMetricName(getMetricName())
        .withUnit(unit)
        .withValues(values)
        .withDimensions(dimension, new Dimension().withName("region").withValue(region));

    PutMetricDataRequest request = new PutMetricDataRequest()
        .withNamespace(namespace)
        .withMetricData(datum);

    try {
      getClient().putMetricData(request);
    }
    catch (Exception e) {
      logger.error("Error publishing metric {}: {}", getMetricName(), e);
    }
  }

  private static AmazonCloudWatch getClient() {
    if (_client == null)
      _client = AmazonCloudWatchAsyncClientBuilder.defaultClient();
    return _client;
  }
}
