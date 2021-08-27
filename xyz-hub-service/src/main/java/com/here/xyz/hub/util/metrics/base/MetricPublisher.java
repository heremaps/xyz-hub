package com.here.xyz.hub.util.metrics.base;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MetricPublisher<V> {

  private static final Logger logger = LogManager.getLogger();
  public static final long DEFAULT_REFRESH_PERIOD = 60; //s

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private long startWaitTime;
  private long refreshPeriod = DEFAULT_REFRESH_PERIOD;
  private Metric metric;

  public MetricPublisher(Metric<V> metric) {
    this.metric = metric;
    executor.scheduleAtFixedRate(() -> {
      final V values = metric.gatherValues();
      if (values == null) {
        logger.debug("Nothing to publish for metric {}", getMetricName());
        return;
      }
      publishValues(values);
      logger.debug("Publishing values for metric {}", getMetricName());
    }, startWaitTime, refreshPeriod, TimeUnit.SECONDS);
    logger.info("Started publishing metric {}", getMetricName());
  }

  public MetricPublisher(Metric metric, long startWaitTime) {
    this(metric);
    this.startWaitTime = startWaitTime;
  }

  public MetricPublisher(Metric metric, long startWaitTime, long refreshPeriod) {
    this(metric, startWaitTime);
    this.refreshPeriod = refreshPeriod;
  }

  abstract void publishValues(V values);

  public String getMetricName() {
    return metric.getName();
  }

  public void stop() {
    executor.shutdown();
    logger.info("Stopped publishing metric {}", getMetricName());
  }

}
