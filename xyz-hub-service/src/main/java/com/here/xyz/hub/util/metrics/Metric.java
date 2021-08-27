package com.here.xyz.hub.util.metrics;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Metric {

  public static final long DEFAULT_REFRESH_PERIOD = 60; //s
  private static final Logger logger = LogManager.getLogger();

  final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  long startWaitTime;
  long refreshPeriod = DEFAULT_REFRESH_PERIOD;
  private MetricPublisher publisher;

  public Metric(MetricPublisher publisher) {
    this.publisher = publisher;
    executor.scheduleAtFixedRate(() -> {
      final Collection<Double> values = gatherValues();
      if (values == null || values.isEmpty()) {
        logger.debug("Nothing to publish for metric {}", publisher.getMetricName());
        return;
      }
      publishValues(values);
      logger.debug("Publishing values for metric {}", publisher.getMetricName());
    }, startWaitTime, refreshPeriod, TimeUnit.SECONDS);
    logger.info("Started publishing metric {}", publisher.getMetricName());
  }

  public Metric(MetricPublisher publisher, long startWaitTime) {
    this(publisher);
    this.startWaitTime = startWaitTime;
  }

  public Metric(MetricPublisher publisher, long startWaitTime, long refreshPeriod) {
    this(publisher, startWaitTime);
    this.refreshPeriod = refreshPeriod;
  }

  abstract Collection<Double> gatherValues();

  private void publishValues(Collection<Double> values) {
    publisher.publishValues(values);
  }

  public void stop() {
    logger.info("Stopped publishing metric {}", publisher.getMetricName());
    executor.shutdown();
  }

}
