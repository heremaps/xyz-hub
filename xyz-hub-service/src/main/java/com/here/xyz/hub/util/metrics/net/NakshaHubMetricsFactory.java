package com.here.xyz.hub.util.metrics.net;

import io.vertx.core.VertxOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.VertxMetrics;
import org.jetbrains.annotations.NotNull;

public class NakshaHubMetricsFactory implements VertxMetricsFactory {

  @Override
  public @NotNull VertxMetrics metrics(@NotNull VertxOptions options) {
    return new NakshaHubMetrics();
  }
}
