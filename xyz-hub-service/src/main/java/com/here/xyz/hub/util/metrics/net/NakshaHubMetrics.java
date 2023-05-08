package com.here.xyz.hub.util.metrics.net;

import com.here.xyz.hub.util.metrics.net.ConnectionMetrics.HubHttpClientMetrics;
import com.here.xyz.hub.util.metrics.net.ConnectionMetrics.HubTCPMetrics;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.HttpClientMetrics;
import io.vertx.core.spi.metrics.TCPMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHubMetrics implements VertxMetrics {

  @Override
  public @NotNull HttpClientMetrics<?, ?, ?, ?> createHttpClientMetrics(@NotNull HttpClientOptions options) {
    return new HubHttpClientMetrics();
  }

  @Override
  public @Nullable ClientMetrics<?, ?, ?, ?> createClientMetrics(@NotNull SocketAddress remoteAddress, @NotNull String type, @NotNull String namespace) {
    return null;
  }

  @Override
  public @NotNull TCPMetrics<?> createNetClientMetrics(@NotNull NetClientOptions options) {
    return new HubTCPMetrics();
  }
}
