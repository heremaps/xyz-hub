/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
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
  public @Nullable ClientMetrics<?, ?, ?, ?> createClientMetrics(
      @NotNull SocketAddress remoteAddress, @NotNull String type, @NotNull String namespace) {
    return null;
  }

  @Override
  public @NotNull TCPMetrics<?> createNetClientMetrics(@NotNull NetClientOptions options) {
    return new HubTCPMetrics();
  }
}
