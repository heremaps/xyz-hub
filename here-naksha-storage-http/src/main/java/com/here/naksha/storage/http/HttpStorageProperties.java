/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.storage.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A Http storage configuration as used by the {@link HttpStorage}.
 */
@AvailableSince(NakshaVersion.v2_0_12)
public class HttpStorageProperties extends XyzProperties {

  public static final Long DEF_CONNECTION_TIMEOUT_SEC = 20L;
  public static final Long DEF_SOCKET_TIMEOUT_SEC = 90L;
  public static final Map<String, String> DEFAULT_HEADERS = Map.of(
      "Content-Type", "application/json",
      "Accept-Encoding", "gzip");

  private static final String URL = "url";
  private static final String CONNECTION_TIMEOUT = "connectTimeout";
  private static final String SOCKET_TIMEOUT = "socketTimeout";
  private static final String HEADERS = "headers";

  @JsonProperty(URL)
  private @NotNull String url;

  @JsonProperty(CONNECTION_TIMEOUT)
  private @NotNull Long connectTimeout;

  @JsonProperty(SOCKET_TIMEOUT)
  private @NotNull Long socketTimeout;

  @JsonProperty(HEADERS)
  private @NotNull Map<String, String> headers;

  @JsonCreator
  public HttpStorageProperties(
      @JsonProperty(value = URL, required = true) @NotNull String url,
      @JsonProperty(CONNECTION_TIMEOUT) @Nullable Long connectTimeout,
      @JsonProperty(SOCKET_TIMEOUT) @Nullable Long socketTimeout,
      @JsonProperty(HEADERS) @Nullable Map<String, String> headers) {
    this.url = url;
    this.connectTimeout = connectTimeout == null ? DEF_CONNECTION_TIMEOUT_SEC : connectTimeout;
    this.socketTimeout = socketTimeout == null ? DEF_SOCKET_TIMEOUT_SEC : socketTimeout;
    this.headers = headers == null ? DEFAULT_HEADERS : headers;
  }

  /**
   * Points to the instance, not to an endpoint.
   */
  public @NotNull String getUrl() {
    return url;
  }

  public @NotNull Long getConnectTimeout() {
    return connectTimeout;
  }

  public @NotNull Long getSocketTimeout() {
    return socketTimeout;
  }

  public @NotNull Map<String, String> getHeaders() {
    return headers;
  }
}
