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
package com.here.naksha.lib.core.models.features;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * An extension is an administrative feature that allows to run proprietary code, outside the Naksha-Hub using proprietary libraries.
 */
@AvailableSince(NakshaVersion.v2_0_3)
public class Extension extends XyzFeature {

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String NUMBER = "number";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String HOST = "host";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String PORT = "port";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String CONN_TIMEOUT = "connTimeout";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String READ_TIMEOUT = "readTimeout";

  /**
   * Create an extension.
   *
   * @param host   the host to contact.
   * @param number the number of the extension.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public Extension(@JsonProperty(HOST) @NotNull String host, @JsonProperty(NUMBER) int number) {
    this(Integer.toString(number), host, number, number);
  }

  /**
   * Create an extension.
   *
   * @param id     the unique identifier of the extension.
   * @param host   the host to contact.
   * @param number the number of the extension.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public Extension(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(HOST) @NotNull String host,
      @JsonProperty(NUMBER) int number) {
    this(id, host, number, number);
  }

  /**
   * Create an extension.
   *
   * @param id     the unique identifier of the extension.
   * @param host   the host to contact.
   * @param port   the port to contact.
   * @param number the number of the extension.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public Extension(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(HOST) @NotNull String host,
      @JsonProperty(PORT) int port,
      @JsonProperty(NUMBER) int number) {
    super(id);
    assert number >= 0 && number < 65536;
    this.number = number;
    this.host = host;
    assert port >= 0 && port < 65536;
    this.port = port;
  }

  /**
   * The extension number.
   */
  @JsonProperty(NUMBER)
  private int number;

  /**
   * The host of the extension.
   */
  @JsonProperty(HOST)
  private @NotNull String host;

  /**
   * The port of the extension.
   */
  @JsonProperty(HOST)
  private int port;

  /**
   * The connection-timeout in milliseconds.
   */
  @JsonProperty(CONN_TIMEOUT)
  private int connTimeout = (int) SECONDS.toMillis(5);

  /**
   * The read-timeout in milliseconds.
   */
  @JsonProperty(READ_TIMEOUT)
  private int readTimeout = (int) SECONDS.toMillis(30);

  /**
   * The extension number.
   *
   * @return the extension number.
   */
  public int getNumber() {
    return number;
  }

  public void setNumber(int number) {
    this.number = number;
  }

  /**
   * The host of the extension.
   *
   * @return the host of the extension.
   */
  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  /**
   * The port to connect to.
   *
   * @return the port to connect to.
   */
  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  /**
   * The timeout in milliseconds when establishing a connection.
   *
   * @return timeout in milliseconds when establishing a connection.
   */
  public int getConnTimeout() {
    return connTimeout;
  }

  public void setConnTimeout(int connTimeout) {
    this.connTimeout = connTimeout;
  }

  /**
   * The timeout in milliseconds when reading from an extension connection.
   *
   * @return timeout in milliseconds when reading from an extension connection.
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }
}
