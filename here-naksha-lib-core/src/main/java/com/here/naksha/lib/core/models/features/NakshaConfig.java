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
package com.here.naksha.lib.core.models.features;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The XYZ-Hub service configuration.
 */
@JsonTypeName(value = "Config")
public final class NakshaConfig extends XyzFeature {

  @JsonCreator
  NakshaConfig(
      @JsonProperty("id") @NotNull String id,
      @JsonProperty("appId") @Nullable String appId,
      @JsonProperty("author") @Nullable String author,
      @JsonProperty("httpPort") @Nullable Integer httpPort,
      @JsonProperty("hostname") @Nullable String hostname,
      @JsonProperty("endpoint") @Nullable String endpoint,
      @JsonProperty("env") @Nullable String env,
      @JsonProperty("webRoot") @Nullable String webRoot,
      @JsonProperty("jwtName") @Nullable String jwtName,
      @JsonProperty("debug") @Nullable Boolean debug) {
    super(id);
    if (httpPort != null && (httpPort < 0 || httpPort > 65535)) {
      currentLogger()
          .atError("Invalid port in Naksha configuration: {}")
          .add(httpPort)
          .log();
      httpPort = 7080;
    } else if (httpPort == null || httpPort == 0) {
      httpPort = 7080;
    }
    if (hostname == null || hostname.length() == 0) {
      try {
        hostname = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        currentLogger()
            .atError("Unable to resolve the hostname using Java's API.")
            .setCause(e)
            .log();
        hostname = "localhost";
      }
    }
    URL __endpoint = null;
    if (endpoint != null && endpoint.length() > 0) {
      try {
        __endpoint = new URL(endpoint);
      } catch (MalformedURLException e) {
        currentLogger()
            .atError("Invalid configuration of endpoint: {}")
            .add(endpoint)
            .setCause(e)
            .log();
      }
    }
    if (__endpoint == null) {
      try {
        //noinspection HttpUrlsUsage
        __endpoint = new URL("http://" + hostname + ":" + httpPort);
      } catch (MalformedURLException e) {
        currentLogger()
            .atError("Invalid hostname: {}")
            .add(hostname)
            .setCause(e)
            .log();
        hostname = "localhost";
        try {
          __endpoint = new URL("http://localhost:" + httpPort);
        } catch (MalformedURLException ignore) {
        }
      }
      assert __endpoint != null;
    }
    if (env == null) {
      env = "local";
    }

    this.appId = appId != null && appId.length() > 0 ? appId : "naksha";
    this.author = author;
    this.httpPort = httpPort;
    this.hostname = hostname;
    this.endpoint = __endpoint;
    this.env = env;
    this.webRoot = webRoot;
    this.jwtName = jwtName != null && !jwtName.isEmpty() ? jwtName : "jwt";
    this.debug = Boolean.TRUE.equals(debug);
  }

  /**
   * The port at which to listen for HTTP requests.
   */
  public final int httpPort;

  /**
   * The hostname to use to refer to this instance, if {@code null}, then auto-detected.
   */
  public final @NotNull String hostname;

  /**
   * The application-id to be used when modifying the admin-database.
   */
  public final @NotNull String appId;

  /**
   * The author to be used when modifying the admin-database.
   */
  public final @Nullable String author;

  /**
   * The public endpoint, for example "https://naksha.foo.com/". If {@code null}, then the hostname and HTTP port used.
   */
  public @NotNull URL endpoint;

  /**
   * The environment, for example "local", "dev", "e2e" or "prd".
   */
  public final @NotNull String env;

  /**
   * If set, then serving static files from this directory.
   */
  public final @Nullable String webRoot;

  /**
   * The JWT key files to be read from the disk ({@code "~/.config/naksha/auth/$<jwtName>.(key|pub)"}).
   */
  public final @NotNull String jwtName;

  /**
   * If debugging mode is enabled.
   */
  public boolean debug;
}
