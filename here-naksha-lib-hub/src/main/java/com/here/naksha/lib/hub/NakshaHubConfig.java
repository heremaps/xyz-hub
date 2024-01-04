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
package com.here.naksha.lib.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Naksha-Hub service configuration.
 */
@JsonTypeName(value = "Config")
public final class NakshaHubConfig extends XyzFeature implements JsonSerializable {

  private static final Logger logger = LoggerFactory.getLogger(NakshaHubConfig.class);

  /**
   * The default application name, used for example as identifier when accessing the PostgresQL database and to read the configuration file
   * using the <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XGD</a> standard, therefore from
   * directory ({@code ~/.config/<APP_NAME>/...}).
   */
  public static final @NotNull String APP_NAME = "naksha";

  /**
   * Returns a default application name used at many placed.
   *
   * @return The default application name.
   */
  public static @NotNull String defaultAppName() {
    return APP_NAME + "/v" + NakshaVersion.latest;
  }

  /**
   * Returns a className of default NakshaHub instance
   *
   * @return The default NakshaHub className
   */
  public static @NotNull String defaultHubClassName() {
    return NakshaHub.class.getName();
  }

  @JsonCreator
  NakshaHubConfig(
      @JsonProperty("id") @NotNull String id,
      @JsonProperty("hubClassName") @Nullable String hubClassName,
      @JsonProperty("userAgent") @Nullable String userAgent,
      @JsonProperty("appId") @Nullable String appId,
      @JsonProperty("author") @Nullable String author,
      @JsonProperty("httpPort") @Nullable Integer httpPort,
      @JsonProperty("hostname") @Nullable String hostname,
      @JsonProperty("endpoint") @Nullable String endpoint,
      @JsonProperty("env") @Nullable String env,
      @JsonProperty("webRoot") @Nullable String webRoot,
      @JsonProperty("jwtName") @Nullable String jwtName,
      @JsonProperty("debug") @Nullable Boolean debug,
      @JsonProperty("maintenanceIntervalInMins") @Nullable Integer maintenanceIntervalInMins,
      @JsonProperty("maintenanceInitialDelayInMins") @Nullable Integer maintenanceInitialDelayInMins,
      @JsonProperty("maintenancePoolCoreSize") @Nullable Integer maintenancePoolCoreSize,
      @JsonProperty("maintenancePoolMaxSize") @Nullable Integer maintenancePoolMaxSize,
      @JsonProperty("storageParams") @Nullable Map<String, Object> storageParams) {
    super(id);
    if (httpPort != null && (httpPort < 0 || httpPort > 65535)) {
      logger.atError()
          .setMessage("Invalid port in Naksha configuration: {}")
          .addArgument(httpPort)
          .log();
      httpPort = 8080;
    } else if (httpPort == null || httpPort == 0) {
      httpPort = 8080;
    }
    if (hostname == null || hostname.length() == 0) {
      try {
        hostname = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        logger.atError()
            .setMessage("Unable to resolve the hostname using Java's API.")
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
        logger.atError()
            .setMessage("Invalid configuration of endpoint: {}")
            .addArgument(endpoint)
            .setCause(e)
            .log();
      }
    }
    if (__endpoint == null) {
      try {
        //noinspection HttpUrlsUsage
        __endpoint = new URL("http://" + hostname + ":" + httpPort);
      } catch (MalformedURLException e) {
        logger.atError()
            .setMessage("Invalid hostname: {}")
            .addArgument(hostname)
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

    this.hubClassName = (hubClassName != null && !hubClassName.isEmpty()) ? hubClassName : defaultHubClassName();
    this.appId = appId != null && appId.length() > 0 ? appId : "naksha";
    this.author = author;
    this.httpPort = httpPort;
    this.hostname = hostname;
    this.endpoint = __endpoint;
    this.env = env;
    this.webRoot = webRoot;
    this.jwtName = jwtName != null && !jwtName.isEmpty() ? jwtName : "jwt";
    this.userAgent = userAgent != null && !userAgent.isEmpty() ? userAgent : defaultAppName();
    this.debug = Boolean.TRUE.equals(debug);
    this.maintenanceIntervalInMins =
        maintenanceIntervalInMins != null ? maintenanceIntervalInMins : defaultMaintenanceIntervalInMins();
    this.maintenanceInitialDelayInMins = maintenanceInitialDelayInMins != null
        ? maintenanceInitialDelayInMins
        : defaultMaintenanceInitialDelayInMins();
    this.maintenancePoolCoreSize =
        maintenancePoolCoreSize != null ? maintenancePoolCoreSize : defaultMaintenancePoolCoreSize();
    this.maintenancePoolMaxSize =
        maintenancePoolMaxSize != null ? maintenancePoolMaxSize : defaultMaintenancePoolMaxSize();
    this.storageParams = storageParams;
  }

  public static final String HTTP_PORT = "httpPort";

  /**
   * The port at which to listen for HTTP requests.
   */
  @JsonProperty(HTTP_PORT)
  public final int httpPort;

  public static final String HOSTNAME = "hostname";

  /**
   * The hostname to use to refer to this instance, if {@code null}, then auto-detected.
   */
  @JsonProperty(HOSTNAME)
  public final @NotNull String hostname;

  public static final String APP_ID = "appId";

  /**
   * The application-id to be used when modifying the admin-database.
   */
  @JsonProperty(APP_ID)
  public final @NotNull String appId;

  public static final String AUTHOR = "author";

  /**
   * The author to be used when modifying the admin-database.
   */
  @JsonProperty(AUTHOR)
  public final @Nullable String author;

  public static final String ENDPOINT = "endpoint";

  /**
   * The public endpoint, for example "https://naksha.foo.com/". If {@code null}, then the hostname and HTTP port used.
   */
  @JsonProperty(ENDPOINT)
  public @NotNull URL endpoint;

  public static final String ENV = "env";

  /**
   * The environment, for example "local", "dev", "e2e" or "prd".
   */
  @JsonProperty(ENV)
  public final @NotNull String env;

  public static final String WEB_ROOT = "webRoot";

  /**
   * If set, then serving static files from this directory.
   */
  @JsonProperty(WEB_ROOT)
  public final @Nullable String webRoot;

  public static final String JWT_NAME = "jwtName";

  /**
   * The JWT key files to be read from the disk ({@code "~/.config/naksha/auth/$<jwtName>.(key|pub)"}).
   */
  @JsonProperty(JWT_NAME)
  public final @NotNull String jwtName;

  public static final String USER_AGENT = "userAgent";

  /**
   * The user-agent to be used for external communication.
   */
  @JsonProperty(USER_AGENT)
  public final @NotNull String userAgent;

  public static final String DEBUG = "debug";

  /**
   * If debugging mode is enabled.
   */
  @JsonProperty(DEBUG)
  @JsonInclude(Include.NON_DEFAULT)
  public boolean debug;

  public static final String HUB_CLASS_NAME = "hubClassName";

  /**
   * The fully qualified class name to be used to initiate NakshaHub instance
   */
  @JsonProperty(HUB_CLASS_NAME)
  public final @NotNull String hubClassName;

  /**
   * The initial delay (in minutes) after the service start up, when the Storage Maintenance job should perform first execution
   */
  public final int maintenanceInitialDelayInMins;

  /**
   * Returns a default initial delay in mins, for starting Storage Maintenance job.
   *
   * @return The default interval
   */
  public static int defaultMaintenanceInitialDelayInMins() {
    return 1 * 60; // 1 hour
  }

  /**
   * The interval in minutes, with which the Storage Maintenance job is to be scheduled
   */
  public final int maintenanceIntervalInMins;

  /**
   * Returns a default interval in mins, for scheduling Storage Maintenance job.
   *
   * @return The default interval
   */
  public static int defaultMaintenanceIntervalInMins() {
    return 12 * 60; // 12 hours
  }

  /**
   * The initial size of thread pool for running Storage Maintenance jobs in parallel
   */
  public final int maintenancePoolCoreSize;

  /**
   * Returns a default initial size of thread pool for running Storage Maintenance jobs in parallel
   *
   * @return the default core size of maintenance thread pool
   */
  public static int defaultMaintenancePoolCoreSize() {
    return 5;
  }

  /**
   * The maximum size of thread pool for running Storage Maintenance jobs in parallel
   */
  public final int maintenancePoolMaxSize;

  /**
   * Returns a default maximum size of thread pool for running Storage Maintenance jobs in parallel
   *
   * @return the default max size of maintenance thread pool
   */
  public static int defaultMaintenancePoolMaxSize() {
    return 5;
  }

  /**
   * Optional storage-specific parameters
   */
  public final Map<String, Object> storageParams;
}
