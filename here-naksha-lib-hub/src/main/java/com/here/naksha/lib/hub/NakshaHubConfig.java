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
package com.here.naksha.lib.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
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
public final class NakshaHubConfig extends XyzFeature implements JsonSerializable {

  private static final Logger logger = LoggerFactory.getLogger(NakshaHubConfig.class);

  /**
   * The default application name, used for example as identifier when accessing the PostgresQL database and to read the configuration file
   * using the <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XGD</a> standard, therefore from
   * directory ({@code ~/.config/<APP_NAME>/...}).
   */
  public static final @NotNull String APP_NAME = "naksha";

  private static final String NAKSHA_ENV = "NAKSHA_ENV";

  /**
   * The default Http request body limit in MB.
   */
  public static final Integer DEF_REQ_BODY_LIMIT = 25;

  /**
   * The maximum supported Http request body limit in MB.
   */
  public static final Integer MAX_REQ_BODY_LIMIT = Math.max(25, DEF_REQ_BODY_LIMIT);

  /**
   * The default Http request header limit in KB.
   */
  public static final Integer DEF_REQ_HEADER_LIMIT = 8;

  /**
   * The maximum supported Http request header limit in KB.
   */
  public static final Integer MAX_REQ_HEADER_LIMIT = Math.max(16, DEF_REQ_HEADER_LIMIT);

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

  /**
   * Returns a default relative path to Private Key useful for JWT signing
   *
   * @return The default private key path
   */
  public static @NotNull String defaultJwtPvtKeyPath() {
    return "auth/jwt.key";
  }

  /**
   * Returns default relative paths to Public Keys useful for JWT signature verification
   *
   * @return The default public key paths
   */
  public static @NotNull String defaultJwtPubKeyPaths() {
    return "auth/jwt.pub";
  }

  @JsonCreator
  NakshaHubConfig(
      @JsonProperty("id") @NotNull String id,
      @JsonProperty(HUB_CLASS_NAME) @Nullable String hubClassName,
      @JsonProperty(USER_AGENT) @Nullable String userAgent,
      @JsonProperty(APP_ID) @Nullable String appId,
      @JsonProperty(AUTHOR) @Nullable String author,
      @JsonProperty(HTTP_PORT) @Nullable Integer httpPort,
      @JsonProperty(HOSTNAME) @Nullable String hostname,
      @JsonProperty(ENDPOINT) @Nullable String endpoint,
      @JsonProperty(ENV) @Nullable String env,
      @JsonProperty(WEB_ROOT) @Nullable String webRoot,
      @JsonProperty(NAKSHA_AUTH) @Nullable AuthorizationMode authMode,
      @JsonProperty(JWT_PVT_KEY_PATH) @Nullable String jwtPvtKeyPath,
      @JsonProperty(JWT_PUB_KEY_PATHS) @Nullable String jwtPubKeyPaths,
      @JsonProperty(DEBUG) @Nullable Boolean debug,
      @JsonProperty("maintenanceIntervalInMins") @Nullable Integer maintenanceIntervalInMins,
      @JsonProperty("maintenanceInitialDelayInMins") @Nullable Integer maintenanceInitialDelayInMins,
      @JsonProperty("maintenancePoolCoreSize") @Nullable Integer maintenancePoolCoreSize,
      @JsonProperty("maintenancePoolMaxSize") @Nullable Integer maintenancePoolMaxSize,
      @JsonProperty("storageParams") @Nullable Map<String, Object> storageParams,
      @JsonProperty("extensionConfigParams") @Nullable ExtensionConfigParams extensionConfigParams,
      @JsonProperty("requestBodyLimit") @Nullable Integer requestBodyLimit,
      @JsonProperty("requestHeaderLimit") @Nullable Integer requestHeaderLimit,
      @JsonProperty("maxParallelRequestsPerCPU") @Nullable Integer maxParallelRequestsPerCPU,
      @JsonProperty("maxPctParallelRequestsPerActor") @Nullable Integer maxPctParallelRequestsPerActor) {
    super(id);
    if (httpPort != null && (httpPort < 0 || httpPort > 65535)) {
      logger.atWarn()
          .setMessage("Invalid port in Naksha configuration: {}, falling back to default \"8080\"")
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
        logger.atWarn()
            .setMessage("Unable to resolve the hostname using Java's API, using default \"localhost\".")
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
    env = getEnv(env);

    this.hubClassName = (hubClassName != null && !hubClassName.isEmpty()) ? hubClassName : defaultHubClassName();
    this.appId = appId != null && appId.length() > 0 ? appId : "naksha";
    this.author = author;
    this.httpPort = httpPort;
    this.hostname = hostname;
    this.endpoint = __endpoint;
    this.env = env;
    this.webRoot = webRoot;
    this.authMode = (authMode == null) ? AuthorizationMode.JWT : authMode;
    this.jwtPvtKeyPath =
        (jwtPvtKeyPath != null && !jwtPvtKeyPath.isEmpty()) ? jwtPvtKeyPath : defaultJwtPvtKeyPath();
    this.jwtPubKeyPaths =
        (jwtPubKeyPaths != null && !jwtPubKeyPaths.isEmpty()) ? jwtPubKeyPaths : defaultJwtPubKeyPaths();
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
    this.extensionConfigParams = extensionConfigParams;
    if (requestBodyLimit == null) {
      this.requestBodyLimit = DEF_REQ_BODY_LIMIT;
    } else if (requestBodyLimit <= 0 || requestBodyLimit > MAX_REQ_BODY_LIMIT) {
      logger.warn(
          "Configured request body limit {} MB not supported. Falling back to max limit of {} MB",
          requestBodyLimit,
          MAX_REQ_BODY_LIMIT);
      this.requestBodyLimit = MAX_REQ_BODY_LIMIT;
    } else {
      this.requestBodyLimit = requestBodyLimit;
    }
    if (requestHeaderLimit == null) {
      this.requestHeaderLimit = DEF_REQ_HEADER_LIMIT;
    } else if (requestHeaderLimit <= 0 || requestHeaderLimit > MAX_REQ_HEADER_LIMIT) {
      logger.warn(
          "Configured request header limit {} KB not supported. Falling back to max limit of {} KB",
          requestHeaderLimit,
          MAX_REQ_HEADER_LIMIT);
      this.requestHeaderLimit = MAX_REQ_HEADER_LIMIT;
    } else {
      this.requestHeaderLimit = requestHeaderLimit;
    }
    this.maxParallelRequestsPerCPU =
        maxParallelRequestsPerCPU != null ? maxParallelRequestsPerCPU : defaultMaxParallelRequestsPerCPU();
    this.maxPctParallelRequestsPerActor = maxPctParallelRequestsPerActor != null
        ? maxPctParallelRequestsPerActor
        : defaultMaxPctParallelRequestsPerActor();
  }

  private String getEnv(String env) {
    // This is only to be backward compatible to support EC2 based deployment
    String envVal = System.getenv(NAKSHA_ENV);
    if (envVal != null && !envVal.isEmpty() && !"null".equalsIgnoreCase(envVal)) return envVal;
    if (env != null && !env.isEmpty() && !"null".equalsIgnoreCase(env)) return env;
    return "local";
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

  public static final String JWT_PVT_KEY_PATH = "jwtPvtKeyPath";
  /**
   * The relative path to Private key file to support JWT signing (e.g. {@code "auth/jwt.key"}).
   * The path should be relative to the directory where config file is supplied.
   * For example - if config file is {@code "/home/config/cloud-config.json"} then the key path {@code "auth/jwt.key"}
   * will be considered relative to {@code "/home/config"} folder, resulting into absolute path as {@code "/home/config/auth/jwt.key}"
   */
  @JsonProperty(JWT_PVT_KEY_PATH)
  public final @NotNull String jwtPvtKeyPath;

  public static final String JWT_PUB_KEY_PATHS = "jwtPubKeyPaths";
  /**
   * The comma separated relative paths to Public key files to support JWT signature verification (e.g. {@code "auth/jwt.pub,auth/jwt_2.pub"}).
   * The path should be relative to the directory where config file is supplied.
   * For example - if config file is {@code "/home/config/cloud-config.json"} then the key path {@code "auth/jwt.pub"}
   * will be considered relative to {@code "/home/config"} folder, resulting into absolute path as {@code "/home/config/auth/jwt.pub}"
   */
  @JsonProperty(JWT_PUB_KEY_PATHS)
  public final @NotNull String jwtPubKeyPaths;

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
   * Returns a default threshold per processor for concurrency
   *
   * @return the default threshold per processor
   */
  public static int defaultMaxParallelRequestsPerCPU() {
    return 30;
  }

  /**
   * Returns a default percentage threshold per principal for concurrency
   *
   * @return the default percentage threshold per principal
   */
  public static int defaultMaxPctParallelRequestsPerActor() {
    return 25;
  }
  /**
   * Optional storage-specific parameters
   */
  public final Map<String, Object> storageParams;
  /**
   * Optional extension-manager parameters
   */
  public final ExtensionConfigParams extensionConfigParams;

  /**
   * Optional Http request body limit in MB. Default is {@link #DEF_REQ_BODY_LIMIT}.
   */
  public final Integer requestBodyLimit;

  /**
   * Optional Http request header limit in KB. Default is {@link #DEF_REQ_HEADER_LIMIT}.
   */
  public final @NotNull Integer requestHeaderLimit;

  /**
   * Optional Total Concurrency Limit
   */
  public final Integer maxParallelRequestsPerCPU;

  /**
   * Optional Total Author Concurrency Threshold
   */
  public final Integer maxPctParallelRequestsPerActor;

  public static final String NAKSHA_AUTH = "authMode";

  /**
   * The authorization mode.
   */
  @JsonProperty(NAKSHA_AUTH)
  public final @NotNull AuthorizationMode authMode;

  public enum AuthorizationMode {
    DUMMY,
    JWT
  }
}
