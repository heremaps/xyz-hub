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
package com.here.naksha.app.service;

import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;
import static java.lang.System.err;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.auth.NakshaAuthProvider;
import com.here.naksha.app.service.metrics.OTelMetrics;
import com.here.naksha.app.service.util.UrlUtil;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.IoHelp.LoadedBytes;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.hub.NakshaHubFactory;
import com.here.naksha.lib.hub.util.ConfigUtil;
import com.here.naksha.lib.psql.PsqlStorage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The main service instance.
 */
@SuppressWarnings("unused")
public final class NakshaApp extends Thread {

  private static final Logger log = LoggerFactory.getLogger(NakshaApp.class);
  private static final String DEFAULT_CONFIG_ID = "default-config";

  private static final String DEFAULT_SCHEMA = "naksha";

  private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd"
      + "&schema=" + DEFAULT_SCHEMA
      + "&app=" + NakshaHubConfig.defaultAppName()
      + "&id=" + PsqlStorage.ADMIN_STORAGE_ID;
  private final AtomicReference<Boolean> stopInstance = new AtomicReference<>(false);

  /**
   * Entry point when used in a JAR as bootstrap class.
   *
   * @param args The console arguments given.
   */
  public static void main(@NotNull String... args) {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }
    try {
      MDC.put("streamId", "naksha-app");
      NakshaApp.newInstance(args).start();
    } catch (IllegalArgumentException e) {
      printUsage();
      System.exit(1);
    } catch (Throwable t) {
      final Throwable cause = cause(t);
      cause.printStackTrace(err);
      err.flush();
      System.exit(1);
    }
  }

  private static void printUsage() {
    err.println(" ");
    err.println("Syntax :");
    err.println("    java -jar naksha.jar <configId> [<url>]");
    err.println(" ");
    err.println("Examples:");
    err.println(" ");
    err.println("    Example 1 : Start service with given config and default (local) database URL");
    err.println("        java -jar naksha.jar default-config");
    err.println(" ");
    err.println("    Example 2 : Start service with given config and custom database URL");
    err.println("        java -jar naksha.jar default-config '" + DEFAULT_URL + "'");
    err.println(" ");
    err.println("    Example 3 : Start service with mock config (with in-memory hub)");
    err.println("        java -jar naksha.jar mock-config");
    err.println(" ");
    err.flush();
  }

  /**
   * Create a new Naksha-App instance by parsing the given console arguments.
   *
   * @param args The console arguments given.
   * @return The created Naksha-App instance.
   */
  public static @NotNull NakshaApp newInstance(@NotNull String... args) {
    log.info("Naksha App v{}", NakshaVersion.latest);

    final String cfgId;
    final String url;
    switch (args.length) {
      case 1 -> {
        cfgId = args[0];
        url = DEFAULT_URL;
        log.info("Starting with config `{}` and default database...", cfgId);
      }
      case 2 -> {
        cfgId = args[0];
        url = args[1];
        if (!url.startsWith("jdbc:postgresql://")) {
          throw new IllegalArgumentException("Missing or invalid argument <url>, must be a value like '"
              + DEFAULT_URL + "', got '" + url + "' instead");
        }
        log.info("Starting with config `{}` and custom database URL...", cfgId);
      }
      default -> {
        throw new IllegalArgumentException("Missing/Invalid argument. Check the usage.");
      }
    }

    // Potentially we could override the app-name:
    // NakshaHubConfig.APP_NAME = ?
    return new NakshaApp(NakshaHubConfig.defaultAppName(), url, cfgId, null);
  }

  /**
   * The thread-group for all Naksha-Hubs.
   */
  public static final ThreadGroup hubs = new ThreadGroup("NakshaApp");

  /**
   * The Naksha-Hub number.
   */
  private static final AtomicLong number = new AtomicLong(1L);

  static {
    // https://github.com/vert-x3/vertx-web/issues/2182
    System.setProperty("io.vertx.web.router.setup.lenient", "true");
  }

  /**
   * Create a new Naksha-Hub instance, connect to the supplied database, initialize it and read the configuration from it, then bootstrap
   * the service.
   *
   * @param appName    The name of the app
   * @param storageUrl The PostgresQL storage url of the admin-db to connect to.
   * @param configId   The identifier of the configuration to read.
   * @param instanceId The (optional) instance identifier; if {@code null}, then a new unique random one created, or derived from the
   *                   environment.
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public NakshaApp(
      @NotNull String appName,
      @NotNull String storageUrl,
      @NotNull String configId,
      @Nullable String instanceId) {
    super(hubs, "NakshaApp");
    this.id = number.getAndIncrement();
    setName("NakshaApp#" + id);
    if (instanceId == null) {
      instanceId = this.discoverInstanceId();
    }
    this.instanceId = instanceId;

    // Read the custom configuration from file (if available)
    NakshaHubConfig config = null;
    try {
      config = ConfigUtil.readConfigFile(configId, appName);
    } catch (Exception ex) {
      log.warn("No external config available, will attempt using default. Error was [{}]", ex.getMessage());
    }
    // Instantiate NakshaHub instance
    this.hub = NakshaHubFactory.getInstance(appName, storageUrl, config, configId);
    config = hub.getConfig(); // use the config finally set by NakshaHub instance
    log.info("Using server config : {}", config);

    log.info("Naksha host/endpoint: {}", config.endpoint);

    // vertxMetricsOptions = new MetricsOptions().setEnabled(true).setFactory(new NakshaHubMetricsFactory());
    this.vertxOptions = new VertxOptions();
    // See: https://vertx.io/docs/vertx-core/java
    // vertxOptions.setMetricsOptions(vertxMetricsOptions);
    this.vertxOptions.setPreferNativeTransport(true);
    if (config.debug) {
      // If running in debug mode, we need to increase the warning time, because we might enter a break-point
      // for
      // some time!
      this.vertxOptions
          .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(3))
          .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(3));
    }
    this.vertx = Vertx.vertx(this.vertxOptions);

    final String jwtKey;
    final String jwtPub;
    {
      final String path = "auth/" + config.jwtName + ".key";
      final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(path, false, NakshaHubConfig.APP_NAME);
      log.info("Loaded JWT key file {}", loaded.getPath());
      jwtKey = new String(loaded.getBytes(), StandardCharsets.UTF_8);
    }
    {
      final String path = "auth/" + config.jwtName + ".pub";
      final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(path, false, NakshaHubConfig.APP_NAME);
      log.info("Loaded JWT key file {}", loaded.getPath());
      jwtPub = new String(loaded.getBytes(), StandardCharsets.UTF_8);
    }
    this.authOptions = new JWTAuthOptions()
        .setJWTOptions(new JWTOptions().setAlgorithm("RS256"))
        .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtKey))
        .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtPub));
    this.authProvider = new NakshaAuthProvider(this.vertx, this.authOptions);

    final WebClientOptions webClientOptions = new WebClientOptions();
    webClientOptions.setUserAgent(config.userAgent);
    webClientOptions.setTcpKeepAlive(true).setTcpQuickAck(true).setTcpFastOpen(true);
    webClientOptions.setIdleTimeoutUnit(TimeUnit.MINUTES).setIdleTimeout(2);
    this.webClient = WebClient.create(this.vertx, webClientOptions);
    this.shutdownThread = new Thread(this::shutdownHook);
  }

  /**
   * The unique identifier of this instance.
   */
  public final long id;

  /**
   * Discover or generate a unique instance identifier.
   *
   * @return The unique instance identifier.
   */
  private @NotNull String discoverInstanceId() {
    // TODO: We may use the MAC address of the network interface.
    //       We may try to detect the instance ID from EC2 metadata.
    //       ...
    return UUID.randomUUID().toString();
  }

  /**
   * The unique instance identifier.
   */
  private final @NotNull String instanceId;

  /**
   * Returns the instance-identifier of this Naksha-Hub.
   *
   * @return The instance-identifier of this Naksha-Hub.
   */
  public @NotNull String instanceId() {
    return instanceId;
  }

  private final @NotNull INaksha hub;

  public @NotNull INaksha getHub() {
    return hub;
  }

  /**
   * A web client to access XYZ Hub nodes and other web resources.
   */
  public final @NotNull WebClient webClient;

  /**
   * The VertX-Options.
   */
  public final @NotNull VertxOptions vertxOptions;

  /**
   * The VertX-Metrics-Options
   */
  // public final @NotNull MetricsOptions vertxMetricsOptions;

  /**
   * The entry point to the Vert.x core API.
   */
  public final @NotNull Vertx vertx;

  /**
   * The authentication options used to generate the {@link #authProvider}.
   */
  public final @NotNull JWTAuthOptions authOptions;

  /**
   * The auth-provider.
   */
  public final @NotNull NakshaAuthProvider authProvider;

  /**
   * Start the server.
   *
   * @throws IllegalStateException If the server already started.
   */
  @Override
  public void start() {
    if (!start.compareAndSet(false, true)) {
      throw new IllegalStateException("Server already started");
    }
    super.start();
  }

  /**
   * The verticles of the Hub (one per CPU).
   */
  @NotNull
  NakshaHttpVerticle[] verticles;

  /**
   * Emergency uncaught exception handler to prevent that the server crashs.
   *
   * @param thread    The thread that cause the exception.
   * @param throwable The exception thrown.
   */
  private static void uncaughtExceptionHandler(@NotNull Thread thread, @NotNull Throwable throwable) {
    log.atError()
        .setMessage("Uncaught exception in thread {}")
        .addArgument(thread.getName())
        .setCause(throwable)
        .log();
  }

  private final AtomicBoolean start = new AtomicBoolean();

  /**
   * Internally called, when invoked from external raises a {@link UnsupportedOperationException}.
   */
  public void run() {
    if (this != Thread.currentThread()) {
      throw new UnsupportedOperationException(
          "Illegal invocation, the run method can only be invoked from the hub itself");
    }

    final Thread appThread = this;

    // initialize OTel metrics collector
    OTelMetrics.init();

    // Add verticles
    final int processors = Runtime.getRuntime().availableProcessors();
    verticles = new NakshaHttpVerticle[processors];
    List<Future<String>> futureList = new ArrayList<>();
    for (int i = 0; i < processors; i++) {
      verticles[i] = new NakshaHttpVerticle(hub, i, this);
      futureList.add(vertx.deployVerticle(verticles[i]));
    }
    // check verticle deployment status (asynchronously)
    for (final Future<String> future : futureList) {
      future.onComplete(event -> {
        if (event.failed()) {
          log.error("Verticle deployment failed due to unexpected error. ", event.cause());
          stopInstance.set(true);
          appThread.interrupt();
          vertx.close();
        }
      });
    }

    Thread.setDefaultUncaughtExceptionHandler(NakshaApp::uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    // TODO HP : Schedule backend Storage maintenance job

    // Keep waiting until explicitly asked to stop (using interrupt + stopInstance flag)
    while (!stopInstance.get()) {
      try {
        Thread.sleep(TimeUnit.DAYS.toMillis(365));
      } catch (InterruptedException ie) {
        log.warn("Interrupted with " + (stopInstance.get() ? "stop signal." : ie.getMessage()));
      } catch (Throwable t) {
        log.error("Unexpected error during Naksha service execution. ", t);
      }
    }
  }

  /**
   * Tests whether the service is currently running.
   *
   * @return {@code true} if the service is currently running; {@code false} otherwise.
   */
  public boolean isRunning() {
    return start.get() && shutdownThread.getState() == State.NEW;
  }

  /**
   * The thread that is registered as shutdown hook.
   */
  private final @NotNull Thread shutdownThread;

  /**
   * Registered by the constructor.
   */
  private void shutdownHook() {
    log.atInfo()
        .setMessage("Service is going down at {}")
        .addArgument(new Date())
        .log();
    // stopMetricPublishers();
  }

  public void stopInstance() {
    log.info("Stop instance trigger received.");
    vertx.close();
    stopInstance.set(true);
    this.interrupt();
  }

  /**
   * Ensures that provided url contains schema - if it doesn't, default schema is applied
   *
   * @param url url for admin db
   * @return url with ensured schema
   */
  private static String urlWithEnsuredSchema(String url) {
    Map<String, List<String>> queryParams = UrlUtil.queryParams(url);
    queryParams.putIfAbsent("schema", List.of(DEFAULT_SCHEMA));
    return UrlUtil.urlWithOverriddenParams(url, queryParams);
  }
}
