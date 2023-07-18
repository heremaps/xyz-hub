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
package com.here.xyz.hub;

import static com.here.naksha.lib.core.util.IoHelp.openResource;
import static com.here.naksha.lib.core.util.IoHelp.parseValue;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.IoHelp.LoadedBytes;
import com.here.naksha.lib.core.util.Params;
import com.here.naksha.lib.psql.AbstractNakshaHub;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.State;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class NakshaHub extends AbstractNakshaHub {

  /**
   * The logger.
   */
  public static final Logger logger = LoggerFactory.getLogger(NakshaHub.class);

  /**
   * The build properties read from {@code build.properties} file.
   */
  public final @NotNull Properties buildProperties;

  /**
   * The build time.
   */
  public final long buildTime;

  /**
   * The build version.
   */
  public final @NotNull String buildVersion;

  /**
   * The user-agent to send when contacting external services via HTTP.
   */
  public final @NotNull String userAgent;

  /**
   * The unique host identifier.
   */
  public final @NotNull String hostId;

  /**
   * The service configuration.
   */
  public final @NotNull NakshaHubConfig config;

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
  public final @NotNull MetricsOptions vertxMetricsOptions;

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

  private static @NotNull String jwtPubKey(@NotNull String jwtPubKey) {
    if (!jwtPubKey.startsWith("-----")) {
      jwtPubKey = "-----BEGIN PUBLIC KEY-----\n" + jwtPubKey;
    }
    if (!jwtPubKey.endsWith("-----")) {
      jwtPubKey = jwtPubKey + "\n-----END PUBLIC KEY-----";
    }
    return jwtPubKey;
  }

  /**
   * Create a new Naksha-Hub service instance using the given configuration, but not yet run it. Therefore, the hub is initialized, but not
   * yet bound to a socket nor able to processed requests. To start the service call the method {@link #start()}.
   *
   * @param config The configuration to use.
   * @throws IOException If loading the build properties failed.
   */
  public NakshaHub(@NotNull NakshaHubConfig config) throws IOException {
    super(config.db, config.serverName, 0L);
    this.config = config;
    buildProperties = NakshaHub.getBuildProperties();
    buildVersion = parseValue(buildProperties.get("naksha.version"), String.class);
    final String buildTime = parseValue(buildProperties.get("naksha.buildTime"), String.class);
    try {
      this.buildTime =
          new SimpleDateFormat("yyyy.MM.dd-HH:mm").parse(buildTime).getTime();
    } catch (ParseException e) {
      throw new IOException("Failed to parse the build time from build.properties resources");
    }
    userAgent = config.serverName + buildVersion;
    hostId = UUID.randomUUID().toString();
    vertxMetricsOptions = new MetricsOptions().setEnabled(true).setFactory(new NakshaHubMetricsFactory());
    vertxOptions = new VertxOptions();
    logger.info("Naksha host: {}", config.hostname);
    logger.info("Naksha endpoint: {}", config.endpoint);

    // See: https://vertx.io/docs/vertx-core/java
    vertxOptions.setMetricsOptions(vertxMetricsOptions);
    vertxOptions.setPreferNativeTransport(true);
    if (config.debug) {
      // If running in debug mode, we need to increase the warning time, because we might enter a break-point for
      // some time!
      vertxOptions
          .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(3))
          .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(3));
    }
    vertx = Vertx.vertx(vertxOptions);

    if (config.jwtPubKey != null) {
      final String jwtPubKey = jwtPubKey(config.jwtPubKey);
      authOptions = new JWTAuthOptions()
          .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtPubKey));
    } else {
      final String jwtKeyPath = "auth/" + config.jwtKeyName + ".key";
      final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(jwtKeyPath, false, NakshaHubConfig.APP_NAME);
      logger.info("Loaded JWT key file {}", loaded.path());
      final String jwtKey = new String(loaded.bytes(), StandardCharsets.UTF_8);
      authOptions = new JWTAuthOptions()
          .setJWTOptions(new JWTOptions().setAlgorithm("RS256"))
          .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtKey));
    }
    authProvider = new NakshaAuthProvider(vertx, authOptions);

    final WebClientOptions webClientOptions = new WebClientOptions();
    webClientOptions.setUserAgent(userAgent);
    webClientOptions.setTcpKeepAlive(true).setTcpQuickAck(true).setTcpFastOpen(true);
    webClientOptions.setIdleTimeoutUnit(TimeUnit.MINUTES).setIdleTimeout(2);
    webClient = WebClient.create(vertx, webClientOptions);
    shutdownThread = new Thread(this::shutdownHook);

    // Create the virtual admin connector.
    adminConnector = new Connector("naksha:admin", 0);
    adminConnector.setEventHandler(PsqlHandler.ID);
    adminConnector.setParams(new Params().with(PsqlHandlerParams.DB_CONFIG, config.db));

    // Create the virtual spaces that are used to manage spaces, connectors and subscriptions.
    spaces = new Space(DEFAULT_SPACE_COLLECTION);
    spaces.setForceOwner(NakshaHubConfig.APP_NAME);
    spaces.setHistory(true);
    spaces.setConnectorIds(adminConnector.getId());

    connectors = new Space(DEFAULT_CONNECTOR_COLLECTION);
    connectors.setForceOwner(NakshaHubConfig.APP_NAME);
    connectors.setHistory(true);
    connectors.setConnectorIds(adminConnector.getId());

    subscriptions = new Space(DEFAULT_SUBSCRIPTIONS_COLLECTION);
    subscriptions.setForceOwner(NakshaHubConfig.APP_NAME);
    subscriptions.setHistory(true);
    subscriptions.setConnectorIds(adminConnector.getId());

    // Register all supported tasks and handlers.
    initTasks();
    initHandlers();
  }

  final @NotNull Connector adminConnector;
  final @NotNull Space spaces;
  final @NotNull Space connectors;
  final @NotNull Space subscriptions;

  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<@NotNull Class<? extends Event>, @NotNull F0<@NotNull AbstractTask>> tasks =
      new ConcurrentHashMap<>();

  /**
   * Register the tasks for the events.
   */
  private void initTasks() {
    tasks.put(GetFeaturesByIdEvent.class, GetFeaturesByIdTask::new);
    tasks.put(ModifyFeaturesEvent.class, ModifyFeaturesTask::new);
  }

  /**
   * Register all handlers.
   */
  private void initHandlers() {
    //    EventHandler.register(PsqlHandler.ID, PsqlHandler.class);
    //    EventHandler.register(HttpHandler.ID, HttpHandler.class);
    //    EventHandler.register(ActivityLogHandler.ID, ActivityLogHandler.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newTask(
      @NotNull Class<EVENT> eventClass) throws XyzErrorException {
    final F0<TASK> constructor = (F0<TASK>) tasks.get(eventClass);
    if (constructor == null) {
      throw new XyzErrorException(XyzError.EXCEPTION, "No task for event " + eventClass.getName() + " found");
    }
    return constructor.call();
  }

  private final AtomicBoolean start = new AtomicBoolean();

  /**
   * Tests whether the service is currently running.
   *
   * @return {@code true} if the service is currently running; {@code false} otherwise.
   */
  public boolean isRunning() {
    return start.get() && shutdownThread.getState() == State.NEW;
  }

  /**
   * Start the server.
   *
   * @throws IllegalStateException If the service already started.
   */
  public void start() {
    if (!start.compareAndSet(false, true)) {
      throw new IllegalStateException("Service already started");
    }
    if (register() != null) {
      currentLogger().warn("Unregistering existing global naksha client!");
    } else {
      currentLogger().info("Registering as new global naksha client.");
    }
    final int processors = Runtime.getRuntime().availableProcessors();
    verticles = new NakshaHubVerticle[processors];
    for (int i = 0; i < processors; i++) {
      verticles[i] = new NakshaHubVerticle(this, i);
      vertx.deployVerticle(verticles[i]);
    }
    Thread.setDefaultUncaughtExceptionHandler(NakshaHub::uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    startMetricPublishers();
  }

  @NotNull
  NakshaHubVerticle[] verticles;

  /**
   * Emergency uncaught exception handler to prevent that the server crashs.
   *
   * @param thread    The thread that cause the exception.
   * @param throwable The exception thrown.
   */
  protected static void uncaughtExceptionHandler(@NotNull Thread thread, @NotNull Throwable throwable) {
    logger.error("Uncaught exception in thread {}", thread.getName(), throwable);
  }

  /**
   * The thread that is registered as shutdown hook.
   */
  protected final @NotNull Thread shutdownThread;

  /**
   * Registered by the constructor
   */
  protected void shutdownHook() {
    final String msg = "Service is going down at " + new Date();
    logger.info(msg);
    System.out.println(msg);
    System.out.flush();
    stopMetricPublishers();
  }

  public static long getUsedMemoryBytes() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public static float getUsedMemoryPercent() {
    float used = getUsedMemoryBytes();
    float total = Runtime.getRuntime().totalMemory();
    return used / total * 100;
  }

  private final List<MetricPublisher<?>> metricPublishers = new LinkedList<>();

  protected void startMetricPublishers() {
    ConnectionMetrics.initialize();
    metricPublishers.add(new CWBareValueMetricPublisher(new MemoryMetric("JvmMemoryUtilization")));
    metricPublishers.add(new CWBareValueMetricPublisher(new MajorGcCountMetric("MajorGcCount")));
    metricPublishers.add(new CWBareValueMetricPublisher(new GcDurationMetric("GcDuration")));
    metricPublishers.add(new CWBareValueMetricPublisher(new GlobalUsedRfcConnections("GlobalUsedRfcConnections")));
    metricPublishers.add(
        new CWBareValueMetricPublisher(new GlobalInflightRequestMemory("GlobalInflightRequestMemory")));
    metricPublishers.addAll(ConnectionMetrics.startConnectionMetricPublishers());
  }

  protected void stopMetricPublishers() {
    metricPublishers.forEach(MetricPublisher::stop);
  }

  private static final AtomicReference<@Nullable Boolean> usesZGC = new AtomicReference<>();

  public static boolean isUsingZgc() {
    final Boolean usesZgc = usesZGC.get();
    if (usesZgc != null) {
      return usesZgc;
    }
    List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
      if (gcMxBean.getName().startsWith("ZGC")) {
        usesZGC.set(Boolean.TRUE);
        return true;
      }
    }
    usesZGC.set(Boolean.FALSE);
    return false;
  }

  /**
   * The LOG4J configuration file.
   */
  protected static final String CONSOLE_LOG_CONFIG = "log4j2-console-plain.json";

  /**
   * The Vertx worker pool size environment variable.
   */
  protected static final String VERTX_WORKER_POOL_SIZE = "VERTX_WORKER_POOL_SIZE";

  /**
   * Read the build properties from the JAR.
   *
   * @return The build properties read from the "build.properties" file.
   */
  public static @NotNull Properties getBuildProperties() throws IOException {
    final InputStream input = openResource("/build.properties");
    // load a properties file
    final Properties buildProperties = new Properties();
    buildProperties.load(input);
    return buildProperties;
  }

  /**
   * A method to let the service die with the given exit-code, error message and optional exception.
   *
   * @param exitCode The exit code to return to the OS.
   * @param reason   The human-readable reason.
   */
  public static void die(final int exitCode, final @NotNull String reason) {
    die(exitCode, reason, null);
  }

  /**
   * A method to let the service die with the given exit-code, error message and optional exception.
   *
   * @param exitCode  The exit code to return to the OS.
   * @param reason    The human-readable reason.
   * @param exception The exception; if any; that caused the exit.
   */
  public static void die(final int exitCode, final @NotNull String reason, @Nullable Throwable exception) {
    // Let's always generate a stack-trace.
    if (exception == null) {
      exception = new RuntimeException();
    }
    logger.error(reason, exception);
    System.out.flush();
    System.err.println(reason);
    exception.printStackTrace(System.err);
    System.err.flush();
    System.exit(exitCode);
  }
}
