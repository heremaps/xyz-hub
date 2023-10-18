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
package com.here.naksha.app.service;

import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;
import static java.lang.System.err;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.auth.NakshaAuthProvider;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.IoHelp.LoadedBytes;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.hub.NakshaHub;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlConfigBuilder;
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
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main service instance.
 */
// @SuppressWarnings("unused")
// TODO HP : Remove old code
// public final class NakshaApp extends Thread implements INaksha {
public final class NakshaApp extends Thread {

  private static final Logger log = LoggerFactory.getLogger(NakshaApp.class);

  // TODO HP : Remove old code
  /*@Override
  public IStorage getAdminStorage() {
  // TODO HP : Add logic
  return null;
  }*/

  // TODO HP : Remove old code
  /*@Override
  public IStorage getSpaceStorage() {
  // TODO HP : Add logic
  return null;
  }*/

  // TODO HP : Remove old code
  /*@Override
  public @NotNull IStorage getStorageById(final @NotNull String storageId) {
  // TODO HP : Add logic
  return null;
  }*/

  /**
   * Entry point when used in a JAR as bootstrap class.
   *
   * @param args The console arguments given.
   */
  public static void main(@NotNull String... args) {
    if (args.length < 2) {
      err.println("Syntax : java -jar naksha.jar <url> <configId>");
      err.println("Example: java -jar naksha.jar <url> <configId>");
    }
    try {
      NakshaApp.newHub(args).start();
    } catch (IllegalArgumentException e) {
      err.println("Missing argument: <url> <configId>");
      err.println(
          "Example: jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=naksha local");
      err.flush();
      System.exit(1);
    } catch (Throwable t) {
      final Throwable cause = cause(t);
      cause.printStackTrace(err);
      err.flush();
      System.exit(1);
    }
  }

  /**
   * Create a new Naksha-Hub by parsing the given console arguments.
   *
   * @param args The console arguments given.
   * @return The created Naksha-Hub.
   */
  public static @NotNull NakshaApp newHub(@NotNull String... args) {
    final String url;
    if (args.length < 2 || !args[0].startsWith("jdbc:postgresql://")) {
      throw new IllegalArgumentException(
          "Missing or invalid argument <url>, must be a value like 'jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=naksha'");
    }
    if (args[1].length() == 0) {
      throw new IllegalArgumentException("Missing argument <configId>, like 'default-config'");
    }

    // Potentially we could override the app-name:
    // NakshaHubConfig.APP_NAME = ?
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName(NakshaHubConfig.defaultAppName())
        .parseUrl(args[0])
        .withDefaultSchema(NakshaAdminCollection.SCHEMA)
        .build();
    return new NakshaApp(config, args[1], null);
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
   * @param adminDbConfig The PostgresQL configuration of the admin-db to connect to.
   * @param configId      The identifier of the configuration to read.
   * @param instanceId    The (optional) instance identifier; if {@code null}, then a new unique random one created, or derived from the
   *                      environment.
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public NakshaApp(@NotNull PsqlConfig adminDbConfig, @NotNull String configId, @Nullable String instanceId) {
    super(hubs, "NakshaApp");
    this.id = number.getAndIncrement();
    setName("NakshaApp#" + id);
    if (instanceId == null) {
      instanceId = this.discoverInstanceId();
    }
    this.instanceId = instanceId;

    NakshaHubConfig config = null;
    // Read the default configuration from file
    try (final Json json = Json.get()) {
      // TODO HP_QUERY : Reason for not supporting custom config path?
      final LoadedBytes loaded =
          IoHelp.readBytesFromHomeOrResource(configId + ".json", false, adminDbConfig.appName);
      final NakshaHubConfig cfg = json.reader(ViewDeserialize.Storage.class)
          .forType(NakshaHubConfig.class)
          .readValue(loaded.bytes());
      log.info("Fetched supplied server config from {}", loaded.path());
      config = cfg;
    } catch (Exception ex) {
      log.warn("Error reading supplied server config, will continue with default. ", ex);
    }

    // TODO HP : OLD code to be removed
    // adminStorage = new PsqlStorage(adminDbConfig, 1L);
    // adminStorage.init();
    hub = new NakshaHub(adminDbConfig, config);

    // final ITransactionSettings tempSettings = adminStorage.createSettings().withAppId(adminDbConfig.appName);
    // try (final PsqlTxWriter tx = adminStorage.openMasterTransaction(tempSettings)) {
    /*
    final Map<@NotNull String, @NotNull CollectionInfo> existing =
    listToMap(tx.iterateCollections().readAll(), CollectionInfo::getId);
    if (!existing.containsKey(NakshaAdminCollection.CATALOGS.getId())) {
    tx.createCollection(NakshaAdminCollection.CATALOGS);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.EXTENSIONS.getId())) {
    tx.createCollection(NakshaAdminCollection.EXTENSIONS);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.CONNECTORS.getId())) {
    tx.createCollection(NakshaAdminCollection.CONNECTORS);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.STORAGES.getId())) {
    tx.createCollection(NakshaAdminCollection.STORAGES);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.SPACES.getId())) {
    tx.createCollection(NakshaAdminCollection.SPACES);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.SUBSCRIPTIONS.getId())) {
    tx.createCollection(NakshaAdminCollection.SUBSCRIPTIONS);
    tx.commit();
    }
    if (!existing.containsKey(NakshaAdminCollection.CONFIGS.getId())) {
    tx.createCollection(NakshaAdminCollection.CONFIGS);
    tx.commit();
    }
    */
    // Run maintenance on AdminDB (to ensure table partitions exist) before performing any DML operations
    // adminStorage.maintain(NakshaAdminCollection.COLLECTION_INFO_LIST);

    // read default storage config and add to DB if not already present
    /*
    Storage defStorage = tx.readFeatures(Storage.class, NakshaAdminCollection.STORAGES)
    .getFeatureById("psql");
    if (defStorage == null) {
    try (final Json json = Json.get()) {
    final String storageJson = IoHelp.readResource("config/default-storage.json");
    defStorage = json.reader(ViewDeserialize.Storage.class)
    .forType(Storage.class)
    .readValue(storageJson);
    tx.writeFeatures(Storage.class, NakshaAdminCollection.STORAGES)
    .modifyFeatures(new ModifyFeaturesReq<Storage>(false).insert(defStorage));
    tx.commit();
    } catch (Exception e) {
    throw unchecked(e);
    }
    }
    this.storage = defStorage;
    */

    // read default config and add to DB if not already present
    /*
    boolean dbConfigExists = false;
    config = tx.readFeatures(NakshaHubConfig.class, NakshaAdminCollection.CONFIGS)
    .getFeatureById(configId);
    if (config != null) {
    dbConfigExists = true;
    }
    try (final Json json = Json.get()) {
    final String configJson = IoHelp.readResource("config/local.json");
    config = json.reader(ViewDeserialize.Storage.class)
    .forType(NakshaHubConfig.class)
    .readValue(configJson);
    ModifyFeaturesReq<NakshaHubConfig> req = new ModifyFeaturesReq<>(false);
    req = dbConfigExists ? req.update(config) : req.insert(config);
    tx.writeFeatures(NakshaHubConfig.class, NakshaAdminCollection.CONFIGS)
    .modifyFeatures(req);
    tx.commit();
    } catch (Exception e) {
    throw unchecked(e);
    }
    log.info("Loaded default configuration '{}' as: {}", configId, config);
    */

    this.config = hub.getConfig();
    log.info("Using server config : {}", this.config);

    // TODO HP : Remove old code
    // this.txSettings = adminStorage.createSettings().withAppId(config.appId).withAuthor(config.author);

    log.info("Naksha host/endpoint: {} / {}", config.hostname, config.endpoint);

    // vertxMetricsOptions = new MetricsOptions().setEnabled(true).setFactory(new NakshaHubMetricsFactory());
    vertxOptions = new VertxOptions();
    // See: https://vertx.io/docs/vertx-core/java
    // vertxOptions.setMetricsOptions(vertxMetricsOptions);
    vertxOptions.setPreferNativeTransport(true);
    if (config.debug) {
      // If running in debug mode, we need to increase the warning time, because we might enter a break-point
      // for
      // some time!
      vertxOptions
          .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(3))
          .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(3));
    }
    vertx = Vertx.vertx(vertxOptions);

    //      if (config.jwtPubKey != null) {
    //        final String jwtPubKey = jwtPubKey(config.jwtPubKey);
    //        authOptions = new JWTAuthOptions()
    //            .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtPubKey));
    //      } else {
    final String jwtKey;
    final String jwtPub;
    {
      final String path = "auth/" + config.jwtName + ".key";
      final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(path, false, NakshaHubConfig.APP_NAME);
      log.info("Loaded JWT key file {}", loaded.path());
      jwtKey = new String(loaded.bytes(), StandardCharsets.UTF_8);
    }
    {
      final String path = "auth/" + config.jwtName + ".pub";
      final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(path, false, NakshaHubConfig.APP_NAME);
      log.info("Loaded JWT key file {}", loaded.path());
      jwtPub = new String(loaded.bytes(), StandardCharsets.UTF_8);
    }
    authOptions = new JWTAuthOptions()
        .setJWTOptions(new JWTOptions().setAlgorithm("RS256"))
        .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtKey))
        .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtPub));
    //      }
    authProvider = new NakshaAuthProvider(vertx, authOptions);

    final WebClientOptions webClientOptions = new WebClientOptions();
    webClientOptions.setUserAgent(config.userAgent);
    webClientOptions.setTcpKeepAlive(true).setTcpQuickAck(true).setTcpFastOpen(true);
    webClientOptions.setIdleTimeoutUnit(TimeUnit.MINUTES).setIdleTimeout(2);
    webClient = WebClient.create(vertx, webClientOptions);
    shutdownThread = new Thread(this::shutdownHook);
    // }
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
   * Discover the environment in which we are.
   *
   * @return The environment in which we are.
   */
  private @NotNull String discoverEnvironment() {
    return "local";
  }

  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<Class<? extends Event>, F0<? extends AbstractTask>> tasks =
      new ConcurrentHashMap<>();

  public @NotNull ErrorResponse toErrorResponse(@NotNull Throwable throwable) {
    return new ErrorResponse(throwable, null);
  }

  public @NotNull <RESPONSE> Future<@Nullable RESPONSE> executeTask(@NotNull Supplier<RESPONSE> execute) {
    return new InternalTask<>(hub, execute).start();
  }

  public @NotNull Future<@NotNull XyzResponse> executeEvent(
      @NotNull Event event, @NotNull EventFeature eventFeature) {
    return new InternalEventTask(hub, event, eventFeature).start();
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

  /**
   * The configuration of this hub.
   */
  private final @NotNull NakshaHubConfig config;

  /**
   * Returns the configuration of this hub.
   *
   * @return The configuration of this hub.
   */
  public @NotNull NakshaHubConfig config() {
    return config;
  }

  private final @NotNull com.here.naksha.lib.hub.NakshaHub hub;
  /**
   * The admin storage.
   */
  // TODO HP : Remove old code
  /*private final @NotNull PsqlStorage adminStorage;*/

  // TODO HP : Remove old code
  /*@Override
  public @NotNull IStorage storage() {
  return adminStorage;
  }*/

  // TODO HP : Remove old code
  /*@Override
  public @NotNull ITransactionSettings settings() {
  return adminStorage.createSettings().withAppId(config().appId);
  }*/

  // TODO HP : Remove old code
  // private final @NotNull ITransactionSettings txSettings;

  /**
   * Returns a new master transaction, must be closed.
   *
   * @return A new master transaction, must be closed.
   */
  // TODO HP : Remove old code
  /*public @NotNull PsqlTxWriter writeTx() {
  return adminStorage.openMasterTransaction(txSettings);
  }*/

  /**
   * Returns a new read transaction, must be closed.
   *
   * @return A new read transaction, must be closed.
   */
  // TODO HP : Remove old code
  /*public @NotNull PsqlTxReader readTx() {
  return adminStorage.openReplicationTransaction(txSettings);
  }*/

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
    final int processors = Runtime.getRuntime().availableProcessors();
    // TODO: Add verticles
    verticles = new NakshaHttpVerticle[processors];
    for (int i = 0; i < processors; i++) {
      verticles[i] = new NakshaHttpVerticle(hub, i, this);
      vertx.deployVerticle(verticles[i]);
    }
    Thread.setDefaultUncaughtExceptionHandler(NakshaApp::uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    // Schedule backend Storage maintenance job
    // TODO HP : Re-enable code after fix in NakshaContext
    /*
    try (final IWriteSession writer = hub.getSpaceStorage().newWriteSession(new NakshaContext(), true)) {
    writer.startMaintainer();
    }
    */
    // TODO HP : Remove old code
    // new StorageMaintainer(this.config, this.storage, this.adminStorage, this.txSettings).scheduleJob();
    // TODO: Start metric publisher!
    // startMetricPublishers();

    // TODO: We need to add some way to stop the service gracefully!
    //noinspection InfiniteLoopStatement
    while (true) {
      try {
        Thread.sleep(TimeUnit.DAYS.toMillis(365));
      } catch (Throwable t) {
        final Throwable cause = cause(t);
        log.atError()
            .setMessage("Unexpected error while during Naksha service execution")
            .setCause(cause)
            .log();
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
}
