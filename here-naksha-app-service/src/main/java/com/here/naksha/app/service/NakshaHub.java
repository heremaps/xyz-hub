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
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.NakshaHelper.listToMap;
import static java.lang.System.err;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.auth.NakshaAuthProvider;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.features.Storage;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.IoHelp.LoadedBytes;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlConfigBuilder;
import com.here.naksha.lib.psql.PsqlStorage;
import com.here.naksha.lib.psql.PsqlTxReader;
import com.here.naksha.lib.psql.PsqlTxWriter;
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
import java.util.Map;
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
@SuppressWarnings("unused")
public final class NakshaHub extends Thread implements INaksha {

  private static final Logger log = LoggerFactory.getLogger(NakshaHub.class);

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
      NakshaHub.newHub(args).start();
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
  public static @NotNull NakshaHub newHub(@NotNull String... args) {
    final String url;
    if (args.length < 2 || !args[0].startsWith("jdbc:postgresql://")) {
      throw new IllegalArgumentException(
          "Missing or invalid argument <url>, must be a value like 'jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=naksha'");
    }
    if (args[1].length() == 0) {
      throw new IllegalArgumentException("Missing argument <configId>");
    }

    // Potentially we could override the app-name:
    // NakshaHubConfig.APP_NAME = ?
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName(NakshaHubConfig.defaultAppName())
        .parseUrl(args[0])
        .withSchema(NakshaAdminCollection.SCHEMA)
        .build();
    return new NakshaHub(config, args[1], null);
  }

  /**
   * The thread-group for all Naksha-Hubs.
   */
  public static final ThreadGroup hubs = new ThreadGroup("NakshaHub");

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
  public NakshaHub(@NotNull PsqlConfig adminDbConfig, @NotNull String configId, @Nullable String instanceId) {
    super(hubs, "NakshaHub");
    this.id = number.getAndIncrement();
    setName("NakshaHub#" + id);
    if (instanceId == null) {
      instanceId = this.discoverInstanceId();
    }
    this.instanceId = instanceId;
    adminStorage = new PsqlStorage(adminDbConfig, 1L);
    adminStorage.init();
    final ITransactionSettings tempSettings = adminStorage.createSettings().withAppId(adminDbConfig.appName);
    NakshaHubConfig config;
    try (final PsqlTxWriter tx = adminStorage.openMasterTransaction(tempSettings)) {
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

        try (final Json json = Json.get()) {
          final String storageJson = IoHelp.readResource("config/storage.json");
          final Storage storage = json.reader(ViewDeserialize.Storage.class)
              .forType(Storage.class)
              .readValue(storageJson);
          // TODO HP_QUERY : How do I generate "number" for Storage class here?
          tx.writeFeatures(Storage.class, NakshaAdminCollection.STORAGES)
              .modifyFeatures(new ModifyFeaturesReq<Storage>(false).insert(storage));
          tx.commit();
        } catch (Exception e) {
          throw unchecked(e);
        }
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

        try (final Json json = Json.get()) {
          final String configJson = IoHelp.readResource("config/local.json");
          config = json.reader(ViewDeserialize.Storage.class)
              .forType(NakshaHubConfig.class)
              .readValue(configJson);
          tx.writeFeatures(NakshaHubConfig.class, NakshaAdminCollection.CONFIGS)
              .modifyFeatures(new ModifyFeaturesReq<NakshaHubConfig>(false).insert(config));
          tx.commit();
        } catch (Exception e) {
          throw unchecked(e);
        }
      } else {
        config = tx.readFeatures(NakshaHubConfig.class, NakshaAdminCollection.CONFIGS)
            .getFeatureById(configId);
        // TODO HP_QUERY : Why this way instead of direct call `log.info()` ?
        log.atInfo()
            .setMessage("Loaded configuration '{}' from admin-db is: {}")
            .addArgument(configId)
            .addArgument(config)
            .log();
      }

      // Read the configuration.
      try (final Json json = Json.get()) {
        // TODO HP_QUERY : Reason for not supporting custom config path?
        final LoadedBytes loaded =
            IoHelp.readBytesFromHomeOrResource(configId + ".json", false, adminDbConfig.appName);
        final NakshaHubConfig cfg = json.reader(ViewDeserialize.Storage.class)
            .forType(NakshaHubConfig.class)
            .readValue(loaded.bytes());
        log.atInfo()
            .setMessage("Override configuration from file {}: {}")
            .addArgument(loaded.path())
            .addArgument(config)
            .log();
        config = cfg;
      } catch (Throwable t) {
        log.atDebug()
            .setMessage("Failed to load {}.json from XGD config directory ~/.config/{}/{}.json")
            .addArgument(configId)
            .addArgument(adminDbConfig.appName)
            .addArgument(configId)
            .log();
      }
      if (config == null) {
        throw new RuntimeException("Configuration with id '" + configId + "' not found!");
      }
      this.config = config;
      this.txSettings =
          adminStorage.createSettings().withAppId(config.appId).withAuthor(config.author);

      log.atInfo()
          .setMessage("Naksha host/endpoint: {} / {}")
          .addArgument(config.hostname)
          .addArgument(config.endpoint)
          .log();

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
        log.atInfo()
            .setMessage("Loaded JWT key file {}")
            .addArgument(loaded.path())
            .log();
        jwtKey = new String(loaded.bytes(), StandardCharsets.UTF_8);
      }
      {
        final String path = "auth/" + config.jwtName + ".pub";
        final LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(path, false, NakshaHubConfig.APP_NAME);
        log.atInfo()
            .setMessage("Loaded JWT key file {}")
            .addArgument(loaded.path())
            .log();
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
    }
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

  @Override
  public @NotNull ErrorResponse toErrorResponse(@NotNull Throwable throwable) {
    return new ErrorResponse(throwable, null);
  }

  @Override
  public @NotNull <RESPONSE> Future<@Nullable RESPONSE> executeTask(@NotNull Supplier<RESPONSE> execute) {
    return new InternalTask<>(this, execute).start();
  }

  @Override
  public @NotNull Future<@NotNull XyzResponse> executeEvent(
      @NotNull Event event, @NotNull EventFeature eventFeature) {
    return new InternalEventTask(this, event, eventFeature).start();
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

  /**
   * The admin storage.
   */
  private final @NotNull PsqlStorage adminStorage;

  @Override
  public @NotNull IStorage storage() {
    return adminStorage;
  }

  @Override
  public @NotNull ITransactionSettings settings() {
    return adminStorage.createSettings().withAppId(config().appId);
  }

  private final @NotNull ITransactionSettings txSettings;

  /**
   * Returns a new master transaction, must be closed.
   *
   * @return A new master transaction, must be closed.
   */
  public @NotNull PsqlTxWriter writeTx() {
    return adminStorage.openMasterTransaction(txSettings);
  }

  /**
   * Returns a new read transaction, must be closed.
   *
   * @return A new read transaction, must be closed.
   */
  public @NotNull PsqlTxReader readTx() {
    return adminStorage.openReplicationTransaction(txSettings);
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
   * @throws IllegalStateException If the service already started.
   */
  @Override
  public void start() {
    if (!start.compareAndSet(false, true)) {
      throw new IllegalStateException("Service already started");
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
      verticles[i] = new NakshaHttpVerticle(this, i);
      vertx.deployVerticle(verticles[i]);
    }
    Thread.setDefaultUncaughtExceptionHandler(NakshaHub::uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(this.shutdownThread);
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
            .setMessage("Unexpected error while executing Naksha-Hub")
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
