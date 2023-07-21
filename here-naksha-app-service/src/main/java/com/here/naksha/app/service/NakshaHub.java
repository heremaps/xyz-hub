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

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.NakshaHelper.listToMap;
import static java.lang.System.err;
import static java.lang.System.exit;

import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.features.NakshaConfig;
import com.here.naksha.lib.core.models.payload.Event;
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
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The main service instance.
 */
@SuppressWarnings("unused")
public final class NakshaHub extends Thread implements INaksha {

  /**
   * Entry point when used in a JAR as bootstrap class.
   *
   * @param args The console arguments given.
   */
  public static void main(@NotNull String @NotNull [] args) {
    final String url;
    if (args.length < 2 || !args[0].startsWith("jdbc:postgresql://") || args[1].length() == 0) {
      err.println("Missing argument: <url> <configId>");
      err.println(
          "Example: java -jar naksha.jar jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=naksha local");
      err.flush();
      exit(1);
      return;
    }
    // Potentially we could override the app-name:
    // NakshaHubConfig.APP_NAME = ?
    try {
      final PsqlConfig config = new PsqlConfigBuilder()
          .withAppName(APP_NAME + "/v" + NakshaVersion.latest)
          .parseUrl(args[0])
          .withSchema(NakshaAdminCollection.SCHEMA)
          .build();
      new NakshaHub(config, args[1], null);
    } catch (Throwable t) {
      err.println("Service start failed:");
      t.printStackTrace(err);
      err.flush();
      exit(1);
    }
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
    if (instanceId == null) {
      instanceId = this.discoverInstanceId();
    }
    this.instanceId = instanceId;
    adminStorage = new PsqlStorage(adminDbConfig, 1L);
    adminStorage.init();
    final ITransactionSettings tempSettings = adminStorage.createSettings().withAppId(adminDbConfig.appName);
    NakshaConfig config;
    try (final PsqlTxWriter tx = adminStorage.openMasterTransaction(tempSettings)) {
      final Map<@NotNull CollectionInfo, Boolean> existing =
          listToMap(tx.iterateCollections().readAll());
      if (!existing.containsKey(NakshaAdminCollection.CATALOGS)) {
        tx.createCollection(NakshaAdminCollection.CATALOGS);
        tx.commit();
      }
      if (!existing.containsKey(NakshaAdminCollection.EXTENSIONS)) {
        tx.createCollection(NakshaAdminCollection.EXTENSIONS);
        tx.commit();
      }
      if (!existing.containsKey(NakshaAdminCollection.CONNECTORS)) {
        tx.createCollection(NakshaAdminCollection.CONNECTORS);
        tx.commit();
      }
      if (!existing.containsKey(NakshaAdminCollection.STORAGES)) {
        tx.createCollection(NakshaAdminCollection.STORAGES);
        tx.commit();
      }

      if (!existing.containsKey(NakshaAdminCollection.SPACES)) {
        tx.createCollection(NakshaAdminCollection.SPACES);
        tx.commit();
      }
      if (!existing.containsKey(NakshaAdminCollection.SUBSCRIPTIONS)) {
        tx.createCollection(NakshaAdminCollection.SUBSCRIPTIONS);
        tx.commit();
      }

      if (!existing.containsKey(NakshaAdminCollection.CONFIGS)) {
        tx.createCollection(NakshaAdminCollection.CONFIGS);
        tx.commit();
        adminStorage.getDataSource().initConnection(tx.getConnection());
        try (final Json json = Json.get()) {
          final String configJson = IoHelp.readResource("config/local.json");
          config = json.reader(ViewDeserialize.Storage.class)
              .forType(NakshaConfig.class)
              .readValue(configJson);
          tx.writeFeatures(NakshaConfig.class, NakshaAdminCollection.CONFIGS)
              .modifyFeatures(new ModifyFeaturesReq<NakshaConfig>(false).insert(config));
          tx.commit();
        } catch (Exception e) {
          throw unchecked(e);
        }
      } else {
        config = tx.readFeatures(NakshaConfig.class, NakshaAdminCollection.CONFIGS)
            .getFeatureById(configId);
      }

      // Read the configuration.
      try (final Json json = Json.get()) {
        final LoadedBytes loaded =
            IoHelp.readBytesFromHomeOrResource(configId + ".json", false, adminDbConfig.appName);
        json.reader(ViewDeserialize.Storage.class)
            .forType(NakshaConfig.class)
            .readValue(loaded.bytes());
        currentLogger()
            .atInfo("Loading configuration file from {}")
            .add(loaded.path())
            .log();
      } catch (Throwable t) {
        currentLogger()
            .atInfo("Failed to load {}.json from XGD config directory ~/.config/{}/{}.json")
            .add(configId)
            .add(adminDbConfig.appName)
            .add(configId)
            .log();
      }
      tx.readFeatures(NakshaConfig.class, NakshaAdminCollection.CONFIGS);
      this.config = config;
      this.txSettings =
          adminStorage.createSettings().withAppId(config.appId).withAuthor(config.author);
    }
  }

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

  @Override
  public <E extends Event, T extends AbstractTask<E>> @NotNull T newEventTask(@NotNull Class<E> eventClass) {
    // TODO: Implement me!
    return null;
  }

  /**
   * The default application name, used as identifier when accessing the PostgresQL database and to read the configuration file using the
   * <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XGD</a> standard, therefore from directory
   * ({@code ~/.config/<appname>/...}).
   */
  public static @NotNull String APP_NAME = "naksha";

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
  private @NotNull NakshaConfig config;

  /**
   * Returns the configuration of this hub.
   *
   * @return The configuration of this hub.
   */
  public @NotNull NakshaConfig config() {
    return config;
  }

  /**
   * The admin storage.
   */
  private final @NotNull PsqlStorage adminStorage;

  @Override
  public @NotNull IStorage adminStorage() {
    return adminStorage;
  }

  private final @NotNull ITransactionSettings txSettings;

  /**
   * Returns a new master transaction, must be closed.
   * @return A new master transaction, must be closed.
   */
  public @NotNull PsqlTxWriter writeTx() {
    return adminStorage.openMasterTransaction(txSettings);
  }

  /**
   * Returns a new read transaction, must be closed.
   * @return A new read transaction, must be closed.
   */
  public @NotNull PsqlTxReader readTx() {
    return adminStorage.openReplicationTransaction(txSettings);
  }

  /**
   * A web client to access XYZ Hub nodes and other web resources.
   */
  // public final @NotNull WebClient webClient;

  /**
   * The VertX-Options.
   */
  // public final @NotNull VertxOptions vertxOptions;

  /**
   * The VertX-Metrics-Options
   */
  // public final @NotNull MetricsOptions vertxMetricsOptions;

  /**
   * The entry point to the Vert.x core API.
   */
  // public final @NotNull Vertx vertx;

  /**
   * The authentication options used to generate the {@link #authProvider}.
   */
  // public final @NotNull JWTAuthOptions authOptions;

  /**
   * The auth-provider.
   */
  // public final @NotNull NakshaAuthProvider authProvider;

  /**
   * Internally called, when invoked from external raises a {@link UnsupportedOperationException}.
   */
  public void run() {
    if (this != Thread.currentThread()) {
      throw new UnsupportedOperationException(
          "Illegal invocation, the run method can only be invoked from the hub itself");
    }
  }
}
