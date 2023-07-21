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

import static com.here.naksha.lib.core.util.NakshaHelper.listToMap;
import static java.lang.System.err;
import static java.lang.System.exit;

import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlConfigBuilder;
import com.here.naksha.lib.psql.PsqlStorage;
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
public class NakshaHub implements INaksha {

  /**
   * Entry point when used in a JAR as bootstrap class.
   *
   * @param args The console arguments given.
   */
  public static void main(@NotNull String @NotNull [] args) {
    final String url;
    if (args.length < 1 || !args[0].startsWith("jdbc:postgresql://")) {
      err.println("Missing argument: <url>");
      err.println("  Example: jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test");
      err.flush();
      exit(1);
      return;
    }
    try {
      final PsqlConfig config = new PsqlConfigBuilder()
          .withAppName("Naksha/v" + NakshaVersion.latest)
          .parseUrl(args[0])
          .build();
      new NakshaHub(config, "demo", null);
    } catch (Throwable t) {
      err.println("Service start failed:");
      t.printStackTrace(err);
      err.flush();
      exit(1);
    }
  }

  /**
   * Create a new Naksha-Hub instance.
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
    adminStorage = new PsqlStorage(adminDbConfig, 1L);
    adminStorage.init();
    try (final PsqlTxWriter tx =
        adminStorage.openMasterTransaction(adminStorage.createSettings().withAppId("hub"))) {
      final Map<@NotNull CollectionInfo, Boolean> existing =
          listToMap(tx.iterateCollections().readAll());
      boolean dirty = false;
      if (!existing.containsKey(AdminCollections.CONFIGS)) {
        dirty = true;
        tx.createCollection(AdminCollections.CONFIGS);
      }
      if (!existing.containsKey(AdminCollections.CATALOGS)) {
        dirty = true;
        tx.createCollection(AdminCollections.CATALOGS);
      }
      if (!existing.containsKey(AdminCollections.EXTENSIONS)) {
        dirty = true;
        tx.createCollection(AdminCollections.EXTENSIONS);
      }
      if (!existing.containsKey(AdminCollections.CONNECTORS)) {
        dirty = true;
        tx.createCollection(AdminCollections.CONNECTORS);
      }
      if (!existing.containsKey(AdminCollections.STORAGES)) {
        dirty = true;
        tx.createCollection(AdminCollections.STORAGES);
      }
      // Eventable.
      if (!existing.containsKey(AdminCollections.SPACES)) {
        dirty = true;
        tx.createCollection(AdminCollections.SPACES);
      }
      if (!existing.containsKey(AdminCollections.SUBSCRIPTIONS)) {
        dirty = true;
        tx.createCollection(AdminCollections.SUBSCRIPTIONS);
      }
      if (dirty) {
        tx.commit();
      }
    }
  }

  /**
   * The admin storage.
   */
  private final @NotNull PsqlStorage adminStorage;

  /**
   * Discover or generate a unique instance identifier.
   *
   * @return The unique instance identifier.
   */
  protected @NotNull String discoverInstanceId() {
    // TODO: We may use the MAC address of the network interface.
    //       We may try to detect the instance ID from EC2 metadata.
    //       ...
    return UUID.randomUUID().toString();
  }

  @Override
  public <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newEventTask(
      @NotNull Class<EVENT> eventClass) {
    // TODO: Implement me!
    return null;
  }

  @Override
  public @NotNull IStorage adminStorage() {
    return adminStorage;
  }
}
