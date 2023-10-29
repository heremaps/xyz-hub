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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.storage.RequestHelper.*;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.models.storage.StorageCollection;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.hub.storages.NHAdminStorage;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlStorage;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHub implements INaksha {

  /** The id of default NakshaHub Config feature object */
  public static final @NotNull String DEF_CFG_ID = "default-config";
  /** The NakshaHub config. */
  protected final @NotNull NakshaHubConfig nakshaHubConfig;
  /** Singleton instance of physical admin storage implementation */
  protected final @NotNull IStorage psqlStorage;
  /** Singleton instance of AdminStorage, which internally uses physical admin storage (i.e. PsqlStorage) */
  protected final @NotNull IStorage adminStorageInstance;
  /** Singleton instance of Space Storage, which is responsible to manage admin collections as spaces
   * and support respective read/write operations on spaces */
  protected final @NotNull IStorage spaceStorageInstance;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NakshaHub(
      final @NotNull PsqlConfig config,
      final @Nullable NakshaHubConfig customCfg,
      final @Nullable String configId) {
    // this.dataSource = new PsqlDataSource(config);
    // create storage instance upfront
    this.psqlStorage = new PsqlStorage(config, "naksha-admin-db");
    this.adminStorageInstance = new NHAdminStorage(this.psqlStorage);
    this.spaceStorageInstance = new NHSpaceStorage(this);
    // setup backend storage DB and Hub config
    final NakshaHubConfig finalCfg = this.storageSetup(customCfg, configId);
    if (finalCfg == null) {
      throw new RuntimeException("Server configuration not found! Neither in Admin storage nor a default file.");
    }
    this.nakshaHubConfig = finalCfg;
  }

  private @Nullable NakshaHubConfig storageSetup(
      final @Nullable NakshaHubConfig customCfg, final @Nullable String configId) {
    /**
     * 1. Init Admin Storage
     * 2. Create all Admin collections
     * 3. run maintenance during startup to ensure history partitions are available
     * 4. fetch / add latest config (ordered preference DB,Custom,Default)
     */

    // 1. Init Admin Storage
    getAdminStorage().initStorage();

    // 2. Create all Admin collections in Admin DB
    final NakshaContext nakshaContext = new NakshaContext().withAppId(NakshaHubConfig.defaultAppName());
    nakshaContext.attachToCurrentThread();
    try (final IWriteSession admin = getAdminStorage().newWriteSession(nakshaContext, true)) {
      final List<WriteOp<StorageCollection>> collectionList = new ArrayList<>();
      for (final String name : NakshaAdminCollection.ALL) {
        final StorageCollection collection = new StorageCollection(name);
        final WriteOp<StorageCollection> writeOp = new WriteOp<>(EWriteOp.INSERT, collection, false);
        collectionList.add(writeOp);
      }
      final Result wrResult = admin.execute(new WriteCollections<>(collectionList));
      if (wrResult == null) {
        admin.rollback();
        throw unchecked(new Exception("Unable to create Admin collections in Admin DB. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback();
        throw unchecked(new Exception(
            "Unable to create Admin collections in Admin DB. " + er.toString(), er.exception));
      }
      admin.commit();
    } // close Admin DB connection

    // 3. run one-time maintenance on Admin DB to ensure history partitions are available
    getAdminStorage().maintainNow();

    // TODO HP : This step to be removed later (once we have ability to add custom storages)
    // fetch / add default storage implementation
    try (final IWriteSession admin = getAdminStorage().newWriteSession(nakshaContext, true)) {
      Storage defStorage = null;
      try (final Json json = Json.get()) {
        final String storageJson = IoHelp.readResource("config/default-storage.json");
        defStorage = json.reader(ViewDeserialize.Storage.class)
            .forType(Storage.class)
            .readValue(storageJson);
      } catch (Exception e) {
        throw unchecked(new Exception("Unable to read default Storage file. " + e.getMessage(), e));
      }
      // persist in Admin DB (if not already exists)
      final Result wrResult =
          admin.execute(createFeatureRequest(NakshaAdminCollection.STORAGES, defStorage, true));
      if (wrResult == null) {
        admin.rollback();
        throw unchecked(new Exception("Unable to add default storage in Admin DB. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback();
        throw unchecked(
            new Exception("Unable to add default storage in Admin DB. " + er.toString(), er.exception));
      }
      admin.commit();
    } // close Admin DB connection

    // 4. fetch / add latest config
    return configSetup(nakshaContext, customCfg, configId);
  }

  private @Nullable NakshaHubConfig configSetup(
      final @NotNull NakshaContext nakshaContext,
      final @Nullable NakshaHubConfig customCfg,
      final @Nullable String configId) {
    /*
     * Config preference, for a given configId (e.g. "custom-config"):
     * 1. Custom config - If provided, persist the same in DB, and use the same for NakshaHub
     * 2. DB custom config - If Database already has custom config (e.g. "custom-config"), use the same
     * 3. DB default config - If Database has default config - "default-config", use the same
     * 3. Default config - Fallback to default config from file - "default-config"
     */

    try (final IWriteSession admin = getAdminStorage().newWriteSession(nakshaContext, true)) {
      if (customCfg != null) {
        // Custom config provided. Persist in AdminDB.
        final Result wrResult = admin.execute(createFeatureRequest(
            NakshaAdminCollection.CONFIGS, customCfg, IfExists.REPLACE, IfConflict.REPLACE));
        if (wrResult == null) {
          admin.rollback();
          throw unchecked(new Exception("Unable to add custom config in Admin DB. Null result!"));
        } else if (wrResult instanceof ErrorResult er) {
          admin.rollback();
          throw unchecked(
              new Exception("Unable to add custom config in Admin DB. " + er.toString(), er.exception));
        }
        admin.commit();
        return customCfg;
      }

      // load custom + default config from DB (if available)
      NakshaHubConfig customDbCfg = null, defDbCfg = null;
      final List<String> cfgIdList = (configId != null) ? List.of(configId, DEF_CFG_ID) : List.of(DEF_CFG_ID);
      final Result rdResult = admin.execute(readFeaturesByIdsRequest(NakshaAdminCollection.CONFIGS, cfgIdList));
      if (rdResult instanceof ErrorResult er) {
        throw unchecked(new Exception(
            "Unable to read custom/default config from Admin DB. " + er.toString(), er.exception));
      } else if (rdResult instanceof ReadResult<?> rr) {
        for (final NakshaHubConfig cfg : rr.withFeatureType(NakshaHubConfig.class)) {
          if (cfg.getId().equals(configId)) {
            customDbCfg = cfg;
          }
          if (cfg.getId().equals(DEF_CFG_ID)) {
            defDbCfg = cfg;
          }
        }
        rr.close();
      }
      if (customDbCfg != null) {
        return customDbCfg; // return custom config from DB
      } else if (defDbCfg != null) {
        return defDbCfg; // return default config from DB
      }

      // load default config from file (as DB didn't have custom/default config)
      NakshaHubConfig defCfg = null;
      try (final Json json = Json.get()) {
        final String configJson = IoHelp.readResource("config/" + DEF_CFG_ID + ".json");
        defCfg = json.reader(ViewDeserialize.Storage.class)
            .forType(NakshaHubConfig.class)
            .readValue(configJson);
        defCfg.setId(DEF_CFG_ID); // overwrite Id to desired value
      } catch (Exception e) {
        throw unchecked(new Exception("Unable to read default Config file. " + e.getMessage(), e));
      }
      // Persist default config in Admin DB
      final Result wrResult = admin.execute(createFeatureRequest(NakshaAdminCollection.CONFIGS, defCfg, true));
      if (wrResult == null) {
        admin.rollback();
        throw unchecked(new Exception("Unable to add default config in Admin DB. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback();
        throw unchecked(
            new Exception("Unable to add default config in Admin DB. " + er.toString(), er.exception));
      }
      admin.commit();
      return defCfg; // return default config obtained from file
    }
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull <T extends XyzFeature> T getConfig() {
    return (T) this.nakshaHubConfig;
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IStorage getAdminStorage() {
    return this.adminStorageInstance;
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IStorage getSpaceStorage() {
    return this.spaceStorageInstance;
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IStorage getStorageById(final @NotNull String storageId) {
    // TODO : Add logic to retrieve Storage from Admin DB and then instantiate respective IStorage implementation
    return null;
  }
}
