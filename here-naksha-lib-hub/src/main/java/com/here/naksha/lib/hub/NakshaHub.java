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
import static com.here.naksha.lib.core.models.PluginCache.getStorageConstructor;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;
import static com.here.naksha.lib.core.util.storage.RequestHelper.readFeaturesByIdRequest;
import static com.here.naksha.lib.core.util.storage.RequestHelper.readFeaturesByIdsRequest;
import static com.here.naksha.lib.core.util.storage.RequestHelper.upsertFeaturesRequest;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeatureFromResult;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.exceptions.StorageNotFoundException;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.ResultHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.hub.storages.NHAdminStorage;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import com.here.naksha.lib.psql.PsqlStorage;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NakshaHub implements INaksha {

  /**
   * The id of default NakshaHub Config feature object
   */
  public static final @NotNull String DEF_CFG_ID = "default-config";

  private static final @NotNull Logger logger = LoggerFactory.getLogger(NakshaHub.class);

  /**
   * The NakshaHub config.
   */
  protected final @NotNull NakshaHubConfig nakshaHubConfig;

  /**
   * Singleton instance of physical admin storage implementation
   */
  protected final @NotNull IStorage psqlStorage;
  /**
   * Singleton instance of AdminStorage, which internally uses physical admin storage (i.e. PsqlStorage)
   */
  protected final @NotNull IStorage adminStorageInstance;
  /**
   * Singleton instance of Space Storage, which is responsible to manage admin collections as spaces and support respective read/write
   * operations on spaces
   */
  protected final @NotNull IStorage spaceStorageInstance;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NakshaHub(
      final @NotNull String appName,
      final @NotNull String storageUrl,
      final @Nullable NakshaHubConfig customCfg,
      final @Nullable String configId) {
    // create storage instance upfront
    logger.info("NakshaHub initialization started.");
    this.psqlStorage = new PsqlStorage(PsqlStorage.ADMIN_STORAGE_ID, appName, storageUrl);
    this.adminStorageInstance = new NHAdminStorage(this.psqlStorage);
    this.spaceStorageInstance = new NHSpaceStorage(this, new NakshaEventPipelineFactory(this));
    // setup backend storage DB and Hub config
    final NakshaHubConfig finalCfg = this.storageSetup(customCfg, configId);
    if (finalCfg == null) {
      throw new RuntimeException("Server configuration not found! Neither in Admin storage nor a default file.");
    }
    this.nakshaHubConfig = finalCfg;
    logger.info("NakshaHub initialization done!");
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
    logger.info("Initializing Admin storage (if not already).");
    if (customCfg != null && customCfg.storageParams != null) {
      getAdminStorage().initStorage(customCfg.storageParams);
    } else {
      getAdminStorage().initStorage();
    }
    logger.info("Admin storage ready.");

    // 2. Create all Admin collections in Admin DB
    final NakshaContext nakshaContext = new NakshaContext().withAppId(NakshaHubConfig.defaultAppName());
    nakshaContext.attachToCurrentThread();
    try (final IWriteSession admin = getAdminStorage().newWriteSession(nakshaContext, true)) {
      logger.info("WriteCollections Request for {}, against Admin storage.", NakshaAdminCollection.ALL);
      try (final Result wrResult = admin.execute(createAdminCollectionsRequest());
          final ForwardCursor<XyzCollection, XyzCollectionCodec> cursor =
              wrResult.getXyzCollectionCursor(); ) {
        while (cursor.hasNext() && cursor.next()) {
          if (EExecutedOp.CREATED == cursor.getOp()) {
            logger.info(
                "Collection {} successfully created.",
                cursor.getFeature().getId());
            continue;
          } else if (EExecutedOp.ERROR == cursor.getOp()) {
            if (cursor.getError() != null && cursor.getError().err == XyzError.CONFLICT) {
              logger.info(
                  "Collection {} already exists.",
                  cursor.getFeature().getId());
              continue;
            }
          }
          logger.error(
              "Unexpected result while creating Admin collections. Op={}, Error={} ",
              cursor.getOp(),
              cursor.getError());
          admin.rollback(true);
          throw unchecked(new Exception("Unable to create Admin collections in Admin DB."));
        }
      } catch (NoCursor e) {
        logger.error("Unexpected NoCursor exception while creating Admin collections.", e);
        admin.rollback(true);
        throw unchecked(new Exception("Unable to create Admin collections in Admin DB."));
      }
      admin.commit(true);
    } // close Admin DB connection

    // 3. run one-time maintenance on Admin DB to ensure history partitions are available
    logger.info("Running one-time maintenance on Admin storage.");
    getAdminStorage().maintainNow();

    // 4. fetch / add latest config
    return configSetup(nakshaContext, customCfg, configId);
  }

  private static WriteXyzCollections createAdminCollectionsRequest() {
    final WriteXyzCollections writeXyzCollections = new WriteXyzCollections();
    NakshaAdminCollection.ALL.stream().map(XyzCollection::new).forEach(writeXyzCollections::create);
    return writeXyzCollections;
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
    logger.info("Running config setup for Nakshs Hub against Admin storage.");
    try (final IWriteSession admin = getAdminStorage().newWriteSession(nakshaContext, true)) {
      if (customCfg != null) {
        // Custom config provided. Persist in AdminDB.
        final Result wrResult =
            admin.execute(upsertFeaturesRequest(NakshaAdminCollection.CONFIGS, List.of(customCfg)));
        if (wrResult == null) {
          admin.rollback(true);
          throw unchecked(new Exception("Unable to add custom config in Admin DB. Null result!"));
        } else if (wrResult instanceof ErrorResult er) {
          admin.rollback(true);
          throw unchecked(
              new Exception("Unable to add custom config in Admin DB. " + er.toString(), er.exception));
        }
        admin.commit(true);
        return customCfg;
      }

      // load custom + default config from DB (if available)
      NakshaHubConfig customDbCfg = null, defDbCfg = null;
      final List<String> cfgIdList = (configId != null) ? List.of(configId, DEF_CFG_ID) : List.of(DEF_CFG_ID);
      final Result rdResult = admin.execute(readFeaturesByIdsRequest(NakshaAdminCollection.CONFIGS, cfgIdList));
      if (rdResult instanceof ErrorResult er) {
        throw unchecked(
            new Exception("Unable to read custom/default config from Admin DB. " + er, er.exception));
      } else {
        try {
          List<NakshaHubConfig> nakshaHubConfigs =
              ResultHelper.readFeaturesFromResult(rdResult, NakshaHubConfig.class);
          for (final NakshaHubConfig cfg : nakshaHubConfigs) {
            if (cfg.getId().equals(configId)) {
              customDbCfg = cfg;
            }
            if (cfg.getId().equals(DEF_CFG_ID)) {
              defDbCfg = cfg;
            }
          }
          if (customDbCfg != null) {
            return customDbCfg; // return custom config from DB
          } else if (defDbCfg != null) {
            return defDbCfg; // return default config from DB
          }
        } catch (NoCursor | NoSuchElementException er) {
          logger.info("No custom/default config found in Admin DB.");
        }
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
        admin.rollback(true);
        throw unchecked(new Exception("Unable to add default config in Admin DB. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback(true);
        throw unchecked(
            new Exception("Unable to add default config in Admin DB. " + er.toString(), er.exception));
      }
      admin.commit(true);
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
    try (final IReadSession reader = getAdminStorage().newReadSession(NakshaContext.currentContext(), false)) {
      try (final Result result =
          reader.execute(readFeaturesByIdRequest(NakshaAdminCollection.STORAGES, storageId))) {
        if (result instanceof ErrorResult er) {
          throw unchecked(new Exception(
              "Exception fetching storage details for id " + storageId + ". " + er.message,
              er.exception));
        }
        Storage storage = readFeatureFromResult(result, Storage.class);
        if (storage == null) {
          throw unchecked(new StorageNotFoundException(storageId));
        }
        return storageInstance(storage);
      }
    }
  }

  private IStorage storageInstance(@NotNull Storage storage) {
    Fe1<IStorage, Storage> constructor = getStorageConstructor(storage.getClassName(), Storage.class);
    try {
      return constructor.call(storage);
    } catch (Exception e) {
      throw unchecked(e);
    }
  }
}
