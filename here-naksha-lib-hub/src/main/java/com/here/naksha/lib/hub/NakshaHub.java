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
import static com.here.naksha.lib.core.models.storage.POp.*;
import static com.here.naksha.lib.core.models.storage.PRef.*;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.models.storage.StorageCollection;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
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
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHub implements INaksha {

  /** The data source. */
  // protected final @NotNull PsqlDataSource dataSource;
  /** The NakshaHub config. */
  protected final @NotNull NakshaHubConfig nakshaHubConfig;
  /** Singleton instance of physical admin storage implementation */
  protected final @NotNull IStorage psqlStorage;
  /** Singleton instance of AdminStorage, which internally uses physical admin storage (i.e. PsqlStorage) */
  protected final @NotNull IStorage adminStorageInstance;
  /** Singleton instance of Space Storage, which is responsible to manage admin collections as spaces
   * and support respective read/write operations on spaces */
  protected final @NotNull IStorage spaceStorageInstance;

  public NakshaHub(final @NotNull PsqlConfig config, final @Nullable NakshaHubConfig customCfg) {
    // this.dataSource = new PsqlDataSource(config);
    // create storage instance upfront
    this.psqlStorage = new PsqlStorage(config, 0);
    this.adminStorageInstance = new NHAdminStorage(this.psqlStorage);
    this.spaceStorageInstance = new NHSpaceStorage(this);
    // set default config
    this.nakshaHubConfig = this.storageSetup(customCfg);
    if (this.nakshaHubConfig == null) {
      throw new RuntimeException("Server configuration not found! Neither in Admin storage nor a default file.");
    }
  }

  private NakshaHubConfig storageSetup(final @Nullable NakshaHubConfig customCfg) {
    NakshaHubConfig crtCfg = customCfg;
    /**
     * 1. Init Admin Storage
     * 2. Create all Admin collections
     * 3. run maintenance during to ensure history partitions are available
     * 4. fetch / add latest config (ordered preference DB,Custom,Default)
     */

    // 1. Init Admin Storage
    getAdminStorage().initStorage();

    try (final IWriteSession admin = getAdminStorage().newWriteSession(new NakshaContext(), true)) {
      // 2. Create all Admin collections in Admin DB
      {
        final List<WriteOp<StorageCollection>> collectionList = new ArrayList<>();
        for (final String name : NakshaAdminCollection.ALL) {
          final StorageCollection collection =
              new StorageCollection(name); // TODO HP : Need to specify history flag and maxAge
          final WriteOp<StorageCollection> writeOp = new WriteOp<>(
              collection,
              null,
              name,
              null,
              false,
              IfExists.RETAIN,
              IfConflict.RETAIN,
              IfNotExists.CREATE);
          collectionList.add(writeOp);
        }
        final Result wrResult = admin.execute(new WriteCollections<>(collectionList));
        if (wrResult instanceof ErrorResult er) {
          admin.rollback();
          throw unchecked(new Exception(
              "Unable to create Admin collections in Admin DB. " + er.toString(), er.exception));
        }
        admin.commit();
      }
    } // close Admin DB connection

    // 3. run one-time maintenance on Admin DB to ensure history partitions are available
    getAdminStorage().maintainNow();

    try (final IWriteSession admin = getAdminStorage().newWriteSession(new NakshaContext(), true)) {
      // TODO HP : This step to be removed later (once we have ability to add custom storages)
      // fetch / add default storage implementation
      {
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
        final WriteOp<Storage> writeOp = new WriteOp<>(
            defStorage,
            null,
            defStorage.getId(),
            null,
            false,
            IfExists.RETAIN,
            IfConflict.RETAIN,
            IfNotExists.CREATE);
        final List<WriteOp<Storage>> featureList = new ArrayList<>();
        featureList.add(writeOp);
        final Result wrResult = admin.execute(new WriteFeatures<>(NakshaAdminCollection.STORAGES, featureList));
        if (wrResult instanceof ErrorResult er) {
          admin.rollback();
          throw unchecked(
              new Exception("Unable to add default storage in Admin DB. " + er.toString(), er.exception));
        }
        admin.commit();
      }

      // 4. fetch / add latest config (ordered preference DB,Custom,Default)
      {
        // load default config from file, if custom config is not provided
        if (crtCfg == null) {
          try (final Json json = Json.get()) {
            final String configJson = IoHelp.readResource("config/default-local.json");
            crtCfg = json.reader(ViewDeserialize.Storage.class)
                .forType(NakshaHubConfig.class)
                .readValue(configJson);
          } catch (Exception e) {
            throw unchecked(new Exception("Unable to read default Config file. " + e.getMessage(), e));
          }
        }
        // Read config from Admin DB (if available)
        NakshaHubConfig dbCfg = null;
        final ReadFeatures featureRequest = new ReadFeatures()
            .addCollection(NakshaAdminCollection.CONFIGS)
            .withPropertyOp(eq(id(), "default-config"));
        final Result rdResult = admin.execute(featureRequest);
        if (rdResult instanceof ErrorResult er) {
          throw unchecked(new Exception(
              "Unable to read default config from Admin DB. " + er.toString(), er.exception));
        }
        if (rdResult instanceof ReadResult<?> rr) {
          if (rr.hasMore()) {
            dbCfg = rr.getFeature(NakshaHubConfig.class);
          }
          rr.close();
        }
        // Persist new config in Admin DB (if DB didn't have one)
        if (dbCfg == null && crtCfg != null) {
          final WriteOp<NakshaHubConfig> writeOp = new WriteOp<>(
              crtCfg,
              null,
              crtCfg.getId(),
              null,
              false,
              IfExists.RETAIN,
              IfConflict.RETAIN,
              IfNotExists.CREATE);
          final List<WriteOp<NakshaHubConfig>> featureList = new ArrayList<>();
          featureList.add(writeOp);
          final Result wrResult =
              admin.execute(new WriteFeatures<>(NakshaAdminCollection.CONFIGS, featureList));
          if (wrResult instanceof ErrorResult er) {
            admin.rollback();
            throw unchecked(new Exception(
                "Unable to add default config in Admin DB. " + er.toString(), er.exception));
          }
          admin.commit();
        } else {
          // return DB config
          crtCfg = dbCfg;
        }
      }
    } // close Admin DB connection

    return crtCfg;
  }

  public @NotNull NakshaHubConfig getConfig() {
    return this.nakshaHubConfig;
  }

  @Override
  public @NotNull IStorage getAdminStorage() {
    return this.adminStorageInstance;
  }

  @Override
  public @NotNull IStorage getSpaceStorage() {
    return this.spaceStorageInstance;
  }

  @Override
  public @NotNull IStorage getStorageById(final @NotNull String storageId) {
    // TODO HP : Add logic
    return null;
  }

  // TODO HP : remove at the end
  @Override
  public @NotNull ErrorResponse toErrorResponse(@NotNull Throwable throwable) {
    return null;
  }

  // TODO HP : remove at the end
  @Override
  public @NotNull <RESPONSE> Future<@NotNull RESPONSE> executeTask(@NotNull Supplier<@NotNull RESPONSE> execute) {
    return null;
  }

  // TODO HP : remove at the end
  @Override
  public @NotNull Future<@NotNull XyzResponse> executeEvent(
      @NotNull Event event, @NotNull EventFeature eventFeature) {
    return null;
  }

  // TODO HP : remove at the end
  @Override
  public @NotNull IStorage storage() {
    return null;
  }

  // TODO HP : remove at the end
  @Override
  public @NotNull ITransactionSettings settings() {
    return null;
  }
}
