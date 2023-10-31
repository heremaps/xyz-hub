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
package com.here.naksha.lib.hub.mock;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import com.here.naksha.lib.psql.PsqlConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHubMock implements INaksha {

  /**
   * The in-memory mock storage, which will be used by Storage Mock reader/writer instances.
   *
   * This is used to support local development and mocked based unit testing,
   * bypassing full/part of PsqlStorage implementation.
   * 1. AdminStorage instance - will internally be mocked using in memory collections
   * 2. SpaceStorage instance - will continue using NakshaHub's real implementation, as that logic has to be part of tests
   * 3. As no interactions will happen with database, not all operations may be simulated accurately (e.g. history, subscriptions)
   *
   * Mock storage will hold collections in memory as:
   *     <space_id 1> holds collection of features <id, Feature>
   *     <space_id 2> holds collection of features <id, Feature>
   *       :
   *     <space_id n> holds collection of features <id, Feature>
   *
   * where,
   *     mock collection, will:
   *       - mandatorily hold admin virtual spaces e.g. naksha:storages, naksha:event_handlers, naksha:spaces
   *       - optional custom spaces e.g. "foo", "bar"
   *
   */
  protected final @NotNull Map<String, Map<String, Object>> mockCollection;

  /** The NakshaHub config. */
  protected final @NotNull NakshaHubConfig nakshaHubConfig;
  /** Singleton instance of AdminStorage, which internally uses mocked admin storage (i.e. NHAdminMock) */
  protected final @NotNull IStorage adminStorageInstance;
  /** Singleton instance of Space Storage, which is responsible to manage admin collections as spaces
   * and support respective read/write operations on spaces */
  protected final @NotNull IStorage spaceStorageInstance;

  public NakshaHubMock(
      final @NotNull PsqlConfig config,
      final @NotNull NakshaHubConfig customCfg,
      final @Nullable String configId) {
    mockCollection = new ConcurrentHashMap<>();
    // create storage instances upfront
    this.adminStorageInstance = new NHAdminMock(mockCollection, customCfg);
    this.spaceStorageInstance = new NHSpaceStorage(this);
    this.nakshaHubConfig = customCfg;
  }

  /**
   * Returns a thin wrapper above the admin-database that adds authorization and internal event handling. Basically, this allows access to the admin collections.
   *
   * @return the admin-storage.
   */
  @Override
  public @NotNull IStorage getAdminStorage() {
    return this.adminStorageInstance;
  }

  /**
   * Returns a virtual storage that maps spaces to collections and allows to execute requests in spaces.
   *
   * @return the virtual space-storage.
   */
  @Override
  public @NotNull IStorage getSpaceStorage() {
    return this.spaceStorageInstance;
  }

  /**
   * Returns the user defined space storage instance based on storageId as per space collection defined in Naksha admin storage.
   *
   * @param storageId Id of the space storage
   * @return the space-storage
   */
  @Override
  public @NotNull IStorage getStorageById(@NotNull String storageId) {
    return null;
  }

  /**
   * Returns the configuration in use by NakshaHub
   *
   * @return the config
   */
  @Override
  public <T extends XyzFeature> @NotNull T getConfig() {
    return (T) this.nakshaHubConfig;
  }
}
