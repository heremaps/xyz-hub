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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.hub.storages.NHAdminStorage;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlDataSource;
import com.here.naksha.lib.psql.PsqlStorage;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHub implements INaksha {

  /** The data source. */
  protected final @NotNull PsqlDataSource dataSource;
  /** The NakshaHub config. */
  protected final @NotNull NakshaHubConfig nakshaHubConfig;
  /** Singleton instance of physical admin storage implementation */
  protected final @NotNull IStorage psqlStorage;

  public NakshaHub(final @NotNull PsqlConfig config, final @Nullable NakshaHubConfig customCfg) {
    this.dataSource = new PsqlDataSource(config);
    // create storage instance upfront
    this.psqlStorage = new PsqlStorage(config, 0);
    // set default config
    NakshaHubConfig crtCfg = customCfg;
    // TODO HP : Move all this logic inside admin storage
    // TODO HP : Init Admin Storage
    // TODO HP : Read all available admin collections and create the missing ones
    // TODO HP : Extract all collections from result here and create new one which doesn't already exists
    // TODO HP : run maintenance during startup to ensure history partitions are available before we perform Write
    // operations
    // TODO HP : fetch / add default storage
    // TODO HP : fetch / add latest config (ordered preference DB,Custom,Default)
    if (crtCfg == null) {
      throw new RuntimeException("Server configuration not found! Neither in Admin storage nor a default file.");
    }
    this.nakshaHubConfig = crtCfg;
  }

  public @NotNull NakshaHubConfig getConfig() {
    return this.nakshaHubConfig;
  }

  @Override
  public @NotNull IStorage getAdminStorage() {
    return new NHAdminStorage(this.psqlStorage);
  }

  @Override
  public @NotNull IStorage getSpaceStorage() {
    return new NHSpaceStorage(this);
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
