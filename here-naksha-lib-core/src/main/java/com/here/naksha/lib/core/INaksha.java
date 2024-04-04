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
package com.here.naksha.lib.core;

import com.here.naksha.lib.core.models.ExtensionConfig;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.IStorage;
import org.jetbrains.annotations.NotNull;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host implementation and exposes it to the Naksha API. The
 * reference implementation is based upon the PostgresQL database, but alternative implementations are possible, for example the Naksha
 * extension library will fake a Naksha-Hub.
 */
@SuppressWarnings("unused")
public interface INaksha {

  /**
   * Returns a thin wrapper above the admin-database that adds authorization and internal event handling. Basically, this allows access to the admin collections.
   * @return the admin-storage.
   */
  @NotNull
  IStorage getAdminStorage();

  /**
   * Returns a virtual storage that maps spaces to collections and allows to execute requests in spaces.
   * @return the virtual space-storage.
   */
  @NotNull
  IStorage getSpaceStorage();

  /**
   * Returns the user defined space storage instance based on storageId as per space collection defined in Naksha admin storage.
   * @param storageId Id of the space storage
   * @return the space-storage
   */
  @NotNull
  IStorage getStorageById(final @NotNull String storageId);

  /**
   * Returns the configuration in use by NakshaHub
   * @return the config
   */
  @NotNull
  <T extends XyzFeature> T getConfig();

  @NotNull
  ExtensionConfig getExtensionConfig();

  @NotNull
  ClassLoader getClassLoader(@NotNull String extensionId);
}
