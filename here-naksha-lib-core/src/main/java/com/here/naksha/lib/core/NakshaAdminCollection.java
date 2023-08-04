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

import com.here.naksha.lib.core.storage.CollectionInfo;
import java.util.List;

/**
 * All well-known collections of the Naksha-Hub itself. Still, not all Naksha-Hubs may support them, for example the Naksha extension
 * library currently does not support any collection out of the box!
 */
public final class NakshaAdminCollection {

  /**
   * The admin schema.
   */
  public static final String SCHEMA = "naksha";

  /**
   * The admin-database is the last database (1,099,511,627,775).
   */
  public static final long ADMIN_DB_NUMBER = 0x0000_00ff_ffff_ffffL;

  /**
   * The Naksha-Hub configurations.
   */
  public static CollectionInfo CONFIGS = new CollectionInfo("naksha:configs", ADMIN_DB_NUMBER);

  /**
   * The collections for all catalogs.
   */
  public static CollectionInfo CATALOGS = new CollectionInfo("naksha:catalogs", ADMIN_DB_NUMBER);

  /**
   * The collections for all spaces.
   */
  public static CollectionInfo SPACES = new CollectionInfo("naksha:spaces", ADMIN_DB_NUMBER);

  /**
   * The collections for all subscriptions.
   */
  public static CollectionInfo SUBSCRIPTIONS = new CollectionInfo("naksha:subscriptions", ADMIN_DB_NUMBER);

  /**
   * The collections for all connectors.
   */
  public static CollectionInfo CONNECTORS = new CollectionInfo("naksha:connectors", ADMIN_DB_NUMBER);

  /**
   * The collections for all storages.
   */
  public static CollectionInfo STORAGES = new CollectionInfo("naksha:storages", ADMIN_DB_NUMBER);

  /**
   * The collections for all extensions.
   */
  public static CollectionInfo EXTENSIONS = new CollectionInfo("naksha:extensions", ADMIN_DB_NUMBER);

  /**
   * List of all AdminDB collections.
   */
  public static List<CollectionInfo> COLLECTION_INFO_LIST =
      List.of(CONFIGS, CATALOGS, SPACES, SUBSCRIPTIONS, CONNECTORS, STORAGES, EXTENSIONS);
}
