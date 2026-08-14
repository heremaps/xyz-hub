/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.util;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.generateUniqueTableName;

import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import java.util.HashMap;
import java.util.Map;

/**
 * Single entry point for the "layer to table" matching on the Hub side.
 * <p>
 * The physical table name of a space is a property of the space itself. It is stored within
 * {@code space.storage.params.tableName} and it is read from there by all components
 * (see {@code XyzSpaceTableHelper#getTableNameFromSpaceParamsOrSpaceId}).
 * <p>
 * For newly created spaces the name is generated once and is completely independent of the space ID.
 * For legacy spaces (which do not have that property) the table name keeps being derived from the space ID,
 * so this class never modifies an already existing space.
 */
public class SpaceTableResolver {

  private SpaceTableResolver() {}

  /**
   * @param space The space for which to read the physical table name
   * @return The physical table name of the space or {@code null} if the space has no explicit one
   *     (in which case the table name is derived from the space ID by the storage connector)
   */
  public static String getTableName(Space space) {
    if (space == null || space.getStorage() == null)
      return null;
    return getTableName(space.getStorage().getParams());
  }

  /**
   * @param storageParams The storage params of a space
   * @return The physical table name contained in the specified params or {@code null}
   */
  public static String getTableName(Map<String, Object> storageParams) {
    if (storageParams == null)
      return null;
    return storageParams.get(TABLE_NAME) instanceof String tableName && !tableName.isEmpty() ? tableName : null;
  }

  /**
   * Assigns a newly generated, unique physical table name to the specified (newly created) space,
   * in case it does not have one yet.
   * <p>
   * @param space The space to be created
   * @return The physical table name of the space
   */
  public static String assignTableName(Space space) {
    ConnectorRef storage = space.getStorage();
    if (storage == null)
      throw new IllegalStateException("The space " + space.getId() + " has no storage definition.");

    String existingTableName = getTableName(storage.getParams());
    if (existingTableName != null)
      return existingTableName;

    Map<String, Object> params = storage.getParams() == null ? new HashMap<>() : new HashMap<>(storage.getParams());
    String tableName = generateUniqueTableName();
    params.put(TABLE_NAME, tableName);
    storage.setParams(params);
    return tableName;
  }

  /**
   * Removes an inherited physical table name from the specified params.
   * <p>
   * That is necessary whenever the storage definition of another space is copied (e.g., for extending spaces),
   * because the physical table name must never be shared between two spaces.
   *
   * @param storageParams The storage params to be sanitized (may be {@code null})
   * @return A modifiable copy of the specified params without the table name
   */
  public static Map<String, Object> withoutTableName(Map<String, Object> storageParams) {
    Map<String, Object> params = storageParams == null ? new HashMap<>() : new HashMap<>(storageParams);
    params.remove(TABLE_NAME);
    return params;
  }
}

