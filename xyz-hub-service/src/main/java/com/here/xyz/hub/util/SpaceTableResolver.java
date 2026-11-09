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

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.generateUniqueTableName;

import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.util.db.pg.LazySpaceTables;
import java.nio.charset.StandardCharsets;
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

  static final int POSTGRES_IDENTIFIER_MAX_BYTES = 63;

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
    if (existingTableName != null) {
      validateTableName(existingTableName);
      return existingTableName;
    }

    Map<String, Object> params = storage.getParams() == null ? new HashMap<>() : new HashMap<>(storage.getParams());
    String tableName = generateUniqueTableName();
    params.put(TABLE_NAME, tableName);
    storage.setParams(params);
    return tableName;
  }

  /**
   * Copies the immutable physical table identity from one incarnation of a space definition to another.
   * Legacy spaces intentionally keep the property absent so connector-side ID resolution remains in effect.
   * The lazy-table flag of the source is copied as well (also for legacy spaces), and a flag of the target that the
   * source does not have is removed.
   *
   * @param source The authoritative space definition
   * @param target The space definition to normalize
   */
  public static void preserveTableName(Space source, Space target) {
    if (target == null)
      return;

    boolean lazyTable = source != null && source.getStorage() != null && Space.isLazyTable(source.getStorage().getParams());
    String tableName = getTableName(source);
    if (target.getStorage() == null) {
      if (tableName == null && !lazyTable)
        return;

      ConnectorRef sourceStorage = source.getStorage();
      Map<String, Object> sourceParams = sourceStorage.getParams() == null
          ? new HashMap<>()
          : new HashMap<>(sourceStorage.getParams());
      preserveLazyTable(lazyTable, sourceParams);
      target.setStorage(new ConnectorRef().withId(sourceStorage.getId()).withParams(sourceParams));
      return;
    }

    //The lazy-table flag is owned by the service: an update always keeps the value of the authoritative definition
    Map<String, Object> targetParams = target.getStorage().getParams();
    if (lazyTable || targetParams != null && targetParams.containsKey(LAZY_TABLE)) {
      Map<String, Object> params = targetParams == null ? new HashMap<>() : new HashMap<>(targetParams);
      preserveLazyTable(lazyTable, params);
      target.getStorage().setParams(params.isEmpty() ? null : params);
    }

    Map<String, Object> currentParams = target.getStorage().getParams();
    if (tableName == null && (currentParams == null || !currentParams.containsKey(TABLE_NAME)))
      return;

    Map<String, Object> params = currentParams == null ? new HashMap<>() : new HashMap<>(currentParams);
    if (tableName == null)
      params.remove(TABLE_NAME);
    else
      params.put(TABLE_NAME, tableName);
    target.getStorage().setParams(params.isEmpty() ? null : params);
  }

  private static void preserveLazyTable(boolean lazyTable, Map<String, Object> params) {
    //Only a boolean true is kept, so that no other value survives an update
    if (lazyTable)
      params.put(LAZY_TABLE, true);
    else
      params.remove(LAZY_TABLE);
  }

  /**
   * Removes a client-supplied lazy-table flag from the storage params of a space that is about to be created.
   * The flag is set by the service only, see {@link Space#LAZY_TABLE}.
   *
   * @param space The space to be created
   */
  public static void removeLazyTable(Space space) {
    if (space == null || space.getStorage() == null || space.getStorage().getParams() == null
        || !space.getStorage().getParams().containsKey(LAZY_TABLE))
      return;

    Map<String, Object> params = new HashMap<>(space.getStorage().getParams());
    params.remove(LAZY_TABLE);
    space.getStorage().setParams(params.isEmpty() ? null : params);
  }

  /**
   * Marks a space that is about to be created as lazy: its table is created by the first write.
   *
   * @param space The space to be created
   */
  public static void setLazyTable(Space space) {
    ConnectorRef storage = space.getStorage();
    Map<String, Object> params = storage.getParams() == null ? new HashMap<>() : new HashMap<>(storage.getParams());
    params.put(LAZY_TABLE, true);
    storage.setParams(params);
  }

  /**
   * Returns an incarnation-aware cache identity for a space. Persisted physical table names distinguish recreated
   * spaces, while legacy spaces retain their logical-ID based identity.
   */
  public static String getTableIdentity(Space space) {
    if (space == null)
      return null;
    String storageId = space.getStorage() != null ? space.getStorage().getId() : null;
    String tableName = getTableName(space);
    return String.valueOf(storageId) + ":" + (tableName != null ? "physical:" + tableName : "legacy:" + space.getId());
  }

  private static void validateTableName(String tableName) {
    if (tableName.indexOf('\0') >= 0)
      throw new IllegalArgumentException("The physical table name must not contain a null character.");
    if (tableName.getBytes(StandardCharsets.UTF_8).length > POSTGRES_IDENTIFIER_MAX_BYTES)
      throw new IllegalArgumentException(
          "The physical table name must not exceed " + POSTGRES_IDENTIFIER_MAX_BYTES + " UTF-8 bytes.");
    if (LazySpaceTables.isReserved(tableName))
      throw new IllegalArgumentException(
          "The physical table name must not start with the reserved prefix \"" + LazySpaceTables.RESERVED_PREFIX + "\".");
  }

  /**
   * Removes an inherited physical table name and the lazy-table flag from the specified params.
   * <p>
   * That is necessary whenever the storage definition of another space is copied (e.g., for extending spaces),
   * because the physical table name must never be shared between two spaces, and the flag belongs to the table.
   *
   * @param storageParams The storage params to be sanitized (may be {@code null})
   * @return A modifiable copy of the specified params without the table name and without the lazy-table flag
   */
  public static Map<String, Object> withoutTableName(Map<String, Object> storageParams) {
    Map<String, Object> params = storageParams == null ? new HashMap<>() : new HashMap<>(storageParams);
    params.remove(TABLE_NAME);
    params.remove(LAZY_TABLE);
    return params;
  }
}