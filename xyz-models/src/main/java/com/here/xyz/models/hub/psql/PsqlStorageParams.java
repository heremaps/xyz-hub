/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.models.hub.psql;

import static com.here.xyz.EventTask.currentTask;

import com.here.xyz.XyzSerializable;
import com.here.xyz.EventHandlerParams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The PostgresQL connector parameters.
 */
@SuppressWarnings("unused")
public class PsqlStorageParams extends EventHandlerParams {

  /**
   * Paramters
   */
  public final static String ID = "id";
  public final static String CONNECTOR_ID = "connectorId";
  public final static String PROPERTY_SEARCH = "propertySearch";
  public final static String MVT_SUPPORT = "mvtSupport";
  public final static String AUTO_INDEXING = "autoIndexing";
  public final static String ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
  public final static String COMPACT_HISTORY = "compactHistory";
  public final static String ON_DEMAND_IDX_LIMIT = "onDemandIdxLimit";
  public final static String HRN_SHORTENING = "hrnShortening";
  public final static String IGNORE_CREATE_MSE = "ignoreCreateMse";

  private final @NotNull String id;
  private final long connectorId;
  private final boolean propertySearch;
  private final boolean mvtSupport;
  private final boolean autoIndexing;
  private final boolean enableHashedSpaceId;
  private final boolean compactHistory;
  private final int onDemandIdxLimit;
  private final boolean hrnShortening;
  private final boolean ignoreCreateMse;

  private final @NotNull PsqlPoolConfig dbConfig;
  private final @NotNull List<@NotNull PsqlPoolConfig> dbReplicas;
  private final @NotNull String spaceRole;
  private final @NotNull String spaceSchema;
  private final @NotNull String adminRole;
  private final @NotNull String adminSchema;

  /**
   * Parse the given connector params into this type-safe class.
   *
   * @param connectorParams the connector parameters.
   * @throws NullPointerException     if a value is {@code null} that must not be null.
   * @throws IllegalArgumentException if a value has an invalid type, for example a map expected, and a string found.
   */
  @SuppressWarnings("unchecked")
  public PsqlStorageParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams) throws NullPointerException {
    Object raw = connectorParams.get("dbConfig");
    if (!(raw instanceof Map)) {
      throw new IllegalArgumentException("dbConfig");
    }
    this.dbConfig = XyzSerializable.fromAnyMap((Map<String, Object>) raw, PsqlPoolConfig.class);
    raw = connectorParams.get("dbReplicas");
    final ArrayList<@NotNull PsqlPoolConfig> replicas;
    if (raw instanceof List) {
      final List<Object> rawList = (List<Object>) raw;
      final int SIZE = rawList.size();
      replicas = new ArrayList<>(SIZE);
      for (final Object o : rawList) {
        if (o instanceof Map) {
          replicas.add(XyzSerializable.fromAnyMap((Map<String, Object>) o, PsqlPoolConfig.class));
        }
      }
    } else {
      replicas = new ArrayList<>();
    }
    dbReplicas = replicas;
    spaceRole = parseValue(connectorParams, "spaceRole", dbConfig.user);
    adminRole = parseValue(connectorParams, "adminRole", spaceRole);
    spaceSchema = parseValue(connectorParams, "spaceSchema", "naksha_spaces");
    adminSchema = parseValue(connectorParams, "adminSchema", "naksha_admin");
    id = parseValue(connectorParams, ID, String.class);
    connectorId = parseValue(connectorParams, CONNECTOR_ID, Long.class);
    if (connectorId <= 0L) {
      currentTask().warn("Illegal cid: {}", connectorParams.get(CONNECTOR_ID));
    }
    autoIndexing = parseValue(connectorParams, AUTO_INDEXING, false);
    propertySearch = parseValue(connectorParams, PROPERTY_SEARCH, false);
    mvtSupport = parseValue(connectorParams, MVT_SUPPORT, false);
    enableHashedSpaceId = parseValue(connectorParams, ENABLE_HASHED_SPACEID, false);
    compactHistory = parseValue(connectorParams, COMPACT_HISTORY, true);
    onDemandIdxLimit = parseValue(connectorParams, ON_DEMAND_IDX_LIMIT, 4);
    hrnShortening = parseValue(connectorParams, HRN_SHORTENING, false);
    ignoreCreateMse = parseValue(connectorParams, IGNORE_CREATE_MSE, false);
  }

  public @NotNull String getId() {
    return id;
  }

  public long getConnectorId() {
    return connectorId;
  }

  public boolean isPropertySearch() {
    return propertySearch;
  }

  public boolean isMvtSupport() {
    return mvtSupport;
  }

  public boolean isAutoIndexing() {
    return autoIndexing;
  }

  public boolean isEnableHashedSpaceId() {
    return enableHashedSpaceId;
  }

  public boolean isCompactHistory() {
    return compactHistory;
  }

  public int getOnDemandIdxLimit() {
    return onDemandIdxLimit;
  }

  public boolean isHrnShortening() {
    return hrnShortening;
  }

  public boolean isIgnoreCreateMse() {
    return ignoreCreateMse;
  }

  /**
   * Returns the schema in which to store the spaces.
   *
   * @return the schema in which to store the spaces.
   */
  public @NotNull String getSpaceSchema() {
    return spaceSchema;
  }

  /**
   * Returns the role to use when modifying space content.
   *
   * @return the role to use when modifying space content.
   */
  public @NotNull String getSpaceRole() {
    return spaceSchema;
  }

  /**
   * Returns the schema in which to store the admin tables (transactions, ...).
   *
   * @return the schema in which to store the admin tables (transactions, ...).
   */
  public @NotNull String getAdminSchema() {
    return spaceSchema;
  }

  /**
   * Returns the role to use when managing spaces.
   *
   * @return the role to use when managing spaces.
   */
  public @NotNull String getAdminRole() {
    return spaceSchema;
  }

  /**
   * Returns the master database configuration.
   *
   * @return the master database configuration.
   */
  public @NotNull PsqlPoolConfig getDbConfig() {
    return dbConfig;
  }

  /**
   * Returns all configured replicas; the returned list may be empty.
   *
   * @return all configured replicas; the returned list may be empty.
   */
  public @NotNull List<@NotNull PsqlPoolConfig> getDbReplicas() {
    return dbReplicas;
  }

  @Override
  public String toString() {
    // TODO: Add replicas?
    return "ConnectorParameters{" +
        "propertySearch=" + propertySearch +
        ", mvtSuppoert=" + mvtSupport +
        ", autoIndexing=" + autoIndexing +
        ", enableHashedSpaceId=" + enableHashedSpaceId +
        ", compactHistory=" + compactHistory +
        ", onDemandIdxLimit=" + onDemandIdxLimit +
        ", dbConfig=" + dbConfig +
        ", spaceRole=" + spaceRole +
        ", spaceSchema=" + spaceSchema +
        ", adminRole=" + adminRole +
        ", adminSchema=" + adminSchema +
        '}';
  }
}