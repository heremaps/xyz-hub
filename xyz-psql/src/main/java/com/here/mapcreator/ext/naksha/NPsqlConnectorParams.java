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

package com.here.mapcreator.ext.naksha;

import com.here.xyz.XyzSerializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PostgresQL connector parameters.
 */
@SuppressWarnings("unused")
public class NPsqlConnectorParams {

  private static final Logger logger = LoggerFactory.getLogger(NPsqlConnectorParams.class);

  /**
   * Paramters
   */
  public final static String CONNECTOR_ID = "connectorId";
  public final static String PROPERTY_SEARCH = "propertySearch";
  public final static String MVT_SUPPORT = "mvtSupport";
  public final static String AUTO_INDEXING = "autoIndexing";
  public final static String ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
  public final static String COMPACT_HISTORY = "compactHistory";
  public final static String ON_DEMAND_IDX_LIMIT = "onDemandIdxLimit";
  public final static String HRN_SHORTENING = "hrnShortening";
  public final static String IGNORE_CREATE_MSE = "ignoreCreateMse";

  private final @NotNull String connectorId;
  private final long dbId;
  private final boolean propertySearch;
  private final boolean mvtSupport;
  private final boolean autoIndexing;
  private final boolean enableHashedSpaceId;
  private final boolean compactHistory;
  private final int onDemandIdxLimit;
  private final boolean hrnShortening;
  private final boolean ignoreCreateMse;

  private final @NotNull NPsqlPoolConfig dbConfig;
  private final @NotNull List<@NotNull NPsqlPoolConfig> dbReplicas;
  private final @NotNull String spaceRole;
  private final @NotNull String spaceSchema;
  private final @NotNull String adminRole;
  private final @NotNull String adminSchema;

  private final @NotNull String logId;

  private static final String THROW_NPE = null;

  @SuppressWarnings("unchecked")
  public NPsqlConnectorParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams, @NotNull String logId)
      throws NullPointerException {
    this.logId = logId;
    Object raw = connectorParams.get("dbConfig");
    if (!(raw instanceof Map)) {
      throw new IllegalArgumentException("dbConfig");
    }
    this.dbConfig = XyzSerializable.fromAnyMap((Map<String, Object>) raw, NPsqlPoolConfig.class);
    raw = connectorParams.get("dbReplicas");
    final ArrayList<@NotNull NPsqlPoolConfig> replicas;
    if (raw instanceof List) {
      final List<Object> rawList = (List<Object>) raw;
      final int SIZE = rawList.size();
      replicas = new ArrayList<>(SIZE);
      for (final Object o : rawList) {
        if (o instanceof Map) {
          replicas.add(XyzSerializable.fromAnyMap((Map<String, Object>) o, NPsqlPoolConfig.class));
        }
      }
    } else {
      replicas = new ArrayList<>();
    }
    this.dbReplicas = replicas;
    this.spaceRole = parseValue(connectorParams, String.class, dbConfig.user, "spaceRole");
    this.adminRole = parseValue(connectorParams, String.class, spaceRole, "adminRole");
    this.spaceSchema = parseValue(connectorParams, String.class, Naksha.SPACE_SCHEMA, "spaceSchema");
    this.adminSchema = parseValue(connectorParams, String.class, Naksha.ADMIN_SCHEMA, "adminSchema");
    this.connectorId = parseValue(connectorParams, String.class, THROW_NPE, CONNECTOR_ID);
    this.dbId = parseValue(connectorParams, Long.class, 0L, "dbId");
    if (dbId <= 0L) {
      logger.warn("{} - Missing or illegal database ID: {}", logId, connectorParams.get("dbId"));
    }
    this.autoIndexing = parseValue(connectorParams, Boolean.class, false, AUTO_INDEXING);
    this.propertySearch = parseValue(connectorParams, Boolean.class, false, PROPERTY_SEARCH);
    this.mvtSupport = parseValue(connectorParams, Boolean.class, false, MVT_SUPPORT);
    this.enableHashedSpaceId = parseValue(connectorParams, Boolean.class, false, ENABLE_HASHED_SPACEID);
    this.compactHistory = parseValue(connectorParams, Boolean.class, true, COMPACT_HISTORY);
    this.onDemandIdxLimit = parseValue(connectorParams, Integer.class, 4, ON_DEMAND_IDX_LIMIT);
    this.hrnShortening = parseValue(connectorParams, Boolean.class, false, HRN_SHORTENING);
    this.ignoreCreateMse = parseValue(connectorParams, Boolean.class, false, IGNORE_CREATE_MSE);
  }

  private <T> @NotNull T parseValue(
      @NotNull Map<@NotNull String, @Nullable Object> connectorParams,
      @NotNull Class<T> type,
      @Nullable T defaultValue,
      @NotNull String parameter
  ) throws NullPointerException {
    final Object value = connectorParams.get(parameter);
    if (value == null) {
      if (defaultValue == null) {
        throw new NullPointerException(parameter);
      }
      return defaultValue;
    }
    if (value.getClass() != type) {
      logger.warn("{} - Cannot set value {}={}. Load default '{}'", logId, parameter, value, defaultValue);
      if (defaultValue == null) {
        throw new NullPointerException(parameter);
      }
      return defaultValue;
    }
    return type.cast(value);
  }

  public @NotNull String getConnectorId() {
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
  public @NotNull NPsqlPoolConfig getDbConfig() {
    return dbConfig;
  }

  /**
   * Returns all configured replicas; the returned list may be empty.
   *
   * @return all configured replicas; the returned list may be empty.
   */
  public @NotNull List<@NotNull NPsqlPoolConfig> getDbReplicas() {
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