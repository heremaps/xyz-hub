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

package com.here.xyz.util.db;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorParameters {
  private static final Logger logger = LogManager.getLogger();
  private static final int MAX_WARNED_INVALID_PARAMS = 100;
  private static final int DEFAULT_LAZY_MATERIALIZATION_MAX_BUDGET = 25_000;
  private static final int MIN_LAZY_MATERIALIZATION_MAX_BUDGET = 3_000;
  private static final int MIN_SCRIPT_INSTALL_TIMEOUT_SECONDS = 5;
  private static final int MAX_SCRIPT_INSTALL_TIMEOUT_SECONDS = 25;
  /**
   * The invalid (name, value) pairs that were logged already. The parsed parameters are cached by their map only weakly,
   * so the setters run again for equal maps of later events; this set keeps the WARN to once per container and pair.
   */
  private static final Set<String> warnedInvalidParams = ConcurrentHashMap.newKeySet();
  private String connectorId;
  private String ecps;
  private static Map<Map<String, Object>, ConnectorParameters> cache = Collections.synchronizedMap(new WeakHashMap<>());
  private boolean propertySearch = false;
  private boolean mvtSupport = false;
  private boolean autoIndexing = false;
  private boolean enableHashedSpaceId = false;
  private int onDemandIdxLimit = 4;
  private boolean readOnly;
  /**
   * The maximum number of concurrent connections the connector may open to its database. A value
   * &lt;= 0 means unset, in which case the deprecated PSQL_MAX_CONN ECPS setting or the
   * {@link com.here.xyz.util.db.datasource.DatabaseSettings} default applies. Only relevant outside
   * Lambda, where a container serves one invocation at a time and never needs more than one.
   */
  private int dbMaxPoolSize;
  /**
   * Whether new spaces of this connector are created without a table, which the first write creates.
   * Only a boolean {@code true} or the string {@code "true"} (any case) switches it on.
   */
  private boolean lazyTableCreation;
  /**
   * The upper bound in milliseconds of the time a lazy table materialization may take, 3000 to 25000.
   */
  private int lazyMaterializationMaxBudget = DEFAULT_LAZY_MATERIALIZATION_MAX_BUDGET;
  /**
   * Whether the existence check of lazy tables runs on the read replica for reads that use the replica.
   */
  private boolean lazyCheckOnReplica;
  /**
   * The timeout in seconds of the database script check, 5 to 25. 0 means unset, in which case the
   * {@link com.here.xyz.util.db.datasource.DatabaseSettings} default applies.
   */
  private int scriptInstallTimeoutSeconds;

  private TableLayout tableLayout;
  public enum TableLayout {
    OLD_LAYOUT, NEW_LAYOUT;
  }

  public ConnectorParameters() {}

  public String getConnectorId() {
    return connectorId;
  }

  public String getEcps() {
    return ecps;
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

  public int getOnDemandIdxLimit() {
    return onDemandIdxLimit;
  }

  public int getDbMaxPoolSize() {
    return dbMaxPoolSize;
  }

  public void setDbMaxPoolSize(int dbMaxPoolSize) {
    this.dbMaxPoolSize = dbMaxPoolSize;
  }

  public ConnectorParameters withDbMaxPoolSize(int dbMaxPoolSize) {
    setDbMaxPoolSize(dbMaxPoolSize);
    return this;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public ConnectorParameters withReadOnly(boolean readOnly) {
    setReadOnly(readOnly);
    return this;
  }

  public boolean isLazyTableCreation() {
    return lazyTableCreation;
  }

  //Lenient: an invalid value must never make the parsing of all connector parameters fail
  @JsonSetter("lazyTableCreation")
  void setLazyTableCreation(Object value) {
    lazyTableCreation = parseFlag("lazyTableCreation", value);
  }

  public int getLazyMaterializationMaxBudget() {
    return lazyMaterializationMaxBudget;
  }

  @JsonSetter("lazyMaterializationMaxBudget")
  void setLazyMaterializationMaxBudget(Object value) {
    int ms = parseIntParam(value);
    if (ms < 0)
      warnInvalid("lazyMaterializationMaxBudget", value, String.valueOf(DEFAULT_LAZY_MATERIALIZATION_MAX_BUDGET));
    lazyMaterializationMaxBudget = ms < 0 ? DEFAULT_LAZY_MATERIALIZATION_MAX_BUDGET
        : Math.max(MIN_LAZY_MATERIALIZATION_MAX_BUDGET, Math.min(DEFAULT_LAZY_MATERIALIZATION_MAX_BUDGET, ms));
  }

  public boolean isLazyCheckOnReplica() {
    return lazyCheckOnReplica;
  }

  @JsonSetter("lazyCheckOnReplica")
  void setLazyCheckOnReplica(Object value) {
    lazyCheckOnReplica = parseFlag("lazyCheckOnReplica", value);
  }

  public int getScriptInstallTimeoutSeconds() {
    return scriptInstallTimeoutSeconds;
  }

  //0 keeps the default of DatabaseSettings (15 s)
  @JsonSetter("scriptInstallTimeoutSeconds")
  void setScriptInstallTimeoutSeconds(Object value) {
    int seconds = parseIntParam(value);
    if (seconds < 0)
      warnInvalid("scriptInstallTimeoutSeconds", value, "15");
    scriptInstallTimeoutSeconds = seconds < 0 ? 0
        : Math.max(MIN_SCRIPT_INSTALL_TIMEOUT_SECONDS, Math.min(MAX_SCRIPT_INSTALL_TIMEOUT_SECONDS, seconds));
  }

  private static boolean parseFlag(String name, Object value) {
    boolean on = Boolean.TRUE.equals(value) || value instanceof String s && s.equalsIgnoreCase("true");
    if (!on && value != null && !Boolean.FALSE.equals(value) && !"false".equalsIgnoreCase(String.valueOf(value)))
      warnInvalid(name, value, "false");
    return on;
  }

  /**
   * @return The value of a JSON number (a fraction is truncated) or of a string of 1 to 9 digits, a negative value for
   * anything else
   */
  private static int parseIntParam(Object value) {
    return value instanceof Number n ? n.intValue()
        : value instanceof String s && s.matches("\\d{1,9}") ? Integer.parseInt(s) : -1;
  }

  //Once per container and invalid (name, value) pair; at most MAX_WARNED_INVALID_PARAMS pairs are remembered
  private static void warnInvalid(String name, Object value, String fallback) {
    String pair = name + "=" + value;
    if (warnedInvalidParams.size() < MAX_WARNED_INVALID_PARAMS && warnedInvalidParams.add(pair))
      logger.warn("Invalid connector param {}, using {}", pair, fallback);
  }

  public TableLayout getTableLayout() { return  tableLayout; }

  public  void setTableLayout(TableLayout tableLayout) { this.tableLayout = tableLayout; }

  @Override
  public String toString() {
    return "ConnectorParameters{" +
            "propertySearch=" + propertySearch +
            ", mvtSupport=" + mvtSupport +
            ", autoIndexing=" + autoIndexing +
            ", enableHashedSpaceId=" + enableHashedSpaceId +
            ", onDemandIdxLimit=" + onDemandIdxLimit +
            ", dbMaxPoolSize=" + dbMaxPoolSize +
            ", ecps='" + ecps + '\'' +
            '}';
  }

  public static ConnectorParameters fromEvent(Event event) {
    final Map<String, Object> connectorParams = event.getConnectorParams();
    return fromMap(connectorParams);
  }

  public static ConnectorParameters fromMap(Map<String, Object> connectorParamsMap) {
    ConnectorParameters params = cache.get(connectorParamsMap);
    if (params == null) {
      params = connectorParamsMap != null ? XyzSerializable.fromMap(connectorParamsMap, ConnectorParameters.class) : new ConnectorParameters();
      cache.put(connectorParamsMap, params);
    }
    return params;
  }
}