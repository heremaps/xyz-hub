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

package com.here.xyz.iceberg;

import com.here.xyz.util.runtime.FunctionRuntime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The configuration of the Iceberg connector, taken from the connector params ({@code event.connectorParams}).
 *
 * @param catalogName The name of the Iceberg catalog
 * @param catalogProperties The Iceberg catalog properties, passed to {@code CatalogUtil.buildIcebergCatalog} with the
 *     {@code ${NAME}} placeholders resolved from the function runtime's environment. They may contain secrets, so they are never
 *     logged.
 * @param maxBytesPerRequest The maximum number of bytes one request may read from storage
 * @param maxResponseBytes The maximum size of the features of one response
 * @param requestTimeoutMs The time after which a request stops with a TIMEOUT error
 * @param scanCacheHeapShare The share of the max heap for the cache of planned scans
 * @param footerCacheHeapShare The share of the max heap for the cache of Parquet footers
 * @param tableCacheTtlSeconds How long loaded table metadata is reused
 * @param missingCacheTtlSeconds How long a table or ref which does not exist is remembered as missing, so that a stale location
 *     does not load the table from the catalog on every request
 * @param planningThreads The number of threads for planning the files of a snapshot
 * @param filterColumns The root properties of the features (property query keys, e.g. {@code momType} for {@code f.momType})
 *     which can be filtered by equality, mapped to the top-level string columns of the table which hold their values. Reads with
 *     other property queries are answered with NOT_IMPLEMENTED. Default: {@link #DEFAULT_FILTER_COLUMNS}.
 */
public record IcebergConnectorConfig(String catalogName, Map<String, String> catalogProperties, long maxBytesPerRequest,
    long maxResponseBytes, long requestTimeoutMs, double scanCacheHeapShare, double footerCacheHeapShare, long tableCacheTtlSeconds,
    long missingCacheTtlSeconds, int planningThreads, Map<String, String> filterColumns) {

  static final String DEFAULT_CATALOG_NAME = "iceberg";
  static final long DEFAULT_MAX_BYTES_PER_REQUEST = 256L * 1024 * 1024;
  static final long DEFAULT_MAX_RESPONSE_BYTES = 100L * 1024 * 1024;
  static final long DEFAULT_REQUEST_TIMEOUT_MS = 9_000;
  static final double DEFAULT_SCAN_CACHE_HEAP_SHARE = 0.05;
  static final double DEFAULT_FOOTER_CACHE_HEAP_SHARE = 0.02;
  static final long DEFAULT_TABLE_CACHE_TTL_SECONDS = 60;
  static final long DEFAULT_MISSING_CACHE_TTL_SECONDS = 30;
  static final int DEFAULT_PLANNING_THREADS = 2;
  public static final Map<String, String> DEFAULT_FILTER_COLUMNS = Map.of("momType", "mom_type");
  static final String FILTER_COLUMNS_PARAM = "filterColumns";
  /**
   * Iceberg's LegacyMd5Plugin is missing in the AWS SDK version of the Hub.
   */
  static final String LEGACY_MD5_PLUGIN_PROPERTY = "client.legacy-md5-plugin-enabled";
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_.-]+)}");

  public IcebergConnectorConfig {
    catalogProperties = catalogProperties == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(catalogProperties));
    filterColumns = filterColumns == null ? DEFAULT_FILTER_COLUMNS : Collections.unmodifiableMap(new TreeMap<>(filterColumns));
    filterColumns.forEach((property, column) -> {
      if (property.isEmpty() || column == null || column.isEmpty() || column.contains("."))
        throw new IllegalArgumentException(FILTER_COLUMNS_PARAM + " must map property names to top-level column names");
    });
    if (catalogName == null || catalogName.isEmpty())
      throw new IllegalArgumentException("catalogName must not be empty");
    requirePositive(maxBytesPerRequest, "maxBytesPerRequest");
    requirePositive(maxResponseBytes, "maxResponseBytes");
    if (requestTimeoutMs < 0)
      throw new IllegalArgumentException("requestTimeoutMs must not be negative");
    requireShare(scanCacheHeapShare, "scanCacheHeapShare");
    requireShare(footerCacheHeapShare, "footerCacheHeapShare");
    requirePositive(tableCacheTtlSeconds, "tableCacheTtlSeconds");
    requirePositive(missingCacheTtlSeconds, "missingCacheTtlSeconds");
    requirePositive(planningThreads, "planningThreads");
    if ("true".equalsIgnoreCase(catalogProperties.get(LEGACY_MD5_PLUGIN_PROPERTY)))
      throw new IllegalArgumentException(LEGACY_MD5_PLUGIN_PROPERTY + "=true is not supported by the AWS SDK version of the Hub");
  }

  /**
   * Reads the configuration from the connector params, resolving placeholders from the current {@link FunctionRuntime}.
   */
  public static IcebergConnectorConfig from(Map<String, Object> connectorParams) {
    return from(connectorParams, IcebergConnectorConfig::environmentVariable);
  }

  static IcebergConnectorConfig from(Map<String, Object> connectorParams, Function<String, String> env) {
    Map<String, Object> params = connectorParams == null ? Map.of() : connectorParams;
    return new IcebergConnectorConfig(
        params.get("catalogName") == null ? DEFAULT_CATALOG_NAME : params.get("catalogName").toString(),
        catalogProperties(params.get("catalogProperties"), env),
        longParam(params, "maxBytesPerRequest", DEFAULT_MAX_BYTES_PER_REQUEST),
        longParam(params, "maxResponseBytes", DEFAULT_MAX_RESPONSE_BYTES),
        longParam(params, "requestTimeoutMs", DEFAULT_REQUEST_TIMEOUT_MS),
        doubleParam(params, "scanCacheHeapShare", DEFAULT_SCAN_CACHE_HEAP_SHARE),
        doubleParam(params, "footerCacheHeapShare", DEFAULT_FOOTER_CACHE_HEAP_SHARE),
        longParam(params, "tableCacheTtlSeconds", DEFAULT_TABLE_CACHE_TTL_SECONDS),
        longParam(params, "missingCacheTtlSeconds", DEFAULT_MISSING_CACHE_TTL_SECONDS),
        (int) longParam(params, "planningThreads", DEFAULT_PLANNING_THREADS),
        filterColumns(params));
  }

  /**
   * Reads the filter columns from the connector params, see {@link #filterColumns()}.
   *
   * @throws IllegalArgumentException if the param is not an object of strings
   */
  public static Map<String, String> filterColumns(Map<String, Object> connectorParams) {
    Object raw = connectorParams == null ? null : connectorParams.get(FILTER_COLUMNS_PARAM);
    if (raw == null)
      return DEFAULT_FILTER_COLUMNS;
    if (!(raw instanceof Map<?, ?> map))
      throw new IllegalArgumentException(FILTER_COLUMNS_PARAM + " must be an object");
    Map<String, String> columns = new TreeMap<>();
    map.forEach((property, column) -> {
      if (!(column instanceof String columnName))
        throw new IllegalArgumentException(FILTER_COLUMNS_PARAM + " must map property names to column names");
      columns.put(property.toString(), columnName);
    });
    return columns;
  }

  /**
   * The part of the configuration that identifies a catalog and its caches. Requests with different limits share it.
   */
  CatalogKey catalogKey() {
    return new CatalogKey(catalogName, catalogProperties, scanCacheHeapShare, footerCacheHeapShare, tableCacheTtlSeconds,
        missingCacheTtlSeconds, planningThreads, filterColumns.values().stream().distinct().sorted().toList());
  }

  @Override
  public String toString() {
    //Never print the catalog property values, they may contain resolved secrets
    return "IcebergConnectorConfig[catalogName=" + catalogName + ", catalogProperties=" + catalogProperties.keySet()
        + ", maxBytesPerRequest=" + maxBytesPerRequest + ", maxResponseBytes=" + maxResponseBytes + ", requestTimeoutMs="
        + requestTimeoutMs + ", scanCacheHeapShare=" + scanCacheHeapShare + ", footerCacheHeapShare=" + footerCacheHeapShare
        + ", tableCacheTtlSeconds=" + tableCacheTtlSeconds + ", missingCacheTtlSeconds=" + missingCacheTtlSeconds
        + ", planningThreads=" + planningThreads + ", filterColumns=" + filterColumns + "]";
  }

  record CatalogKey(String catalogName, Map<String, String> catalogProperties, double scanCacheHeapShare,
      double footerCacheHeapShare, long tableCacheTtlSeconds, long missingCacheTtlSeconds, int planningThreads,
      List<String> statsColumns) {

    @Override
    public String toString() {
      return "CatalogKey[catalogName=" + catalogName + ", catalogProperties=" + catalogProperties.keySet() + "]";
    }
  }

  private static Map<String, String> catalogProperties(Object raw, Function<String, String> env) {
    if (raw == null)
      return Map.of();
    if (!(raw instanceof Map<?, ?> map))
      throw new IllegalArgumentException("catalogProperties must be an object");
    Map<String, String> resolved = new TreeMap<>();
    map.forEach((key, value) -> {
      if (value != null)
        resolved.put(key.toString(), resolvePlaceholders(key.toString(), value.toString(), env));
    });
    return resolved;
  }

  private static String resolvePlaceholders(String key, String value, Function<String, String> env) {
    Matcher matcher = PLACEHOLDER.matcher(value);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String name = matcher.group(1);
      String replacement = env.apply(name);
      if (replacement == null)
        throw new IllegalStateException("Unresolved placeholder ${" + name + "} in the catalog property " + key);
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String environmentVariable(String name) {
    FunctionRuntime runtime = FunctionRuntime.getInstance();
    return runtime != null ? runtime.getEnvironmentVariable(name) : System.getenv(name);
  }

  private static long longParam(Map<String, Object> params, String name, long defaultValue) {
    Object value = params.get(name);
    if (value == null)
      return defaultValue;
    try {
      return value instanceof Number number ? number.longValue() : Long.parseLong(value.toString());
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a number");
    }
  }

  private static double doubleParam(Map<String, Object> params, String name, double defaultValue) {
    Object value = params.get(name);
    if (value == null)
      return defaultValue;
    try {
      return value instanceof Number number ? number.doubleValue() : Double.parseDouble(value.toString());
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a number");
    }
  }

  private static void requirePositive(long value, String name) {
    if (value <= 0)
      throw new IllegalArgumentException(name + " must be positive");
  }

  private static void requireShare(double value, String name) {
    if (!(value > 0 && value < 1))
      throw new IllegalArgumentException(name + " must be between 0 and 1");
  }
}
