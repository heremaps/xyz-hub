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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergConnectorConfigTest {

  @Test
  void usesTheDefaults() {
    IcebergConnectorConfig config = IcebergConnectorConfig.from(Map.of(), name -> null);
    assertThat(config.catalogName()).isEqualTo("iceberg");
    assertThat(config.catalogProperties()).isEqualTo(Map.of("s3.retry.num-retries", "3", "s3.retry.min-wait-ms", "50",
        "s3.retry.max-wait-ms", "400"));
    assertThat(config.maxBytesPerRequest()).isEqualTo(268_435_456L);
    assertThat(config.maxResponseBytes()).isEqualTo(104_857_600L);
    assertThat(config.requestTimeoutMs()).isEqualTo(9_000);
    assertThat(config.scanCacheHeapShare()).isEqualTo(0.05);
    assertThat(config.footerCacheHeapShare()).isEqualTo(0.02);
    assertThat(config.tableCacheTtlSeconds()).isEqualTo(60);
    assertThat(config.planningThreads()).isEqualTo(2);
    assertThat(config.missingCacheTtlSeconds()).isEqualTo(30);
    assertThat(config.filterColumns()).isEqualTo(Map.of("momType", "mom_type"));
    assertThat(config.twoStepReads()).isTrue();
    assertThat(config.rangeReadThreads()).isEqualTo(32);
    assertThat(config.pageIndexCacheHeapShare()).isEqualTo(0.05);
  }

  @Test
  void explicitRetrySettingsWin() {
    IcebergConnectorConfig config = IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("type", "hadoop",
        "s3.retry.num-retries", 7)), name -> null);
    assertThat(config.catalogProperties()).containsEntry("type", "hadoop").containsEntry("s3.retry.num-retries", "7")
        .containsEntry("s3.retry.min-wait-ms", "50").containsEntry("s3.retry.max-wait-ms", "400");
  }

  @Test
  void rejectsInvalidRetrySettings() {
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("s3.retry.num-retries", "-1")),
        name -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("s3.retry.num-retries must be a non-negative integer");
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("s3.retry.min-wait-ms", "soon")),
        name -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("s3.retry.min-wait-ms must be a non-negative integer");
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("s3.retry.max-wait-ms", "1.5")),
        name -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("s3.retry.max-wait-ms must be a non-negative integer");
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("s3.retry.min-wait-ms", "500")),
        name -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("s3.retry.min-wait-ms must not be greater than s3.retry.max-wait-ms");
  }

  @Test
  void theRetrySettingsArePartOfTheCatalogKey() {
    IcebergConnectorConfig a = IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("type", "hadoop")), n -> null);
    IcebergConnectorConfig b = IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("type", "hadoop",
        "s3.retry.max-wait-ms", "800")), n -> null);
    assertThat(a.catalogKey()).isNotEqualTo(b.catalogKey());
    assertThat(a.catalogKey().catalogProperties()).containsEntry("s3.retry.max-wait-ms", "400");
  }

  @Test
  void readsTheTwoStepReadParams() {
    IcebergConnectorConfig config = IcebergConnectorConfig.from(Map.of("twoStepReads", false, "rangeReadThreads", "8",
        "pageIndexCacheHeapShare", 0.1), name -> null);
    assertThat(config.twoStepReads()).isFalse();
    assertThat(config.rangeReadThreads()).isEqualTo(8);
    assertThat(config.pageIndexCacheHeapShare()).isEqualTo(0.1);
    assertThat(IcebergConnectorConfig.from(Map.of("twoStepReads", "TRUE"), name -> null).twoStepReads()).isTrue();
    assertThat(IcebergConnectorConfig.from(Map.of("twoStepReads", "false"), name -> null).twoStepReads()).isFalse();
    assertThat(config.toString()).contains("twoStepReads=false", "rangeReadThreads=8", "pageIndexCacheHeapShare=0.1");
  }

  @Test
  void rejectsInvalidTwoStepReadParams() {
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("twoStepReads", "yes"), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("rangeReadThreads", 0), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("rangeReadThreads", 1L << 32), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("pageIndexCacheHeapShare", 0), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("pageIndexCacheHeapShare", 1), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theRangeReadResourcesArePartOfTheCatalogKey() {
    Map<String, Object> catalog = Map.of("type", "hadoop", "warehouse", "file:/tmp/w");
    IcebergConnectorConfig a = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog), n -> null);
    IcebergConnectorConfig b = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog, "rangeReadThreads", 4), n -> null);
    IcebergConnectorConfig c = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog, "pageIndexCacheHeapShare", 0.2),
        n -> null);
    IcebergConnectorConfig d = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog, "twoStepReads", false), n -> null);
    assertThat(a.catalogKey()).isNotEqualTo(b.catalogKey()).isNotEqualTo(c.catalogKey()).isEqualTo(d.catalogKey());
  }

  @Test
  void readsTheFilterColumns() {
    Map<String, Object> params = Map.of("filterColumns", Map.of("kind", "kind_col", "momType", "mom_type"));
    assertThat(IcebergConnectorConfig.from(params, name -> null).filterColumns())
        .isEqualTo(Map.of("kind", "kind_col", "momType", "mom_type"));
    assertThat(IcebergConnectorConfig.filterColumns(params)).containsOnlyKeys("kind", "momType");
    assertThat(IcebergConnectorConfig.filterColumns(Map.of("filterColumns", Map.of()))).isEmpty();
    assertThat(IcebergConnectorConfig.filterColumns(null)).isEqualTo(IcebergConnectorConfig.DEFAULT_FILTER_COLUMNS);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("filterColumns", "momType"), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("filterColumns", Map.of("kind", "nested.col")), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsTheParams() {
    IcebergConnectorConfig config = IcebergConnectorConfig.from(Map.of(
        "catalogName", "exports",
        "catalogProperties", Map.of("type", "hadoop", "s3.path-style-access", true),
        "maxBytesPerRequest", 1000,
        "maxResponseBytes", "2000",
        "requestTimeoutMs", 0,
        "scanCacheHeapShare", 0.1,
        "planningThreads", 4), name -> null);
    assertThat(config.catalogName()).isEqualTo("exports");
    assertThat(config.catalogProperties()).containsEntry("type", "hadoop").containsEntry("s3.path-style-access", "true");
    assertThat(config.maxBytesPerRequest()).isEqualTo(1000);
    assertThat(config.maxResponseBytes()).isEqualTo(2000);
    assertThat(config.requestTimeoutMs()).isZero();
    assertThat(config.scanCacheHeapShare()).isEqualTo(0.1);
    assertThat(config.planningThreads()).isEqualTo(4);
  }

  @Test
  void resolvesPlaceholders() {
    Map<String, String> env = Map.of("DB_HOST", "localhost", "DB_PORT", "5432", "DB_PASSWORD", "secret");
    IcebergConnectorConfig config = IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of(
        "uri", "jdbc:postgresql://${DB_HOST}:${DB_PORT}/db",
        "jdbc.password", "${DB_PASSWORD}")), env::get);
    assertThat(config.catalogProperties())
        .containsEntry("uri", "jdbc:postgresql://localhost:5432/db")
        .containsEntry("jdbc.password", "secret");
    assertThat(config.toString()).doesNotContain("secret").doesNotContain("localhost");
  }

  @Test
  void failsOnAnUnresolvedPlaceholder() {
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("jdbc.password", "${MISSING}")),
        name -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("${MISSING}");
  }

  @Test
  void rejectsTheLegacyMd5Plugin() {
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("catalogProperties",
        Map.of("client.legacy-md5-plugin-enabled", "true")), name -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("client.legacy-md5-plugin-enabled");
    assertThat(IcebergConnectorConfig.from(Map.of("catalogProperties", Map.of("client.legacy-md5-plugin-enabled", "false")),
        name -> null).catalogProperties()).containsEntry("client.legacy-md5-plugin-enabled", "false");
  }

  @Test
  void rejectsInvalidLimits() {
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("maxBytesPerRequest", 0), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("scanCacheHeapShare", 1.5), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IcebergConnectorConfig.from(Map.of("requestTimeoutMs", "abc"), name -> null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void limitsDoNotChangeTheCatalogKey() {
    Map<String, Object> catalog = Map.of("type", "hadoop", "warehouse", "file:/tmp/w");
    IcebergConnectorConfig a = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog, "maxBytesPerRequest", 1), n -> null);
    IcebergConnectorConfig b = IcebergConnectorConfig.from(Map.of("catalogProperties", catalog, "requestTimeoutMs", 5), n -> null);
    assertThat(a.catalogKey()).isEqualTo(b.catalogKey());
  }
}
