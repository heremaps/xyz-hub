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
    assertThat(config.catalogProperties()).isEmpty();
    assertThat(config.maxBytesPerRequest()).isEqualTo(268_435_456L);
    assertThat(config.maxResponseBytes()).isEqualTo(104_857_600L);
    assertThat(config.requestTimeoutMs()).isEqualTo(9_000);
    assertThat(config.scanCacheHeapShare()).isEqualTo(0.05);
    assertThat(config.footerCacheHeapShare()).isEqualTo(0.02);
    assertThat(config.tableCacheTtlSeconds()).isEqualTo(60);
    assertThat(config.planningThreads()).isEqualTo(2);
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
