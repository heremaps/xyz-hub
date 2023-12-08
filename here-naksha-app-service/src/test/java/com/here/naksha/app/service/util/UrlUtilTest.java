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
package com.here.naksha.app.service.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UrlUtilTest {

  @Test
  void shouldVerifyQueryValueExistence() {
    // Given
    String url =
        "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd&schema=some_schema&query=x&query=y";
    String nonExistingKey = "foo";
    String keyForMultipleValues = "query";
    String keyForSingleValue = "schema";

    // Then
    Assertions.assertFalse(UrlUtil.containsSingleQueryValue(url, nonExistingKey));
    Assertions.assertFalse(UrlUtil.containsSingleQueryValue(url, keyForMultipleValues));
    Assertions.assertTrue(UrlUtil.containsSingleQueryValue(url, keyForSingleValue));
  }

  @Test
  void shouldReturnQueryParams() {
    // Given
    String url =
        "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd&schema=some_schema&query=x&query=y";

    // When
    Map<String, List<String>> queryParams = UrlUtil.queryParams(url);

    // Then
    assertMapsEquality(
        Map.of(
            "user", List.of("postgres"),
            "password", List.of("pswd"),
            "schema", List.of("some_schema"),
            "query", List.of("x", "y")),
        queryParams);
  }

  @Test
  void shouldReturnEmptyQueryParams() {
    // Given
    String url = "jdbc:postgresql://localhost:5432/postgres";

    // When
    Map<String, List<String>> queryParams = UrlUtil.queryParams(url);

    // Then
    assertTrue(queryParams.isEmpty());
  }

  @Test
  void shouldReturnOverriddenUrl() {
    // Given
    String url = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd";
    Map<String, List<String>> newParams = Map.of(
        "key_a", List.of("val_a_1", "val_a_2"),
        "key_b", List.of("val_b"));

    // When
    String newUrl = UrlUtil.urlWithOverriddenParams(url, newParams);

    // Then
    Assertions.assertEquals(newParams, UrlUtil.queryParams(newUrl));
  }

  private void assertMapsEquality(Map<String, List<String>> expected, Map<String, List<String>> actual) {
    expected.forEach((key, value) -> {
      assertEquals(actual.get(key), value);
    });
  }
}
