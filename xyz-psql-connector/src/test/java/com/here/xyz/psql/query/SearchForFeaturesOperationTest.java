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

package com.here.xyz.psql.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.SearchForFeaturesEvent;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Operations for which no valid SQL exists must be rejected as illegal arguments (400) instead of failing in the database (502).
 */
class SearchForFeaturesOperationTest {

  private static SearchForFeaturesEvent event(String query) {
    SearchForFeaturesEvent event = new SearchForFeaturesEvent();
    event.setSpace("test-space");
    event.setPropertiesQuery(PropertiesQuery.fromString(query, "", false));
    return event;
  }

  @ParameterizedTest
  @ValueSource(strings = {"f.id=cs=f1", "f.id@>f1", "f.geometry.type=cs=Point", "f.id=bw=f1", "f.id^=f1", "p.name=bw=n1",
      "p.name^=n1"})
  void unsupportedOperationsAreIllegalArguments(String query) {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> SearchForFeatures.generatePropertiesQuery(event(query)));
    assertTrue(e.getMessage().contains("is not supported"), e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"f.id=f1", "f.id!=f1", "f.id=f1,f2", "p.name=n1", "p.name=cs=n1", "p.value>=5", "f.geometry.type=Point"})
  void supportedOperationsStillGenerateSql(String query) {
    String sql = SearchForFeatures.generatePropertiesQuery(event(query)).substitute().text();
    assertTrue(sql.contains(" = ") || sql.contains(" != ") || sql.contains(" <> ") || sql.contains(" >= ") || sql.contains(" @> "), sql);
  }

  /**
   * For null values the operation is ignored and only an (is not) null check is generated, so these keep working.
   */
  @ParameterizedTest
  @ValueSource(strings = {"p.name=bw=.null", "p.name=cs=.null", "f.id=cs=.null", "f.id=bw=.null", "f.geometry.type=cs=.null"})
  void nullValuesIgnoreTheOperation(String query) {
    String sql = SearchForFeatures.generatePropertiesQuery(event(query)).substitute().text();
    assertTrue(sql.contains("IS NOT NULL"), sql);
  }
}
