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

package com.here.xyz.models.hub;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SpaceCompositeParamsTest {

  @Test
  void includesThePhysicalTableNameOfTheExtendedSpace() {
    Space composite = space("composite", "composite_table")
        .withExtension(new Extension().withSpaceId("base"));
    Space base = space("base", "base_table");

    Map<String, Object> extensionParams = extensionParams(composite.resolveCompositeParams(base));

    assertEquals("base", extensionParams.get("spaceId"));
    assertEquals("base_table", extensionParams.get(TABLE_NAME));
  }

  @Test
  void omitsThePhysicalTableNameForLegacySpaces() {
    Space composite = space("composite", "composite_table")
        .withExtension(new Extension().withSpaceId("legacy-base"));
    Space legacyBase = space("legacy-base", null);

    Map<String, Object> extensionParams = extensionParams(composite.resolveCompositeParams(legacyBase));

    assertFalse(extensionParams.containsKey(TABLE_NAME));
  }

  @Test
  void includesPhysicalTableNamesForTwoExtensionLevels() {
    Space composite = space("composite", "composite_table")
        .withExtension(new Extension().withSpaceId("intermediate"));
    Space intermediate = space("intermediate", "intermediate_table")
        .withExtension(new Extension().withSpaceId("base"));
    Space base = space("base", "base_table");
    intermediate.getExtension().resolvedSpace = base;

    Map<String, Object> intermediateParams = extensionParams(composite.resolveCompositeParams(intermediate));
    Map<String, Object> baseParams = extensionParams(intermediateParams);

    assertEquals("intermediate_table", intermediateParams.get(TABLE_NAME));
    assertEquals("base", baseParams.get("spaceId"));
    assertEquals("base_table", baseParams.get(TABLE_NAME));
  }

  private static Space space(String id, String tableName) {
    Map<String, Object> params = tableName == null ? null : Map.of(TABLE_NAME, tableName);
    return new Space().withId(id).withStorage(new ConnectorRef().withId("psql").withParams(params));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extensionParams(Map<String, Object> params) {
    return (Map<String, Object>) params.get("extends");
  }
}
