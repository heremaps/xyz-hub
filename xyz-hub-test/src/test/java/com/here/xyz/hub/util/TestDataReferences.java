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

import com.here.xyz.models.hub.DataReference;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

final class TestDataReferences {

  private TestDataReferences() {
    // utility class
  }

  static DataReference dataReference(UUID referenceId) {
    return new DataReference()
      .withId(referenceId)
      .withEntityId("entityId-A")
      .withPatch(true)
      .withStartVersion(1)
      .withEndVersion(5)
      .withObjectType("object-type-A")
      .withContentType("content-type-A")
      .withLocation("location-A")
      .withSourceSystem("source-system-A")
      .withTargetSystem("target-system-A");
  }

  static Map<String, Object> dataReferenceAsMap(UUID referenceId) {
    Map<String, Object> dataReference = new HashMap<>();
    dataReference.put("id", referenceId.toString());
    dataReference.put("entityId", "entityId-A");
    dataReference.put("isPatch", true);
    dataReference.put("startVersion", new BigDecimal(1));
    dataReference.put("endVersion", new BigDecimal(5));
    dataReference.put("objectType", "object-type-A");
    dataReference.put("contentType", "content-type-A");
    dataReference.put("location", "location-A");
    dataReference.put("sourceSystem", "source-system-A");
    dataReference.put("targetSystem", "target-system-A");
    dataReference.put("customSortKey", "endVersion#000005#id#" + referenceId);

    return dataReference;
  }

}
