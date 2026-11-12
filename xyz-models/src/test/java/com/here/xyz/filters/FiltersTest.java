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
package com.here.xyz.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import org.junit.jupiter.api.Test;

public class FiltersTest {

  @Test
  public void refIsRejectedInsteadOfSilentlyIgnored() {
    JsonProcessingException e = assertThrows(JsonProcessingException.class,
        () -> XyzSerializable.deserialize("{\"ref\":\"2\"}", Filters.class));

    assertTrue(e.getMessage().contains("versionRef"), "Unexpected message: " + e.getMessage());
  }

  @Test
  public void refIsRejectedNextToValidFilters() {
    assertThrows(JsonProcessingException.class,
        () -> XyzSerializable.deserialize("{\"propertyFilter\":\"p.test<10\",\"ref\":\"2\"}", Filters.class));
  }

  @Test
  public void validFiltersAreStillDeserialized() throws JsonProcessingException {
    Filters filters = XyzSerializable.deserialize("{\"propertyFilter\":\"p.test<10\",\"context\":\"EXTENSION\"}", Filters.class);

    assertFalse(filters.getPropertyFilter().isEmpty());
    assertEquals("EXTENSION", filters.getContext().name());
  }

  @Test
  public void otherUnknownPropertiesAreStillIgnored() throws JsonProcessingException {
    Filters filters = XyzSerializable.deserialize("{\"somethingElse\":1}", Filters.class);

    assertTrue(filters.isEmpty());
  }

  @Test
  public void serializedFiltersDoNotContainRef() {
    assertFalse(XyzSerializable.serialize(new Filters().withJsonPath("$.a")).contains("\"ref\""));
  }
}
