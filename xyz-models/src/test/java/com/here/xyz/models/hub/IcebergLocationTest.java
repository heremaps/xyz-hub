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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IcebergLocationTest {

  @Test
  void parsesAValidLocation() {
    IcebergLocation location = IcebergLocation.parse("iceberg://exports/space_a?ref=ext_space_a_v5");
    assertEquals("exports", location.namespace());
    assertEquals("space_a", location.table());
    assertEquals("ext_space_a_v5", location.ref());
  }

  @Test
  void decodesUrlEncodedParts() {
    IcebergLocation location = IcebergLocation.parse("iceberg://a.b/t%20x?other=1&ref=tag%2F1");
    assertEquals("a.b", location.namespace());
    assertEquals("t x", location.table());
    assertEquals("tag/1", location.ref());
  }

  @Test
  void roundTrips() {
    IcebergLocation location = new IcebergLocation("exports.sub", "my table", "ext_t_b2_v5");
    assertEquals(location, IcebergLocation.parse(location.toString()));
    assertEquals("iceberg://exports/space_a?ref=v1", new IcebergLocation("exports", "space_a", "v1").toString());
  }

  @Test
  void rejectsAMissingRef() {
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("iceberg://exports/space_a"));
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("iceberg://exports/space_a?ref="));
  }

  @Test
  void rejectsAWrongScheme() {
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("s3://bucket/space_a?ref=v1"));
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("exports/space_a?ref=v1"));
  }

  @Test
  void rejectsAnEmptyTable() {
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("iceberg://exports/?ref=v1"));
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("iceberg://exports?ref=v1"));
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse("iceberg:///space_a?ref=v1"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> IcebergLocation.parse(null));
  }
}
