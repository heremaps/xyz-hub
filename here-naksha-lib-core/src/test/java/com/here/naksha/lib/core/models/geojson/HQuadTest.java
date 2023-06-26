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
package com.here.naksha.lib.core.models.geojson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import org.junit.jupiter.api.Test;

public class HQuadTest {

  BBox bbox = new BBox()
      .withEast(13.38134765625)
      .withNorth(52.53662109375)
      .withWest(13.359375)
      .withSouth(52.5146484375);
  String base4QK = "12201203120220";
  String base10QK = "377894440";

  @Test
  public void testBase4Quadkey() {
    HQuad hQuad = new HQuad(base4QK, true);

    assertEquals(bbox, hQuad.getBoundingBox());
    assertEquals(14, hQuad.level);
    assertEquals(8800, hQuad.x);
    assertEquals(6486, hQuad.y);
    assertEquals(base4QK, hQuad.quadkey);
  }

  @Test
  public void testBase10Quadkey() {
    HQuad hQuad = new HQuad(base10QK, false);

    assertEquals(bbox, hQuad.getBoundingBox());
    assertEquals(14, hQuad.level);
    assertEquals(8800, hQuad.x);
    assertEquals(6486, hQuad.y);
    assertEquals(base4QK, hQuad.quadkey);
  }

  @Test
  public void testLRC() {
    HQuad hQuad = new HQuad(8800, 6486, 14);

    assertEquals(bbox, hQuad.getBoundingBox());
    assertEquals(14, hQuad.level);
    assertEquals(8800, hQuad.x);
    assertEquals(6486, hQuad.y);
    assertEquals(base4QK, hQuad.quadkey);
  }

  @Test
  public void testInvalidBase4QK() {
    assertThrows(IllegalArgumentException.class, () -> new HQuad("5031", true));
  }

  @Test
  public void testInvalidBase10QK() {
    assertThrows(IllegalArgumentException.class, () -> new HQuad("12s", false));
  }

  @Test
  public void testInvalidLRC() {
    assertThrows(IllegalArgumentException.class, () -> new HQuad(10, 10, 1));
  }
}
