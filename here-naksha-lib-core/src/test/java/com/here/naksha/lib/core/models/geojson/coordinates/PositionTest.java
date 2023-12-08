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
package com.here.naksha.lib.core.models.geojson.coordinates;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PositionTest {

  @Test
  void shouldHaveTwoElemsForLonLat() {
    // Given:
    Position position = new Position(1, 2);

    // Then
    Assertions.assertEquals(2, position.size());
    Assertions.assertEquals(1, position.get(0));
    Assertions.assertEquals(2, position.get(1));
  }

  @Test
  void shouldHaveThreeElemsForLonLatAlt() {
    // Given:
    Position position = new Position(1, 2, 3);

    // Then
    Assertions.assertEquals(3, position.size());
    Assertions.assertEquals(1, position.get(0));
    Assertions.assertEquals(2, position.get(1));
    Assertions.assertEquals(3, position.get(2));
  }
}
