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
package com.here.naksha.lib.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NakshaVersionTest {

  @Test
  void test_basics() {
    NakshaVersion v = NakshaVersion.of(NakshaVersion.v2_0_3);
    assertNotNull(v);
    assertEquals(2, v.major());
    assertEquals(0, v.minor());
    assertEquals(3, v.revision());

    v = new NakshaVersion(1, 2, 3);
    assertEquals(1, v.major());
    assertEquals(2, v.minor());
    assertEquals(3, v.revision());
    assertEquals("1.2.3", v.toString());
  }
}
