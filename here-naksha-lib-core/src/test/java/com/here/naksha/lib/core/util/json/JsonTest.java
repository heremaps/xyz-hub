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
package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.util.json.Json.toJsonString;
import static org.junit.jupiter.api.Assertions.*;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class JsonTest {

  static final StringBuilder sb = new StringBuilder();

  private static @NotNull StringBuilder empty() {
    sb.setLength(0);
    return sb;
  }

  @Test
  void test_toJsonString() {
    toJsonString("test", empty());
    assertEquals("\"test\"", sb.toString());

    toJsonString("\u0000\t\r\nHello World", empty());
    assertEquals("\"\\u0000\\t\\r\\nHello World\"", sb.toString());
  }
}
