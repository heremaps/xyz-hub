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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.util.StringHelper;
import org.junit.jupiter.api.Test;

public class StringHelperTest {

  @Test
  public void equals_test() {
    assertTrue(StringHelper.equals("a", "a"));
    assertTrue(StringHelper.equals("Hello", "Hello"));
    assertFalse(StringHelper.equals("Hello", "Hello World"));
    assertTrue(StringHelper.equals("Hello", 0, 5, "Hello World", 0, 5));
    assertTrue(StringHelper.equals("Hello", 1, 4, "Hello World", 1, 4));
    assertTrue(StringHelper.equals("Hello", 0, 0, "Hello World", 0, 0));
    assertFalse(StringHelper.equals("Hello", 0, 5, "Hello World", 1, 6));
    assertTrue(StringHelper.equals("ello ", 0, 5, "Hello World", 1, 6));
  }
}
