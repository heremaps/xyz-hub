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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class SimpleTaskTest {
  private static @NotNull String testFn(@NotNull String a) {
    return a + "_world";
  }

  @Test
  void testF1Call() throws ExecutionException, InterruptedException {
    final Future<String> future = new SimpleTask<String>().start(SimpleTaskTest::testFn, "hello");
    assertNotNull(future);
    final String result = future.get();
    assertNotNull(result);
    assertEquals("hello_world", result);
  }
}
