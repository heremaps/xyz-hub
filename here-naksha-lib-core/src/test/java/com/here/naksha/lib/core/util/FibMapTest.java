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
package com.here.naksha.lib.core.util;

import static com.here.naksha.lib.core.util.FibMap.ANY;
import static com.here.naksha.lib.core.util.FibMap.CONFLICT;
import static com.here.naksha.lib.core.util.FibMap.LPT_HASH_CODE;
import static com.here.naksha.lib.core.util.FibMap.LPT_HEADER_SIZE;
import static com.here.naksha.lib.core.util.FibMap.LPT_ID;
import static com.here.naksha.lib.core.util.FibMap.LPT_ID_OBJECT;
import static com.here.naksha.lib.core.util.FibMap.SEGMENT_bits;
import static com.here.naksha.lib.core.util.FibMap.UNDEFINED;
import static com.here.naksha.lib.core.util.FibMap.VOID;
import static com.here.naksha.lib.core.util.FibMap.capacityBitsOf;
import static com.here.naksha.lib.core.util.FibMap.count;
import static com.here.naksha.lib.core.util.FibMap.get;
import static com.here.naksha.lib.core.util.FibMap.newFibMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class FibMapTest {

  private static @NotNull Object intern(@NotNull Object key) {
    return key;
  }

  private static @NotNull Object conflict(
      @NotNull Object key, @Nullable Object expected_value, @Nullable Object new_value, @Nullable Object value) {
    return CONFLICT;
  }

  private static @Nullable Object put(
      @NotNull Object key,
      final @Nullable Object expected_value,
      final @Nullable Object new_value,
      final boolean create,
      final Object @NotNull [] array) {
    return FibMap.put(key, expected_value, new_value, create, array, FibMapTest::intern, FibMapTest::conflict);
  }

  private static class Collider {

    Collider(@NotNull String name) {
      this.name = name;
    }

    public @NotNull String name;

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    public @NotNull String toString() {
      return name;
    }
  }

  @Test
  void test_collision() {
    final var a = new Collider("a");
    final var b = new Collider("b");
    assertEquals(a, a);
    assertNotEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    final Object[] array = newFibMap();
    assertEquals(SEGMENT_bits, capacityBitsOf(array));
    Object result;

    result = put("foo", UNDEFINED, 1L, true, array);
    assertSame(UNDEFINED, result);
    assertEquals(1L, get("foo", array));
    assertEquals(1L, count(array));

    result = put("foo", VOID, UNDEFINED, false, array);
    assertSame(CONFLICT, result);

    result = put(a, UNDEFINED, 0L, true, array);
    assertEquals(UNDEFINED, result);
    assertEquals(0L, get(a, array));
    assertEquals(2L, count(array));

    result = put(b, UNDEFINED, 1L, true, array);
    assertEquals(UNDEFINED, result);
    assertEquals(1L, get(b, array));
    assertEquals(3L, count(array));

    // Ensure that we really have a collision.
    Object[] sub_array;
    sub_array = array; // depth 0
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 1
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 2
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 3
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 4
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 5
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 6
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 7
    sub_array = assertInstanceOf(Object[].class, sub_array[0]); // depth 8, this is linear probing!
    assertSame(LPT_ID_OBJECT, sub_array[LPT_ID]);
    assertSame(0, sub_array[LPT_HASH_CODE]);

    result = put(a, VOID, UNDEFINED, false, array);
    assertSame(CONFLICT, result);
    result = put(a, ANY, UNDEFINED, false, array);
    assertEquals(0L, result);
    result = put(b, ANY, UNDEFINED, false, array);
    assertEquals(1L, result);
    assertEquals(1L, count(array));

    for (int i = LPT_HEADER_SIZE; i < sub_array.length; i++) {
      assertNull(sub_array[i]);
    }

    result = put("foo", ANY, UNDEFINED, false, array);
    assertEquals(1L, result);
    assertEquals(0L, count(array));
  }

  @Test
  void basic_createUpdateDelete() {
    final Object[] array = newFibMap();
    assertEquals(SEGMENT_bits, capacityBitsOf(array));
    Object result;

    result = put("foo", UNDEFINED, 1L, false, array);
    assertSame(CONFLICT, result);
    assertEquals(0L, count(array));

    result = get("foo", array);
    assertEquals(UNDEFINED, result);

    result = put("foo", UNDEFINED, 1L, true, array);
    assertSame(UNDEFINED, result);
    assertEquals(1L, count(array));

    result = get("foo", array);
    assertEquals(1L, result);

    result = put("foo", 1L, 2L, false, array);
    assertEquals(1L, result);
    assertEquals(1L, count(array));
    assertEquals(2L, get("foo", array));

    result = put("foo", 2L, UNDEFINED, false, array);
    assertEquals(2L, result);
    assertEquals(0L, count(array));
    assertEquals(UNDEFINED, get("foo", array));
  }
}
