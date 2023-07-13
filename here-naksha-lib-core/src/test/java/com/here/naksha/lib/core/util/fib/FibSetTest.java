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
package com.here.naksha.lib.core.util.fib;

import static com.here.naksha.lib.core.util.fib.FibSet.CAPACITY_bits;
import static com.here.naksha.lib.core.util.fib.FibSet.EMPTY;
import static com.here.naksha.lib.core.util.fib.FibSet.SOFT;
import static com.here.naksha.lib.core.util.fib.FibSet.STRONG;
import static com.here.naksha.lib.core.util.fib.FibSet.WEAK;
import static com.here.naksha.lib.core.util.fib.FibSet.indexOf;
import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;
import static com.here.naksha.lib.core.util.fib.FibSetOp.REMOVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import org.junit.jupiter.api.Test;

class FibSetTest {

  // Force a garbage collection and wait.
  static void gc(final WeakReference<?> ref) {
    System.gc();
    while (ref.get() != null) {
      try {
        //noinspection BusyWait
        Thread.sleep(10);
      } catch (InterruptedException ignore) {
      }
      System.gc();
    }
  }

  static final int foo_INDEX = indexOf("foo".hashCode(), CAPACITY_bits, 0);

  @Test
  void test_putUpgradeAndRemove() {
    final FibSet<String, FibMapEntry<String, String>> fibSet = new FibSet<>(FibMapEntry::new);
    assertSame(EMPTY, fibSet.root);

    // Create a new weak entry.
    final FibMapEntry<String, String> entry = fibSet.execute(PUT, "foo", WEAK);
    assertNotSame(EMPTY, fibSet.root);
    assertNotNull(fibSet.root[foo_INDEX]);
    assertInstanceOf(WeakReference.class, fibSet.root[foo_INDEX]);
    assertEquals(1L, fibSet.size);

    // Only the reference should have been replaced with a soft-reference.
    final FibMapEntry<String, String> softEntry = fibSet.execute(PUT, "foo", SOFT);
    assertSame(entry, softEntry);
    assertNotNull(fibSet.root[foo_INDEX]);
    assertInstanceOf(SoftReference.class, fibSet.root[foo_INDEX]);
    assertEquals(1L, fibSet.size);

    // Only the reference should have been replaced with a strong reference.
    final FibMapEntry<String, String> strongEntry = fibSet.execute(PUT, "foo", STRONG);
    assertSame(entry, strongEntry);
    assertNotNull(fibSet.root[foo_INDEX]);
    assertSame(entry, fibSet.root[foo_INDEX]);
    assertEquals(1L, fibSet.size);

    // We should not downgrade from strong to weak reference!
    final FibMapEntry<String, String> notWeak = fibSet.execute(PUT, "foo", WEAK);
    assertSame(entry, notWeak);
    assertSame(entry, fibSet.root[foo_INDEX]);
    assertEquals(1L, fibSet.size);

    // Removing the entry must return the original entry.
    final FibMapEntry<String, String> removed = fibSet.execute(REMOVE, "foo", STRONG);
    assertSame(entry, removed);
    assertNull(fibSet.root[foo_INDEX]);
    assertEquals(0L, fibSet.size);

    // Removing the key again must return null.
    final FibMapEntry<String, String> undefined = fibSet.execute(GET, "foo", STRONG);
    assertNull(undefined);
    assertNull(fibSet.root[foo_INDEX]);
  }

  @Test
  void test_gc() {
    final FibSet<String, FibMapEntry<String, String>> fibSet = new FibSet<>(FibMapEntry::new);
    assertSame(EMPTY, fibSet.root);

    FibMapEntry<String, String> foo = fibSet.putWeak("foo");
    assertNotNull(foo);
    assertNotNull(fibSet.root[foo_INDEX]);
    assertEquals(1L, fibSet.size);

    final WeakReference<?> weakReference = assertInstanceOf(WeakReference.class, fibSet.root[foo_INDEX]);
    assertNotNull(weakReference);
    assertSame(foo, weakReference.get());
    assertEquals(1L, fibSet.size);

    // Collect the weak ref.
    foo = null;
    gc(weakReference);
    assertNull(weakReference.get());

    // For now, the entry should still be reported.
    assertEquals(1L, fibSet.size);

    // Note: A side effect of the GET should be fixing of the size and removing the pending weak-reference.
    foo = fibSet.get("foo");
    assertNull(foo);
    assertNull(fibSet.root[foo_INDEX]);
    assertEquals(0L, fibSet.size);
  }
}
