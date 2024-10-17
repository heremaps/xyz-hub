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

import static com.here.naksha.lib.core.util.fib.FibSet.SOFT;
import static com.here.naksha.lib.core.util.fib.FibSet.STRONG;
import static com.here.naksha.lib.core.util.fib.FibSet.WEAK;
import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;
import static com.here.naksha.lib.core.util.fib.FibSetOp.REMOVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class FibLinearProbeTableTest {

  @Test
  void test_putUpgradeAndRemove() {
    final FibSet<String, FibMapEntry<String, String>> SET = new FibSet<>(FibMapEntry::new);
    final FibLinearProbeTable<String, FibMapEntry<String, String>> lpt = new FibLinearProbeTable<>(SET, 0);
    assertEquals(0L, SET.size);

    // Create a new weak entry.
    final FibMapEntry<String, String> entry = lpt.execute(PUT, "foo", WEAK);
    assertNotNull(lpt.entries[0]);
    assertInstanceOf(WeakReference.class, lpt.entries[0]);
    assertEquals(1L, SET.size);

    // Only the reference should have been replaced with a soft-reference.
    final FibMapEntry<String, String> softEntry = lpt.execute(PUT, "foo", SOFT);
    assertSame(entry, softEntry);
    assertNotNull(lpt.entries[0]);
    assertInstanceOf(SoftReference.class, lpt.entries[0]);
    assertEquals(1L, SET.size);

    // Only the reference should have been replaced with a strong reference.
    final FibMapEntry<String, String> strongEntry = lpt.execute(PUT, "foo", STRONG);
    assertSame(entry, strongEntry);
    assertNotNull(lpt.entries[0]);
    assertSame(entry, lpt.entries[0]);
    assertEquals(1L, SET.size);

    // We should not downgrade from strong to weak reference!
    final FibMapEntry<String, String> notWeak = lpt.execute(PUT, "foo", WEAK);
    assertSame(entry, notWeak);
    assertSame(entry, lpt.entries[0]);
    assertEquals(1L, SET.size);

    // Removing the entry must return the original entry.
    final FibMapEntry<String, String> removed = lpt.execute(REMOVE, "foo", STRONG);
    assertSame(entry, removed);
    assertNull(lpt.entries[0]);
    assertEquals(0L, SET.size);

    // Removing the key again must return null.
    final FibMapEntry<String, String> undefined = lpt.execute(GET, "foo", STRONG);
    assertNull(undefined);
    assertNull(lpt.entries[0]);
  }

  @Test
  void test_gc() {
    final FibSet<String, FibMapEntry<String, String>> SET = new FibSet<>(FibMapEntry::new);
    final FibLinearProbeTable<String, FibMapEntry<String, String>> lpt = new FibLinearProbeTable<>(SET, 0);
    assertEquals(0L, SET.size);

    FibMapEntry<String, String> foo = lpt.execute(PUT, "foo", WEAK);
    assertNotNull(foo);
    assertNotNull(lpt.entries[0]);
    assertEquals(1L, SET.size);

    final WeakReference<?> weakReference = assertInstanceOf(WeakReference.class, lpt.entries[0]);
    assertNotNull(weakReference);
    assertSame(foo, weakReference.get());
    assertEquals(1L, SET.size);

    // Collect the weak ref.
    foo = null;
    FibSetTest.gc(weakReference);
    assertNull(weakReference.get());

    // For now, the entry should still be reported.
    assertEquals(1L, SET.size);

    // Note: A side effect of the GET should be fixing of the size and removing the pending weak-reference.
    // For a linear probing table this does not always work, but in our test it should as we have no other thread
    // accessing the LPT.
    foo = lpt.execute(GET, "foo", WEAK);
    assertNull(foo);
    assertNull(lpt.entries[0]);
    assertEquals(0L, SET.size);
  }

  @Test
  void test_expansion() {
    final FibSet<String, FibMapEntry<String, String>> SET = new FibSet<>(FibMapEntry::new);
    final FibLinearProbeTable<String, FibMapEntry<String, String>> lpt = new FibLinearProbeTable<>(SET, 0);
    assertEquals(0L, SET.size);

    final int SEG_SIZE = 1 << FibSet.CAPACITY_bits;
    final int TOTAL_SIZE = SEG_SIZE * 100;

    // Add 1000 entries.
    for (int i = 0; i < TOTAL_SIZE; i++) {
      final String si = Integer.toString(i);
      final FibMapEntry<?, ?> entry = assertInstanceOf(FibMapEntry.class, lpt.execute(PUT, si, STRONG));
      assertNotNull(entry);
      entry.value = si;
    }
    assertEquals(TOTAL_SIZE, SET.size);
    assertEquals(TOTAL_SIZE, lpt.entries.length);

    // We expect that the LPT now has an array with exactly this 1000 entries in order.
    for (int i = 0; i < TOTAL_SIZE; i++) {
      final String si = Integer.toString(i);
      final FibMapEntry<?, ?> entry = assertInstanceOf(FibMapEntry.class, lpt.entries[i]);
      assertEquals(si, entry.key);
      assertEquals(si, entry.value);
    }

    // Remove all keys and ensure the table is empty there-after.
    for (int i = 0; i < TOTAL_SIZE; ) {
      final String si = Integer.toString(i);
      final FibMapEntry<String, String> entry = lpt.execute(REMOVE, si, STRONG);
      assertNotNull(entry);
      assertNull(lpt.entries[i]);
      assertNull(lpt.execute(REMOVE, si, STRONG));
      i++;
      assertEquals(TOTAL_SIZE - i, SET.size);
    }
  }

  @Test
  void test_Lock() {
    final FibSet<String, FibMapEntry<String, String>> SET = new FibSet<>(FibMapEntry::new);
    final FibLinearProbeTable<String, FibMapEntry<String, String>> lpt = new FibLinearProbeTable<>(SET, 0);

    /**
     * First we put 2 weak references to lpt, then we call gc() - in such scenario next GET call should remove
     * all empty references, but to do this it has to acquire lock, and release it at the end.
     */

    lpt.execute(PUT, "foo", WEAK);
    lpt.execute(PUT, "foo1", WEAK);
    System.gc();
    lpt.execute(GET, "foo1", WEAK);

    // then
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<String> future = executor.submit(() -> {
      lpt.execute(PUT, "foo3", WEAK);
      return "done";
    });

    /**
     * Now we try to put new value to lpt in another thread - if previous locks were not released it should
     * throw timeout exception.
     */
    try {
      future.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      fail("lock not released! " + e);
    }
    executor.shutdownNow();
  }
}
