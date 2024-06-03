/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import static com.here.naksha.lib.core.util.fib.FibSet.SIZE;
import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;
import static com.here.naksha.lib.core.util.fib.FibSetOp.REMOVE;

import com.here.naksha.lib.core.util.ILike;
import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is the “last” resort to resolve hash collisions. We do use a lock on this table for modifications.
 *
 * @param <KEY>   the key-type.
 * @param <ENTRY> the entry-type.
 */
class FibLinearProbeTable<KEY, ENTRY extends FibEntry<KEY>> {

  FibLinearProbeTable(@NotNull FibSet<KEY, ENTRY> fibSet, int hashCode, @NotNull Object... init_refs) {
    assert init_refs.length < (1 << CAPACITY_bits);
    this.fibSet = fibSet;
    this.hashCode = hashCode;
    entries = new Object[1 << CAPACITY_bits];
    System.arraycopy(init_refs, 0, entries, 0, init_refs.length);
    VarHandle.fullFence();
  }

  final @NotNull FibSet<KEY, ENTRY> fibSet;
  final int hashCode;

  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * The lock, when performing modifications for this table.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * The linear probing table.
   */
  volatile Object @NotNull [] entries;

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  ENTRY execute(final @NotNull FibSetOp op, final @NotNull KEY key, final @NotNull FibRefType refType) {
    if (op == GET) {
      // A read operation lock-free, except we clear pending dead references (side-effect implementation).
      boolean locked = false;
      try {
        final Object[] array = this.entries;
        int i = 0;
        while (i < array.length) {
          final Object raw = array[i];
          final ENTRY entry;
          if (raw instanceof Reference<?>) {
            Reference<?> ref = (Reference<?>) raw;
            entry = (ENTRY) ref.get();
            if (entry == null && lock.tryLock()) {
              locked = true;
              array[i] = null;
              SIZE.getAndAdd(fibSet, -1L);
            }
          } else {
            entry = (ENTRY) raw;
          }
          if (entry != null && ILike.equals(entry, key)) {
            return entry;
          }
          i++;
        }
        return null;
      } finally {
        if (locked) {
          lock.unlock();
        }
      }
    }
    assert !op.readOnly;
    // We lock all mutations or possible mutations.
    lock.lock();
    try {
      Object[] array = this.entries;
      int index = 0;
      int emptyIndex = -1;
      while (index < array.length) {
        final Object raw_ref = array[index];
        final ENTRY raw_entry;
        if (raw_ref instanceof Reference) {
          Reference ref = (Reference) raw_ref;
          raw_entry = (ENTRY) ref.get();
          if (raw_entry == null) {
            // We have a lock and can remove the pending reference.
            array[index] = null;
            SIZE.getAndAdd(fibSet, -1L);
          }
        } else {
          raw_entry = (ENTRY) raw_ref;
        }

        if (raw_entry == null) {
          if (emptyIndex < 0) {
            emptyIndex = index;
          }
        } else if (ILike.equals(raw_entry, key)) {
          if (op == PUT) {
            if (refType.upgradeRaw(raw_ref)) {
              array[index] = refType.newRef(raw_entry);
            }
            return raw_entry;
          }
          assert op == REMOVE;
          array[index] = null;
          SIZE.getAndAdd(fibSet, -1L);
          return raw_entry;
        }
        index++;
      }

      // The key does not exist.
      if (op == REMOVE) {
        return null;
      }
      assert op == PUT;

      // We need to resize.
      if (emptyIndex < 0) {
        emptyIndex = array.length;
        this.entries = array = Arrays.copyOf(array, array.length + (1 << CAPACITY_bits));
      }

      final ENTRY new_entry = fibSet.newEntry.call(key);
      array[emptyIndex] = refType.newRef(new_entry);
      SIZE.getAndAdd(fibSet, 1L);
      return new_entry;
    } finally {
      lock.unlock();
    }
  }
}
