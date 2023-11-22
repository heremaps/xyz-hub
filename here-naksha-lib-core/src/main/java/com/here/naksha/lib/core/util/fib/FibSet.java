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

import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;
import static com.here.naksha.lib.core.util.fib.FibSetOp.REMOVE;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.util.ILike;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A recursive, thread safe, weak/soft/strong referencing set, based upon <a
 * href="https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/">Fibonacci
 * Hashing</a>, see as well the <a href="https://news.ycombinator.com/item?id=17328756">Y-Combinator discussion</a> with helpful hints about
 * the good and the bad.
 *
 * <p>Beware that we only use Fibonacci as distribution method, this set does not implement an own hash algorithm, it still operates best
 * with good hash algorithms.
 *
 * <p>If there is a hash collision, this implementation will perform a linear probing, resulting in rather bad performance and memory
 * allocation behavior. The main idea behind this implementation is to be memory efficient with fair performance.
 */
@AvailableSince(NakshaVersion.v2_0_5)
@SuppressWarnings({"rawtypes", "unused"})
public class FibSet<KEY, ENTRY extends FibEntry<KEY>> {

  /**
   * The empty array used by default, so that empty maps do not consume memory.
   */
  static final Object[] EMPTY = new Object[0];

  /**
   * The <a href="https://www.sciencedirect.com/science/article/pii/S089812210800031X">golden ratio (first 50 digits)</a>.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final double GOLDEN_RATIO = 1.6180339887498948482045868343656381177203091798058d;

  /**
   * The 32-bit hash multiplier ({@code 2654435769}). This was calculated using <a
   * href="https://keisan.casio.com/calculator">keisan.casio.com</a> and then converted into binary using <a
   * href="https://www.rapidtables.com/convert/number/decimal-to-binary.html">www.rapidtables.com</a>.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final int MUL32 = 0b10011110001101110111100110111001;

  /**
   * The 64-bit hash multiplier ({@code 11400714819323198485}). This was calculated using <a
   * href="https://keisan.casio.com/calculator">keisan.casio.com</a> and then converted into binary using <a
   * href="https://www.rapidtables.com/convert/number/decimal-to-binary.html">www.rapidtables.com</a>.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final long MUL64 = 0b1001111000110111011110011011100101111111010010100111110000010101L;

  /**
   * The default amount of bits to used for each segment.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final int CAPACITY_bits = 4;

  /**
   * The default maximum depth, basically the hash-code size divided by the amount of bits used per segment. This results in a maximum
   * amount of 16^8 (4,294,967,296, ~4 billion) distinct keys, when combined with the default {@link #CAPACITY_bits}. If that level reached,
   * the table will fall back to linear probing, when hash-codes collide.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final int MAX_DEPTH = 32 / CAPACITY_bits;

  /**
   * Because we store the keys and values behind each other, the key is always at an even index and the value at an odd index. So, when we
   * turn the hash into an index, we have to clear the lowest bit, to know the index of the key. We use this opportunity to mask the
   * sign-bit too.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final int KEY_INDEX_MASK = 0x7ffffffe;

  /**
   * Strong reference.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final FibRefType STRONG = new FibRefType() {

    @Override
    public boolean upgradeStrong(@NotNull FibEntry<?> entry) {
      return false;
    }

    @Override
    public boolean upgradeRef(@NotNull Reference<FibEntry<?>> reference) {
      return true;
    }

    @Override
    public @NotNull Object newRef(@NotNull FibEntry<?> entry) {
      return entry;
    }
  };

  /**
   * Soft-reference.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final FibRefType SOFT = new FibRefType() {

    @Override
    public boolean upgradeStrong(@NotNull FibEntry<?> entry) {
      return false;
    }

    @Override
    public boolean upgradeRef(@NotNull Reference<FibEntry<?>> reference) {
      return reference instanceof WeakReference<?>;
    }

    @Override
    public @NotNull Object newRef(@NotNull FibEntry<?> entry) {
      return new SoftReference<>(entry);
    }
  };

  /**
   * Weak-reference.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static final FibRefType WEAK = new FibRefType() {

    @Override
    public boolean upgradeStrong(@NotNull FibEntry<?> entry) {
      return false;
    }

    @Override
    public boolean upgradeRef(@NotNull Reference<FibEntry<?>> reference) {
      return false;
    }

    @Override
    public @NotNull Object newRef(@NotNull FibEntry<?> entry) {
      return new WeakReference<>(entry);
    }
  };

  /**
   * Returns the index between {@code 0 and 2^bits-1} using <a href="https://news.ycombinator.com/item?id=17328756">Fibonacci Hashing</a>.
   * Beware that the name is a bit awkward, because it is rather a distribution algorithm and not an own hash algorithm.
   *
   * @param hash  The hash for which to calculate the index.
   * @param bits  The amount of bits to use (the maximum value will then be 2^bits-1).
   * @param depth The depth, a value greater than or equal to zero, where zero means (root level).
   * @return The index for the hash in an array of the size 2^bits.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static int indexOf(int hash, int bits, int depth) {
    assert ((depth * bits) + bits) <= 32;
    hash = hash ^ (hash >>> bits);
    return ((hash * MUL32) >>> (32 - bits - depth * bits)) & ((1 << bits) - 1);
  }

  /**
   * Returns the amount of bits that are available for use.
   *
   * @param array the array.
   * @return The amount of bits that are available for use.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static int capacityBitsOf(Object @NotNull [] array) {
    // length 0 (empty) has 32 trailing zeros: 32 & 31 = 0 bits.
    // length 2 has 1 trailing zero: 1 bit.
    // length 4 has 2 trailing zeros: 2 bit.
    // length 8 has 3 trailing zeros: 3 bit.
    // length 16 has 4 trailing zeros: 4 bit.
    // ...
    return Integer.numberOfTrailingZeros(array.length) & 31;
  }

  /**
   * Creates a new empty Fibonacci-Set.
   *
   * @param newEntry The constructor for new entries.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public FibSet(@NotNull F1<ENTRY, KEY> newEntry) {
    this.newEntry = newEntry;
    this.root = EMPTY;
  }

  /**
   * The root of the set.
   */
  @SuppressWarnings("FieldMayBeFinal")
  Object @NotNull [] root;

  /**
   * The constructor to create new entries.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  protected @NotNull F1<ENTRY, KEY> newEntry;

  /**
   * The maximal amount of references. This includes weak and soft references, that may already been garbage collected, but where the
   * remaining weak-/soft-references are not yet removed.
   */
  long size;

  /**
   * Returns the amount of references. This includes weak and soft references, that may already been garbage collected, but where the *
   * remaining weak-/soft-references are not yet removed., limited to integer. If the map contains more than 2 billion
   * ({@link Integer#MAX_VALUE}) elements, the returning {@link Integer#MAX_VALUE}.
   *
   * <p>This method simply invokes {@link #entries()}.
   *
   * @return the size.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public int size() {
    final long size = this.entries();
    return size > (long) Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
  }

  /**
   * Returns the amount of references. This includes weak and soft references, that may already been garbage collected, but where the
   * remaining weak-/soft-references are not yet removed.
   *
   * @return the maximal amount of valid references.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public long entries() {
    return this.size;
  }

  /**
   * Returns the all entries in our FibSet.
   *
   * @return the set of Entries.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public Set<ENTRY> getAll() {
    Set<ENTRY> allValues = new HashSet<>();
    List<@NotNull FibLinearProbeTable<KEY, ENTRY>> lpts = getAllLPTs();
    for (FibLinearProbeTable<KEY, ENTRY> lpt : lpts) {
      Object[] entries = lpt.entries;
      for (int i = 0; i < entries.length; i += 2) {
        if (entries[i] instanceof FibEntry) {
          allValues.add((ENTRY) entries[i]);
        }
      }
    }
    return allValues;
  }

  /**
   * Returns a mutable or read-only root.
   *
   * @param op the operation for which to return the root.
   * @return a mutable root.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  protected Object @NotNull [] root(@NotNull FibSetOp op) {
    Object[] root = this.root;
    if (op.readOnly) {
      return root;
    }
    if (root == EMPTY) {
      root = new Object[1 << CAPACITY_bits];
      if (ROOT.compareAndSet(this, EMPTY, root)) {
        return root;
      }
      VarHandle.loadLoadFence();
      root = this.root;
    }
    return root;
  }

  @NotNull
  List<@NotNull FibLinearProbeTable<KEY, ENTRY>> getAllLPTs() {
    final ArrayList<@NotNull FibLinearProbeTable<KEY, ENTRY>> list = new ArrayList<>();
    getAllLPTs(list, root);
    return list;
  }

  void getAllLPTs(@NotNull List<@NotNull FibLinearProbeTable<KEY, ENTRY>> list, Object @NotNull [] array) {
    for (final Object raw : array) {
      if (raw instanceof Object[]) {
        Object[] children = (Object[]) raw;
        getAllLPTs(list, children);
      } else if (raw instanceof FibLinearProbeTable<?, ?>) {
        //noinspection unchecked
        list.add((FibLinearProbeTable<KEY, ENTRY>) raw);
      }
    }
  }

  /**
   * Returns the strong referred entry for the key. If the existing reference is not strong, or {@code null}, make it strong or create the
   * entry.
   *
   * @param key the key of the entry to lookup.
   * @return The entry for the given key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @NotNull ENTRY put(final @NotNull KEY key) {
    final ENTRY entry = _execute(PUT, key, key.hashCode(), STRONG, root(PUT), 0);
    assert entry != null;
    return entry;
  }

  /**
   * Returns the at least soft referred entry for the key. If the existing reference is not at least soft, or {@code null}, make it soft or
   * create the entry.
   *
   * @param key the key of the entry to lookup.
   * @return The entry for the given key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @NotNull ENTRY putSoft(final @NotNull KEY key) {
    final ENTRY entry = _execute(PUT, key, key.hashCode(), SOFT, root(PUT), 0);
    assert entry != null;
    return entry;
  }

  /**
   * Returns the at least weak referred entry for the key, create a new weak referred entry, if no exists yet.
   *
   * @param key the key of the entry to lookup.
   * @return The entry for the given key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @NotNull ENTRY putWeak(final @NotNull KEY key) {
    final ENTRY entry = _execute(PUT, key, key.hashCode(), WEAK, root(PUT), 0);
    assert entry != null;
    return entry;
  }

  /**
   * Returns the entry of the given key.
   *
   * @param key     the key of the entry to lookup.
   * @param refType the reference type to the entry.
   * @return The entry.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @NotNull ENTRY put(final @NotNull KEY key, @NotNull FibRefType refType) {
    final ENTRY entry = _execute(PUT, key, key.hashCode(), refType, root(PUT), 0);
    assert entry != null;
    return entry;
  }

  /**
   * Returns the entry of the given key.
   *
   * @param key the key of the entry to lookup.
   * @return The entry or {@code null}, if the set does not contain the key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @Nullable ENTRY get(final @NotNull KEY key) {
    return _execute(GET, key, key.hashCode(), STRONG, root(GET), 0);
  }

  /**
   * Removes the entry of the given key.
   *
   * @param key the key of the entry to lookup.
   * @return The removed entry or {@code null}, if the set did not contain the key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @Nullable ENTRY remove(final @NotNull KEY key) {
    return _execute(REMOVE, key, key.hashCode(), STRONG, root(REMOVE), 0);
  }

  /**
   * Create an entry for the given key and return it. If the key is currently referred differently than the given
   * {@link FibRefType ref-type}, the increasing the strength of the reference.
   *
   * @param op      the operation to perform for the entry.
   * @param key     the key of the entry to lookup.
   * @param refType the reference type to the entry.
   * @return The entry or {@code null}, value depends upon state and operation.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public @Nullable ENTRY execute(
      final @NotNull FibSetOp op, final @NotNull KEY key, final @NotNull FibRefType refType) {
    return _execute(op, key, key.hashCode(), refType, root(op), 0);
  }

  /**
   * Create an entry for the given key and return it. If the key is currently referred differently than the given
   * {@link FibRefType ref-type}, the increasing the strength of the reference.
   *
   * @param op       the operation to perform for the entry.
   * @param key      the key of the entry to lookup.
   * @param key_hash the hash-code of the key (key.hashCode()).
   * @param refType  the reference type to the entry.
   * @param array    the array in which to search.
   * @param depth    the current depth, in doubt use 0.
   * @return The entry or {@code null}, value depends upon state and operation.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  ENTRY _execute(
      final @NotNull FibSetOp op,
      final @NotNull KEY key,
      final int key_hash,
      final @NotNull FibRefType refType,
      final Object @NotNull [] array,
      final int depth) {
    if (array.length == 0) {
      assert op == GET;
      return null;
    }
    assert depth < MAX_DEPTH;
    assert array.length == (1 << CAPACITY_bits);
    assert key.hashCode() == key_hash;

    final int index = indexOf(key_hash, CAPACITY_bits, depth);
    assert index >= 0 && index < array.length;
    // Optimistic locking, when we hit a race condition, we simply repeat the op.
    while (true) {
      final Object raw_ref = array[index];
      final Object raw_entry;
      if (raw_ref instanceof Reference<?>) {
        Reference<?> ref = (Reference<?>) raw_ref;
        raw_entry = ref.get();
        if (raw_entry == null) {
          if (!ARRAY.compareAndSet(array, index, ref, null)) {
            // Race condition, another thread modified the array slot.
            continue;
          }
          SIZE.getAndAdd(this, -1L);
        }
      } else {
        raw_entry = raw_ref;
      }

      if (raw_entry instanceof Object[]) {
        final Object[] children = (Object[]) raw_entry;
        return _execute(op, key, key_hash, refType, children, depth + 1);
      }

      if (raw_entry instanceof FibLinearProbeTable) {
        final FibLinearProbeTable lpt = (FibLinearProbeTable) raw_entry;
        assert lpt.hashCode() == key_hash;
        //noinspection unchecked
        return (ENTRY) lpt.execute(op, key, refType);
      }

      if (raw_entry instanceof FibEntry) {
        final FibEntry entry = (FibEntry) raw_entry;
        if (ILike.equals(entry, key)) {
          if (op == GET) {
            return (ENTRY) entry;
          }
          if (op == PUT) {
            if (!refType.upgradeRaw(raw_ref)) {
              return (ENTRY) entry;
            }
            final Object new_ref = refType.newRef(entry);
            if (ARRAY.compareAndSet(array, index, raw_ref, new_ref)) {
              return (ENTRY) entry;
            }
            // Race condition, other thread updated the reference concurrently.
            continue;
          }
          assert op == REMOVE;
          if (ARRAY.compareAndSet(array, index, raw_ref, null)) {
            SIZE.getAndAdd(this, -1L);
            return (ENTRY) entry;
          }
          // Race condition, other thread updated the reference concurrently.
          continue;
        }

        // The key does not exist.
        if (op == GET || op == REMOVE) {
          return null;
        }
        assert op == PUT;

        // Collision: Same hash, but other key instance.
        if (depth + 1 < MAX_DEPTH) {
          // We need to create a new sub-array that must be initialized with the existing key, so we can add
          // the new key.
          final Object[] sub_array = new Object[1 << CAPACITY_bits];
          final int sub_index = indexOf(raw_ref.hashCode(), CAPACITY_bits, depth + 1);
          assert sub_index >= 0 && sub_index < sub_array.length;
          sub_array[sub_index] = raw_ref;
          if (ARRAY.compareAndSet(array, index, raw_ref, sub_array)) {
            return _execute(op, key, key_hash, refType, sub_array, depth + 1);
          }
          // Race condition, another thread modified concurrently.
          continue;
        }

        // If we reached the end of the depth, add a linear probing table and the new key into it.
        final ENTRY new_entry = newEntry.call(key);
        final Object new_ref = refType.newRef(new_entry);
        assert new_ref instanceof Reference<?> || new_ref == new_entry;
        final FibLinearProbeTable lpt = new FibLinearProbeTable(this, key_hash, raw_ref, new_ref);
        if (ARRAY.compareAndSet(array, index, raw_ref, lpt)) {
          SIZE.getAndAdd(this, 1L);
          return new_entry;
        }
        // Race condition, another thread modified concurrently.
        continue;
      }

      // The key does not exist.
      if (op == GET || op == REMOVE) {
        return null;
      }
      assert op == PUT;

      // Create the key.
      final ENTRY new_entry = newEntry.call(key);
      final Object new_ref = refType.newRef(new_entry);
      if (ARRAY.compareAndSet(array, index, null, new_ref)) {
        SIZE.getAndAdd(this, 1L);
        return new_entry;
      }
      // Race condition, another thread modified concurrently.
    }
  }

  /**
   * Before we modify a key, we insert a lock object to lock the slot. All other threads, including readers, will wait for the lock. When we
   * are done with the modification, we will release the lock and reinsert either the key or a new sub-segment. This prevents that any
   * concurrently reading client reads inconsistent values.
   */
  static final Object LOCK_OBJECT = new String("LOCK_OBJECT");

  /**
   * Internally used as unique identifier for linear probing tables.
   */
  static final Object LPT_ID_OBJECT = new String("LPT_ID_OBJECT");

  // Position of the unique linear probing table identifier.
  static final int LPT_ID = 0;
  // Position of the hash code to check code integrity.
  static final int LPT_HASH_CODE = 1;
  // Position of the reference to the next linear probing table, should we need more.
  static final int LPT_HEADER_SIZE = 2;
  // The amount of slots to be acquired when increasing the size of the linear probing table.
  static final int LPT_CHUNK_SIZE = 16;

  static final @NotNull VarHandle ARRAY;
  static final @NotNull VarHandle ROOT;
  static final @NotNull VarHandle SIZE;

  static {
    try {
      ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
      ROOT = MethodHandles.lookup().findVarHandle(FibSet.class, "root", Object[].class);
      SIZE = MethodHandles.lookup().findVarHandle(FibSet.class, "size", long.class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
