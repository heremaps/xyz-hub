package com.here.xyz.util;

import static java.lang.invoke.VarHandle.loadLoadFence;
import static java.lang.invoke.VarHandle.storeStoreFence;

import com.here.xyz.lambdas.F1;
import com.here.xyz.lambdas.F4;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Functional implementation of a recursive, thread safe, growing map, based upon <a
 * href="https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/">Fibonacci
 * Hashing</a>, see as well the <a href="https://news.ycombinator.com/item?id=17328756">Y-Combinator
 * discussion</a> with helpful hints about the good and the bad. In Naksha we use {@link String}'s
 * as keys, for which the Fibonacci distribution seems to fit perfectly well.
 *
 * <p>Beware that we only use Fibonacci as distribution, this map does not implement an own hash
 * algorithm, it still operates best with good hash algorithms.
 *
 * <p><b>Note</b>: This implementation compares the key and values using {@link ILike#equals(Object,
 * Object)}, therefore, when using own key, implementing the {@link ILike} interface will be helpful
 * to get better matches and even allows to add weak-keys, which are automatically reclaimed, when
 * another key with the same hash-code is added (this is what the {@link StringCache} implementation
 * does).
 *
 * <p>If there is a hash collision, this implementation will perform a linear probing, resulting in
 * rather bad performance and memory consumption. The main idea behind this implementation is to be
 * memory efficient with fair performance. There is no fill-factor, because the map automatically
 * expands when a hash collision happens. As long as only some part of the hash-code collides this
 * is solved with just a small amount of memory, but for a full hash collision a rather big amount
 * of memory is used.
 */
@SuppressWarnings({"unused", "StringOperationCanBeSimplified"})
public final class FibMap {
    //
    // Note: Even while LoadLoad barriers are not needed on Intel:
    // * Reads are not reordered with other reads. [LoadLoad]
    // * Writes are not reordered with older reads. [LoadStore]
    // * Writes to memory are not reordered with other writes [StoreStore]
    // See:
    // http://www.intel.com/content/www/us/en/processors/architectures-software-developer-manuals.html
    //
    // This does not apply to ARM. For ARM CPU we add the LoadLoad barriers, which will effectively
    // translate into a LoadLoadStore barrier:
    // * Load - Load/Store: This means that the barrier requires all loads to complete before the
    // barrier but does not require
    //                      stores to complete. Both loads and stores that appear after the barrier in
    // program order must wait
    //                      for the barrier to complete.
    // See: https://developer.arm.com/documentation/100941/0101/Barriers
    //

    /** The empty array used by default, so that empty maps do not consume memory. */
    public static final Object[] EMPTY = new Object[0];

    /**
     * The <a href="https://www.sciencedirect.com/science/article/pii/S089812210800031X">golden ratio
     * (first 50 digits)</a>.
     */
    public static final double GOLDEN_RATIO = 1.6180339887498948482045868343656381177203091798058d;

    /**
     * The 32-bit hash multiplier ({@code 2654435769}). This was calculated using <a
     * href="https://keisan.casio.com/calculator">keisan.casio.com</a> and then converted into binary
     * using <a
     * href="https://www.rapidtables.com/convert/number/decimal-to-binary.html">www.rapidtables.com</a>.
     */
    public static final int MUL32 = 0b10011110001101110111100110111001;

    /**
     * The 64-bit hash multiplier ({@code 11400714819323198485}). This was calculated using <a
     * href="https://keisan.casio.com/calculator">keisan.casio.com</a> and then converted into binary
     * using <a
     * href="https://www.rapidtables.com/convert/number/decimal-to-binary.html">www.rapidtables.com</a>.
     */
    public static final long MUL64 = 0b1001111000110111011110011011100101111111010010100111110000010101L;

    /** The default amount of bits to used for each segment. */
    public static final int SEGMENT_bits = 4;

    /**
     * The default maximum depth, basically the hash-code size divided by the amount of bits used per
     * segment. This results in a maximum amount of 16^8 (4,294,967,296, ~4 billion) distinct keys,
     * when combined with the default {@link #SEGMENT_bits}. If that level reached, the table will
     * fall back to linear probing, when hash-codes collide.
     */
    public static final int MAX_DEPTH = 32 / SEGMENT_bits;

    /**
     * Because we store the keys and values behind each other, the key is always at an even index and
     * the value at an odd index. So, when we turn the hash into an index, we have to clear the lowest
     * bit, to know the index of the key. We use this opportunity to mask the sign-bit too.
     */
    public static final int KEY_INDEX_MASK = 0x7ffffffe;

    /** The object that represents the absence of a key-value pair. */
    public static final Object UNDEFINED = new String("UNDEFINED");

    /** An object to signal a conflict without details. */
    public static final Object CONFLICT = new String("CONFLICT");

    /** The object to signal that any value is applicable. */
    public static final Object ANY = new String("ANY");

    /** The object to signal that {@link #UNDEFINED} or {@code null} are applicable. */
    public static final Object VOID = new String("VOID");

    /** Internally used when searching for values. */
    private static final Object START_KEY_IS_NULL = new Object();

    /**
     * Returns the index between {@code 0 and 2^bits-1} using <a
     * href="https://news.ycombinator.com/item?id=17328756">Fibonacci Hashing</a>. Beware that the
     * name is a bit awkward, because it is rather a distribution algorithm and not an own hash
     * algorithm.
     *
     * @param hash The hash for which to calculate the index.
     * @param bits The amount of bits to use (the maximum value will then be 2^bits-1).
     * @param depth The depth, a value greater than or equal to zero, where zero means (root level).
     * @return The index for the hash in an array of the size 2^bits.
     */
    public static int indexOf(int hash, int bits, int depth) {
        assert ((depth * bits) + bits) <= 32;
        hash = hash ^ (hash >>> bits);
        return ((hash * MUL32) >>> (32 - bits - depth * bits)) & ((1 << bits) - 1);
    }

    /**
     * Returns the index between {@code 0 and 2^bits-1} using <a
     * href="https://news.ycombinator.com/item?id=17328756">Fibonacci Hashing</a>. This guarantees
     * that the lowest bit is always zero.
     *
     * @param hash The hash for which to calculate the index.
     * @param bits The amount of bits to use (the maximum value will then be 2^bits-1).
     * @param depth The depth, a value greater than or equal to zero, where zero means (root level).
     * @return The index for the hash in an array of the size 2^bits.
     */
    public static int keyIndex(int hash, int bits, int depth) {
        return indexOf(hash, bits, depth) & KEY_INDEX_MASK;
    }

    /**
     * Returns the amount of bits that are available for use.
     *
     * @param array The array.
     * @return The amount of bits that are available for use.
     */
    @SuppressWarnings("GrazieInspection")
    public static int capacityBitsOf(Object @NotNull [] array) {
        // length 0 (empty) has 32 trailing zeros: 32 & 31 = 0 bits.
        // length 2 (one key-value pair) has 1 trailing zero: 1 bit.
        // length 4 (two key-value pairs) has 2 trailing zeros: 2 bit.
        // length 8 (four key-value pairs) has 3 trailing zeros: 3 bit.
        // length 16 (eight key-value pairs) has 4 trailing zeros: 4 bit.
        // ...
        return Integer.numberOfTrailingZeros(array.length) & 31;
    }

    /**
     * Returns the maximal amount of key-value pairs.
     *
     * @param array The array for which to return.
     * @return The maximal amount of key-value pairs.
     */
    public static int keyValuePairsOf(Object @NotNull [] array) {
        // For an empty array (0 bits) this returns zero: (1 << 0) >>> 1 == (1) >>> 1 == 0.
        // For an array with 1 bit, two values, therefore one key-value pair: (1 << 1) >>> 1 == (2) >>>
        // 1 == 1.
        // For an array with 2 bits, four values, therefore two key-value pairs: (1 << 2) >>> 1 == (4)
        // >>> 1 == 2.
        // ...
        return (1 << capacityBitsOf(array)) >>> 1;
    }

    /**
     * Returns the next entry that stores the given value or {@code null} if no such value found. Note
     * that the method accepts an entry as input, which will be returned when a result found. This
     * makes the call garbage free, no matter if a value found or not.
     *
     * @param startKey The key to start search (ignoring the value of this key and any value before
     *     it); {@code null} if start at the first available key.
     * @param value The value to search.
     * @param array The key-value array in which to search.
     * @param entry The entry to return; if {@code null}, creating a new entry on demand.
     * @return The next entry that stores the given value or {@code null} if no such value found.
     */
    public static @Nullable FibMapEntry searchValue(
            @Nullable Object startKey,
            final @Nullable Object value,
            final Object @NotNull [] array,
            @Nullable FibMapEntry entry) {
        final Object result = _searchValue(startKey, value, array, entry);
        return result instanceof FibMapEntry e ? e : null;
    }

    /**
     * Returns the next entry that stores the given value, {@code null} if no such value found or
     * {@link #START_KEY_IS_NULL}, if the {@code startKey} was found, but no further occurrence of the
     * value.
     *
     * @param startKey The key to start search (ignoring the value of this key and any value before
     *     it); {@code null} if start at the first available key.
     * @param value The value to search.
     * @param array The key-value array in which to search.
     * @param entry The entry to return; if {@code null}, creating a new entry on demand.
     * @return The next key-value entry, {@code null} if no such value found or {@link
     *     #START_KEY_IS_NULL}, if the {@code startKey} was found, but no further occurrence of the
     *     value.
     */
    static @Nullable Object _searchValue(
            @Nullable Object startKey,
            final @Nullable Object value,
            final Object @NotNull [] array,
            @Nullable FibMapEntry entry) {
        int i = 0;
        while (i < array.length) {
            final Object existing_key = array[i++];
            final Object existing_value = array[i++];

            if (existing_key instanceof Object[] sub_array) {
                final Object result = _searchValue(startKey, value, sub_array, entry);
                if (result instanceof FibMapEntry found) {
                    return found;
                }
                if (result == START_KEY_IS_NULL) {
                    startKey = null;
                }
                assert result == null;
            } else if (existing_key != null) {
                if (startKey != null) {
                    if (ILike.equals(existing_key, startKey)) {
                        startKey = null;
                    }
                } else if (ILike.equals(existing_value, value)) {
                    if (entry == null) {
                        return new FibMapEntry(existing_key, existing_value);
                    }
                    return entry.with(existing_key, existing_value);
                }
            }
        }
        return startKey == null ? START_KEY_IS_NULL : null;
    }

    /**
     * The default implementation of interning keys. This method only interns {@link String}'s using
     * the {@link StringCache}.
     *
     * @param key The input key.
     * @return The internalized key.
     */
    public static @NotNull Object intern(@NotNull Object key) {
        if (key instanceof String string) {
            return StringCache.intern(string);
        }
        return key;
    }

    /**
     * Create a conflict result, this just returns {@link #CONFLICT}. If more details needed, use
     * {@link FibMapConflict#FibMapConflict(Object, Object, Object, Object)}.
     *
     * @param key The key, that should be modified.
     * @param expected_value The value expected.
     * @param new_value The value to be set.
     * @param value The value found (will not be expected).
     * @return the conflict case.
     */
    public static @NotNull Object conflict(
            @NotNull Object key, @Nullable Object expected_value, @Nullable Object new_value, @Nullable Object value) {
        return CONFLICT;
    }

    /**
     * Returns true if the given key exists in this map.
     *
     * @param searchKey The key to search for.
     * @param array The array to search recursively through.
     * @return true if the key exists; false otherwise.
     */
    public static boolean containsKey(@NotNull Object searchKey, Object @NotNull [] array) {
        return _get(searchKey, searchKey.hashCode(), array, 0) != UNDEFINED;
    }

    /**
     * Returns the value assigned to the given search key.
     *
     * @param key The key to look for.
     * @param array The array in which to search.
     * @return The value found or {@link #UNDEFINED}, if the key does not exist.
     */
    public static @Nullable Object get(@NotNull Object key, Object @NotNull [] array) {
        return _get(key, key.hashCode(), array, 0);
    }

    /**
     * Returns the value assigned to the given search key.
     *
     * @param key The key to look for.
     * @param key_hash The hash-code of the key.
     * @param array The array in which to search.
     * @return The value found or {@link #UNDEFINED}, if the key does not exist.
     */
    static @Nullable Object _get(
            final @NotNull Object key, final int key_hash, final Object @NotNull [] array, final int depth) {
        assert depth < MAX_DEPTH;
        if (array.length == 0) {
            return false;
        }
        final int i = keyIndex(key_hash, capacityBitsOf(array), depth);
        assert i >= 0 && i + 1 < array.length;
        while (true) {
            final Object existing_value = array[i + 1];
            loadLoadFence();
            final Object existing_key = array[i];

            if (existing_key == LOCK_OBJECT) {
                Thread.yield();
                continue;
            }

            if (existing_key instanceof Object[] sub_array) {
                if (sub_array.length > LPT_HEADER_SIZE && sub_array[LPT_ID] == LPT_ID_OBJECT) {
                    assert sub_array[LPT_HASH_CODE] instanceof Integer hashCode && hashCode == key_hash;
                    int j = LPT_HEADER_SIZE;
                    while (j < sub_array.length) {
                        final Object sub_value = sub_array[j + 1];
                        loadLoadFence();
                        final Object sub_key = sub_array[j];

                        if (sub_key == LOCK_OBJECT) {
                            Thread.yield();
                            continue;
                        }
                        if (ILike.equals(sub_key, key)) {
                            return sub_value;
                        }
                        j += 2;
                    }
                    return UNDEFINED;
                }
                return _get(key, key_hash, sub_array, depth + 1);
            }

            if (ILike.equals(existing_key, key)) {
                return existing_value;
            }
            return UNDEFINED;
        }
    }

    /**
     * Returns the amount of set key-value pairs in the given array.
     *
     * @param array the fibonacci map array.
     * @return the amount of set key-value pairs.
     */
    public static long count(@Nullable Object @NotNull [] array) {
        if (array.length == 0) {
            return 0L;
        }
        long size = 0L;
        int i = array.length > LPT_HEADER_SIZE && array[LPT_ID] == LPT_ID_OBJECT ? LPT_HEADER_SIZE : 0;
        while (i < array.length) {
            final Object existing_key = array[i];

            if (existing_key == LOCK_OBJECT) {
                Thread.yield();
                continue;
            }

            if (existing_key instanceof Object[] sub_array) {
                size += count(sub_array);
            } else if (existing_key != null) {
                size++;
            }
            i += 2;
        }
        return size;
    }

    /**
     * Assigns the given key to the given value. If the value {@link #UNDEFINED} is given, removing
     * the key. This method uses the default values for segment sizes, key interning and maximum
     * depth.
     *
     * @param key The key to look for.
     * @param expected_value the existing value that is expected for the operation to succeed; {@link
     *     #ANY} if any value is okay or {@link #VOID} if {@link #UNDEFINED} and {@code null} are
     *     acceptable.
     * @param new_value The value to set; if {@link #UNDEFINED} removing the key.
     * @param create Whether, if no such key exists yet, creating the key ({@code true} or {@code
     *     false}), in doubt use {@code true}.
     * @param array The array in which to search.
     * @return The previously assigned value, {@link #UNDEFINED}, if the key did not exist and {@link
     *     FibMapConflict} if the operation failed due to a conflicting situation.
     * @throws NullPointerException If any of the required parameters is {@code null}.
     * @throws IllegalArgumentException If the given array is empty (length of zero), the new value is
     *     {@link FibMapConflict} or {@link #ANY}, or the expected value is {@link FibMapConflict}.
     */
    public static @Nullable Object put(
            @NotNull Object key,
            final @Nullable Object expected_value,
            final @Nullable Object new_value,
            final boolean create,
            final Object @NotNull [] array) {
        return put(key, expected_value, new_value, create, array, FibMap::intern, FibMap::conflict);
    }

    /**
     * Assigns the given key to the given value. If the value {@link #UNDEFINED} is given, removing
     * the key.
     *
     * @param key The key to look for.
     * @param expected_value the existing value that is expected for the operation to succeed; {@link
     *     #ANY} if any value is okay or {@link #VOID} if {@link #UNDEFINED} and {@code null} are
     *     acceptable.
     * @param new_value The value to set; if {@link #UNDEFINED} removing the key.
     * @param create Whether, if no such key exists yet, creating the key ({@code true} or {@code
     *     false}), in doubt use {@code true}.
     * @param array The array in which to search.
     * @param intern The method to intern the key, doubt, use {@link #intern(Object)}.
     * @param conflict the method to create a conflict case, in doubt use {@link #conflict(Object,
     *     Object, Object, Object)}.
     * @return The previously assigned value, {@link #UNDEFINED}, if the key did not exist, and some
     *     arbitrary conflict case, if the operation failed due to a conflicting situation.
     * @throws NullPointerException If any of the required parameters is {@code null}.
     * @throws IllegalArgumentException If the given array is empty (length of zero), the new value is
     *     {@link FibMapConflict} or {@link #ANY}, or the expected value is {@link FibMapConflict}.
     */
    public static @Nullable Object put(
            @NotNull Object key,
            final @Nullable Object expected_value,
            final @Nullable Object new_value,
            final boolean create,
            final Object @NotNull [] array,
            final @NotNull F1<@NotNull Object, @NotNull Object> intern,
            final @NotNull F4<@NotNull Object, @NotNull Object, @Nullable Object, @Nullable Object, @Nullable Object>
                            conflict) {
        if (array.length == 0) {
            throw new IllegalArgumentException("The given array has a length of zero");
        }
        if (new_value instanceof FibMapConflict || new_value == CONFLICT || new_value == ANY || new_value == VOID) {
            throw new IllegalArgumentException("new value must not be FibConflict, CONFLICT, ANY or VOID");
        }
        if (expected_value instanceof FibMapConflict || expected_value == CONFLICT) {
            throw new IllegalArgumentException("expected value must not be FibConflict or CONFLICT");
        }
        return _put(key, key.hashCode(), expected_value, new_value, create, array, 0, intern, conflict);
    }

    /**
     * Assigns the given key to the given value. If the value {@link #UNDEFINED} is given, removing
     * the key.
     *
     * @param key the key to look for.
     * @param key_hash the hash-code of the key.
     * @param expected_value the existing value that is expected for the operation to succeed; {@link
     *     #ANY} if any value is okay or {@link #VOID} if {@link #UNDEFINED} and {@code null} are
     *     acceptable.
     * @param new_value the value to set; if {@link #UNDEFINED} removing the key.
     * @param create whether, if no such key exists yet, creating the key ({@code true} or {@code
     *     false}), in doubt use {@code true}.
     * @param array the array in which to search.
     * @param depth the current depth, in doubt use 0.
     * @param intern the method to intern the key, in doubt use {@link #intern(Object)}.
     * @param conflict the method to create a conflict case, in doubt use {@link #conflict(Object,
     *     Object, Object, Object)}.
     * @return The previously assigned value, {@link #UNDEFINED}, if the key did not exist, and some
     *     arbitrary conflict case, if the operation failed due to a conflicting situation.
     * @throws NullPointerException If any of the required parameters is {@code null}.
     * @throws IllegalArgumentException If the given array is empty (length of zero), the new value is
     *     {@link FibMapConflict} or {@link #ANY}, or the expected value is {@link FibMapConflict}.
     */
    static @Nullable Object _put(
            @NotNull Object key,
            final int key_hash,
            final @Nullable Object expected_value,
            final @Nullable Object new_value,
            final boolean create,
            final Object @NotNull [] array,
            final int depth,
            final @NotNull F1<@NotNull Object, @NotNull Object> intern,
            final @NotNull F4<@NotNull Object, @NotNull Object, @Nullable Object, @Nullable Object, @Nullable Object>
                            conflict) {
        assert depth < MAX_DEPTH;
        final int ki = keyIndex(key_hash, capacityBitsOf(array), depth);
        final int vi = ki + 1;
        assert ki >= 0 && vi < array.length;
        while (true) {
            final Object existing_key = array[ki];
            loadLoadFence();
            final Object existing_value = array[vi];
            assert existing_value != UNDEFINED
                    && existing_value != ANY
                    && existing_value != VOID
                    && existing_value != CONFLICT
                    && !(existing_value instanceof FibMapConflict);

            if (existing_key == LOCK_OBJECT) {
                Thread.yield();
                continue;
            }

            if (existing_key instanceof Object[] segment) {
                if (segment.length > LPT_HEADER_SIZE && segment[LPT_ID] == LPT_ID_OBJECT) {
                    assert depth + 1 == MAX_DEPTH
                            && segment[LPT_HASH_CODE] instanceof Integer hashCode
                            && hashCode == key_hash;
                    // Lock the key and perform a linear insertion.
                    if (ARRAY.compareAndSet(array, ki, segment, LOCK_OBJECT)) {
                        // We know that no other thread is mutating this array the same time, but:
                        // There can be concurrent readers!
                        try {
                            key = intern.call(key);
                            int empty = -1;
                            int j = LPT_HEADER_SIZE;
                            while (j < segment.length) {
                                final Object the_key = segment[j];
                                final Object the_value = segment[j + 1];
                                if (the_key != null && ILike.equals(the_key, key)) {
                                    // Matching key, update the value.
                                    if (expected_value == UNDEFINED) {
                                        return conflict.call(key, expected_value, new_value, the_value);
                                    }
                                    if ((expected_value == VOID && the_value != null)
                                            || (expected_value != ANY && the_value != existing_value)) {
                                        return conflict.call(key, expected_value, new_value, the_value);
                                    }
                                    if (new_value == UNDEFINED) {
                                        // We want the key to become visible before the value, because for null-keys the
                                        // value is ignored.
                                        segment[j] = null;
                                        storeStoreFence();
                                        segment[j + 1] = null;
                                    } else {
                                        segment[j + 1] = new_value;
                                    }
                                    return the_value;
                                }
                                if (empty < 0 && the_key == null) {
                                    empty = j;
                                }
                                j += 2;
                            }
                            // The key does not yet exist.
                            if (expected_value != ANY && expected_value != VOID && expected_value != UNDEFINED) {
                                return conflict.call(key, expected_value, new_value, UNDEFINED);
                            }
                            if (new_value == UNDEFINED) {
                                return UNDEFINED;
                            }
                            if (empty < 0) {
                                empty = segment.length;
                                segment = Arrays.copyOf(segment, segment.length + LPT_CHUNK_SIZE);
                            }
                            assert empty + 1 < segment.length;
                            segment[empty + 1] = new_value;
                            // We ensure order:
                            // Write value first, when this becomes visible to readers before the key-write, they
                            // will ignore it, because key is null.
                            storeStoreFence();
                            segment[empty] = key;
                            return UNDEFINED;
                        } finally {
                            // Replace the lock with the segment again.
                            ARRAY.setRelease(array, ki, segment);
                        }
                    }
                    // Conflict: concurrent update.
                    continue;
                }
                return _put(key, key_hash, expected_value, new_value, create, segment, depth + 1, intern, conflict);
            }

            if (existing_key != null) {
                if (ILike.equals(existing_key, key)) {
                    if (expected_value == UNDEFINED) {
                        return conflict.call(key, expected_value, new_value, existing_value);
                    }
                    if ((expected_value == VOID && existing_value != null)
                            || (expected_value != ANY && expected_value != existing_value)) {
                        return conflict.call(key, expected_value, new_value, existing_value);
                    }
                    if (ARRAY.compareAndSet(array, ki, existing_key, LOCK_OBJECT)) {
                        if (new_value == UNDEFINED) {
                            array[vi] = null;
                            ARRAY.setRelease(array, ki, null);
                            return existing_value;
                        }
                        array[vi] = new_value;
                        ARRAY.setRelease(array, ki, existing_key);
                        return existing_value;
                    }
                    // Race condition, another thread modifies concurrently.
                    Thread.yield();
                    continue;
                }

                // Collision: Same hash, but new key.
                // This code is a duplicate, but checking right here safes use from allocating memory for
                // the sub-array, if not needed.
                if (new_value == UNDEFINED) {
                    // We should remove the key, and the key does not exist.
                    if (expected_value == ANY || expected_value == UNDEFINED || expected_value == VOID) {
                        return UNDEFINED;
                    }
                    return conflict.call(key, expected_value, new_value, UNDEFINED);
                }
                if (!create) {
                    return conflict.call(key, expected_value, new_value, existing_value);
                }
                // End of duplication for memory efficiency.

                // We need to insert the new key.

                // If we reached the end of the depth, add a linear probing table and the new key into it.
                if (depth + 1 == MAX_DEPTH) {
                    final Object[] linear_array = new Object[LPT_HEADER_SIZE + LPT_CHUNK_SIZE];
                    linear_array[LPT_ID] = LPT_ID_OBJECT;
                    linear_array[LPT_HASH_CODE] = key_hash;
                    linear_array[LPT_HEADER_SIZE] = existing_key;
                    linear_array[LPT_HEADER_SIZE + 1] = existing_value;
                    key = intern.call(key);
                    linear_array[LPT_HEADER_SIZE + 2] = key;
                    linear_array[LPT_HEADER_SIZE + 3] = new_value;
                    // We do not need the lock, because the value is ignored ones the key is updated!
                    if (ARRAY.compareAndSet(array, ki, existing_key, linear_array)) {
                        array[vi] = null; // Be nice to the GC.
                        // The actual insertion need to happen in this method!
                        return UNDEFINED;
                    }
                    // Race condition, another thread modifies concurrently.
                    continue;
                }
                // We need to create a new sub-array that must be initialized with the existing key, so we
                // can add the given key.
                final Object[] sub_array = new Object[1 << SEGMENT_bits];
                final int sub_ki = keyIndex(existing_key.hashCode(), capacityBitsOf(sub_array), depth + 1);
                final int sub_vi = sub_ki + 1;
                assert sub_ki >= 0 && sub_vi < sub_array.length;
                sub_array[sub_ki] = existing_key;
                sub_array[sub_vi] = existing_value;
                // We do not need the lock, because the value is ignored ones the key is updated!
                if (ARRAY.compareAndSet(array, ki, existing_key, sub_array)) {
                    // We created a new sub-array, so delete the old value reference for GC.
                    array[vi] = null; // Be nice to the GC.
                    return _put(key, key_hash, expected_value, new_value, true, sub_array, depth + 1, intern, conflict);
                }
                // Race condition, another thread modifies concurrently.
                continue;
            }

            // Key does not exist.

            // If we should remove the key.
            if (new_value == UNDEFINED) {
                if (expected_value == ANY || expected_value == UNDEFINED || expected_value == VOID) {
                    return UNDEFINED;
                }
                return conflict.call(key, expected_value, new_value, UNDEFINED);
            }
            if (!create) {
                return conflict.call(key, expected_value, new_value, existing_value);
            }

            // Create the key.
            key = intern.call(key);
            if (ARRAY.compareAndSet(array, ki, null, LOCK_OBJECT)) {
                array[vi] = new_value;
                VarHandle.storeStoreFence();
                array[ki] = key;
                return UNDEFINED;
            }
            // Race condition, another thread modified concurrently.
        }
    }

    /**
     * Creates a new fibonacci map. Technically {@link #EMPTY} is as well valid, but immutable.
     *
     * @return a new fibonacci map.
     */
    public static @Nullable Object @NotNull [] newFibMap() {
        return new Object[1 << SEGMENT_bits];
    }

    /**
     * Before we modify a key, we insert a lock object to lock the slot. All other threads, including
     * readers, will wait for the lock. When we are done with the modification, we will release the
     * lock and reinsert either the key or a new sub-segment. This prevents that any concurrently
     * reading client reads inconsistent values.
     */
    static final Object LOCK_OBJECT = new String("LOCK_OBJECT");

    /** Internally used as unique identifier for linear probing tables. */
    static final Object LPT_ID_OBJECT = new String("LPT_ID_OBJECT");

    // Position of the unique linear probing table identifier.
    static final int LPT_ID = 0;
    // Position of the hash code to check code integrity.
    static final int LPT_HASH_CODE = 1;
    // Position of the reference to the next linear probing table, should we need more.
    static final int LPT_HEADER_SIZE = 2;
    // The amount of slots to be acquired when increasing the size of the linear probing table.
    static final int LPT_CHUNK_SIZE = 16;

    private static final @NotNull VarHandle ARRAY;

    static {
        try {
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }
}
