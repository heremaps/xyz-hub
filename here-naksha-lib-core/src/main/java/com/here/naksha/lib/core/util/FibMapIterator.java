package com.here.naksha.lib.core.util;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** An iterator above a fibonacci map. */
@SuppressWarnings("unused")
public class FibMapIterator implements Iterator<Entry<@NotNull Object, @Nullable Object>> {

    /**
     * Creates a new iterator above the given array, which must be a fibonacci array.
     *
     * @param root The fibonacci array.
     */
    public FibMapIterator(@Nullable Object @NotNull [] root) {
        arrays = new Object[FibMap.MAX_DEPTH + 1][];
        indices = new int[FibMap.MAX_DEPTH + 1];
        arrays[0] = root;
        indices[0] = 0;
        depth = 0;
    }

    private final @Nullable Object @NotNull [] @Nullable [] arrays;
    private final int @NotNull [] indices;
    private int depth;

    /** The entry that is shared between this and all sub-iterators. */
    private @Nullable FibMapEntry the_entry;

    private @NotNull FibMapEntry entry(@NotNull Object key, @Nullable Object value) {
        FibMapEntry entry = the_entry;
        if (entry == null) {
            return the_entry = new FibMapEntry(key, value);
        }
        return entry.with(key, value);
    }

    private @Nullable FibMapEntry entry;

    @Override
    public boolean hasNext() {
        if (entry != null) {
            return true;
        }

        // No cached entry, and the sub is empty too, forward.
        final @Nullable Object @NotNull [] @Nullable [] arrays = this.arrays;
        final int @NotNull [] indices = this.indices;
        int depth = this.depth;
        while (depth >= 0) {
            assert depth < arrays.length && arrays[depth] != null;

            @Nullable Object @NotNull [] array = arrays[depth];
            int index = indices[depth];
            while (index < array.length) {
                assert index + 1 < array.length;
                final Object raw_key = array[index];
                final Object raw_value = array[index + 1];
                index += 2;

                if (raw_key instanceof Object[] sub_array) {
                    indices[depth++] = index;
                    arrays[depth] = array = sub_array;
                    indices[depth] = index = 0;
                    this.depth = depth;
                    continue;
                }

                if (raw_key != null) {
                    entry = entry(raw_key, raw_value);
                    indices[depth] = index;
                    this.depth = depth;
                    return true;
                }
            }
            arrays[depth--] = null;
        }
        this.depth = -1;
        return false;
    }

    /**
     * The same as {@link #next()}, except that the entry is not consumed. If this method is invoked
     * in a loop, it will always return the same entry.
     *
     * @return the next entry.
     */
    public @NotNull Entry<@NotNull Object, @Nullable Object> peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        assert entry != null;
        return entry;
    }

    @Override
    public @NotNull Entry<@NotNull Object, @Nullable Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        assert entry != null;
        final FibMapEntry entry = this.entry;
        this.entry = null;
        return entry;
    }
}
