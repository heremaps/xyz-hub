package com.here.naksha.lib.core.util.json;

import com.here.naksha.lib.core.util.FibMapIterator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class JsonMapEntryIterator implements Iterator<Entry<@NotNull String, @Nullable Object>> {

    JsonMapEntryIterator(@NotNull JsonMap map) {
        this.map = map;
        this.it = new FibMapIterator(map.root);
    }

    private final @NotNull JsonMap map;
    private final @NotNull FibMapIterator it;
    private @Nullable MapEntry<@NotNull String, @Nullable Object> entry;

    @NotNull
    MapEntry<@NotNull String, @Nullable Object> entry(@NotNull String key, @Nullable Object value) {
        MapEntry<@NotNull String, @Nullable Object> entry = this.entry;
        if (entry == null) {
            return this.entry = new MapEntry<>(map, key, value);
        }
        entry.key = key;
        entry.value = value;
        return entry;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public Entry<@NotNull String, @Nullable Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Entry<@NotNull Object, @Nullable Object> entry = it.next();
        assert entry.getKey() instanceof String;
        return entry((String) entry.getKey(), entry.getValue());
    }
}
