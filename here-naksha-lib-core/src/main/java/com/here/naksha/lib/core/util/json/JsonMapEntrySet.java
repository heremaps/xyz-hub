package com.here.naksha.lib.core.util.json;


import java.util.Iterator;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class JsonMapEntrySet extends MapEntrySet<String, Object, JsonMap> {

    JsonMapEntrySet(@NotNull JsonMap map) {
        super(map, String.class, Object.class);
    }

    @Override
    public @NotNull Iterator<@NotNull Entry<@NotNull String, @Nullable Object>> iterator() {
        return new JsonMapEntryIterator(map);
    }
}
