package com.here.naksha.lib.core.util.json;


import java.util.Iterator;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class JsonObjectEntrySet extends MapEntrySet<String, Object, JsonObject> {

    JsonObjectEntrySet(@NotNull JsonObject map) {
        super(map, String.class, Object.class);
    }

    @Nonnull
    @Override
    public Iterator<@NotNull Entry<@NotNull String, @Nullable Object>> iterator() {
        return new JsonObjectEntryIterator(map);
    }
}
