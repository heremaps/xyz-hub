package com.here.xyz.util.json;


import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Core implementation of a map-entry. */
public class MapEntry<K, V> implements Map.Entry<@NotNull K, @Nullable V> {

    public MapEntry(@NotNull Map<K, V> map, @NotNull K key, @Nullable V value) {
        this.map = map;
        this.key = key;
        this.value = value;
    }

    @JsonIgnore
    protected final @NotNull Map<K, V> map;

    @JsonIgnore
    protected @NotNull K key;

    @JsonIgnore
    protected @Nullable V value;

    @Override
    public @NotNull K getKey() {
        return key;
    }

    @Override
    public @Nullable V getValue() {
        return value;
    }

    @Override
    public @Nullable V setValue(@Nullable V value) {
        final V old = map.put(key, value);
        this.value = value;
        return old;
    }
}
