package com.here.xyz.util.diff;


import java.util.Map;
import org.jetbrains.annotations.NotNull;

/** A method invoked to test if a key of a map should be ignored by the patcher. */
@FunctionalInterface
public interface IgnoreKey {

    /**
     * Tests whether the given key should be ignored by the patcher.
     *
     * @param key The key in question.
     * @param sourceMap The source map.
     * @param targetOrPatchMap The target map, or the partial patch map.
     * @return {@code true} if the key should be ignored; {@code false} otherwise.
     */
    @SuppressWarnings("rawtypes")
    boolean ignore(@NotNull Object key, @NotNull Map sourceMap, @NotNull Map targetOrPatchMap);
}
