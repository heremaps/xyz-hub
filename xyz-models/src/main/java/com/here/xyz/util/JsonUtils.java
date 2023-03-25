package com.here.xyz.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JsonUtils {

  /**
   * Clone the given object, but requires it to only persist out of JSON compatible types, being String, Boolean, Number, Map, List and
   * array of objects.
   *
   * @param object The object to clone.
   * @return the clone.
   * @throws IllegalArgumentException if the given object is of an illegal type.
   */
  @SuppressWarnings("unchecked")
  public static <T> @Nullable T deepCopy(@Nullable T object) {
    if (object == null) {
      return null;
    }
    if (object instanceof Map) {
      return (T) deepCopyMap((Map<String, Object>) object);
    }
    if (object instanceof List) {
      return (T) deepCopyList((List<Object>) object);
    }
    if (object.getClass().isArray()) {
      return (T) deepCopyArray((Object[]) object);
    }
    // These objects are immutable anyway, we can stick with the references!
    if (object instanceof String || object instanceof Number || object instanceof Boolean) {
      return object;
    }
    throw new IllegalArgumentException("Object is of illegal type: " + object.getClass().getName());
  }

  /**
   * Make a deep (recursive/shallow) copy of the given original map. This expects a JSON like map that only contains strings as key and
   * maps, arrays, lists or primitives.
   *
   * @param original The original JSON like map.
   * @return the copy.
   * @throws IllegalArgumentException if the given map contains a value that can't be cloned.
   */
  @SuppressWarnings("unchecked")
  public static @NotNull Map<@NotNull String, Object> deepCopyMap(@NotNull Map<@NotNull String, Object> original) {
    final HashMap<String, Object> clone = new HashMap<>();
    for (final Map.Entry<String, Object> entry : original.entrySet()) {
      final String key = entry.getKey();
      final Object value = deepCopy(entry.getValue());
      clone.put(key, value);
    }
    return clone;
  }

  /**
   * Clone the given JSON like list.
   *
   * @param original the list to clone.
   * @return the deep clone.
   * @throws IllegalArgumentException if the given list contains a value that can't be cloned.
   */
  public static @NotNull List<Object> deepCopyList(@NotNull List<Object> original) {
    final int SIZE = original.size();
    final ArrayList<Object> clone = new ArrayList<>(SIZE);
    for (final Object o : original) {
      clone.add(deepCopy(o));
    }
    return clone;
  }

  /**
   * Clone the given JSON like array.
   *
   * @param original the array to clone.
   * @return the deep clone.
   * @throws IllegalArgumentException if the given array contains a value that can't be cloned.
   */
  public static <T> @Nullable T @NotNull [] deepCopyArray(@Nullable T @NotNull [] original) {
    final @Nullable T @NotNull [] clone = Arrays.copyOf(original, original.length);
    for (int i = 0; i < clone.length; i++) {
      clone[i] = deepCopy(clone[i]);
    }
    return clone;
  }
}