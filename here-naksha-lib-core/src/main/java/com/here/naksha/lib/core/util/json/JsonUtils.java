package com.here.naksha.lib.core.util.json;

import com.here.naksha.lib.core.lambdas.F2;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JsonUtils {

    //  /**
    //   * Clone the given object, but requires it to only persist out of JSON compatible types, being
    // String, Boolean, Number, Map, List and
    //   * array of objects.
    //   *
    //   * @param object The object to clone.
    //   * @return the clone.
    //   * @throws IllegalArgumentException if the given object is of an illegal type.
    //   */
    //  @SuppressWarnings("unchecked")
    //  public static <T> @Nullable T deepCopy(@Nullable T object) {
    //    if (object == null) {
    //      return null;
    //    }
    //    if (object instanceof Map) {
    //      return (T) deepCopyMap((Map<String, Object>) object);
    //    }
    //    if (object instanceof List) {
    //      return (T) deepCopyList((List<Object>) object);
    //    }
    //    if (object.getClass().isArray()) {
    //      final Class<?> componentClass = object.getClass().getComponentType();
    //      if (componentClass.isPrimitive()) {
    //        // TODO: Fix me!
    //        throw new UnsupportedOperationException("deepCopy of primitive array");
    //      }
    //      return (T) deepCopyObjectArray((T[]) object, (Class<T>) componentClass);
    //    }
    //    // These objects are immutable anyway, we can stick with the references!
    //    if (object instanceof String || object instanceof Number || object instanceof Boolean) {
    //      return object;
    //    }
    //    throw new IllegalArgumentException("Object is of illegal type: " +
    // object.getClass().getName());
    //  }
    //
    //  /**
    //   * Make a deep (recursive/shallow) copy of the given original map. This expects a JSON like
    // map that only contains strings as key and
    //   * maps, arrays, lists or primitives.
    //   *
    //   * @param original The original JSON like map.
    //   * @return the copy.
    //   * @throws IllegalArgumentException if the given map contains a value that can't be cloned.
    //   */
    //  public static @NotNull Map<@NotNull String, Object> deepCopyMap(@NotNull Map<@NotNull String,
    // Object> original) {
    //    final HashMap<String, Object> clone = new HashMap<>();
    //    for (final Map.Entry<String, Object> entry : original.entrySet()) {
    //      final String key = entry.getKey();
    //      final Object value = deepCopy(entry.getValue());
    //      clone.put(key, value);
    //    }
    //    return clone;
    //  }
    //
    //  /**
    //   * Clone the given JSON like list.
    //   *
    //   * @param original the list to clone.
    //   * @return the deep clone.
    //   * @throws IllegalArgumentException if the given list contains a value that can't be cloned.
    //   */
    //  public static @NotNull List<Object> deepCopyList(@NotNull List<Object> original) {
    //    final int SIZE = original.size();
    //    final ArrayList<Object> clone = new ArrayList<>(SIZE);
    //    for (final Object o : original) {
    //      clone.add(deepCopy(o));
    //    }
    //    return clone;
    //  }
    //
    //  /**
    //   * Clone the given JSON like array.
    //   *
    //   * @param original       the array to clone.
    //   * @param componentClass the class of the component.
    //   * @param <T>            the array component-type.
    //   * @return the deep clone.
    //   * @throws IllegalArgumentException if the given array contains a value that can't be cloned.
    //   */
    //  public static <T> @Nullable T @NotNull [] deepCopyObjectArray(@Nullable T @NotNull []
    // original, @NotNull Class<T> componentClass) {
    //    final @Nullable T @NotNull [] clone = Arrays.copyOf(original, original.length);
    //    for (int i = 0; i < clone.length; i++) {
    //      clone[i] = deepCopy(clone[i]);
    //    }
    //    return clone;
    //  }

    /**
     * A default extractor that extract the key, to be used with {@link #mapToArray(Map, Object[],
     * F2)}.
     *
     * @param entry The map entry.
     * @param map The map.
     * @param <K> The key-type.
     * @param <V> The value-type.
     * @return The key.
     */
    public static <K, V> K extractKey(
            final Entry<@NotNull K, @Nullable V> entry, final @NotNull Map<@NotNull K, @Nullable V> map) {
        return entry.getKey();
    }

    /**
     * A default extractor that extract the key, to be used with {@link #mapToArray(Map, Object[],
     * F2)}.
     *
     * @param entry The map entry.
     * @param map The map.
     * @param <K> The key-type.
     * @param <V> The value-type.
     * @return The key.
     */
    public static <K, V> V extractValue(
            final Entry<@NotNull K, @Nullable V> entry, final @NotNull Map<@NotNull K, @Nullable V> map) {
        return entry.getValue();
    }

    private static final class MapEntryCopy<K, V> implements Entry<@NotNull K, @Nullable V> {

        private MapEntryCopy(@NotNull K key, @Nullable V value) {
            this.key = key;
            this.value = value;
        }

        private final @NotNull K key;
        private final @Nullable V value;

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
            throw new UnsupportedOperationException("setValue");
        }
    }

    /**
     * A default extractor that extract the hole entry, to be used with {@link #mapToArray(Map,
     * Object[], F2)}.
     *
     * @param entry The map entry.
     * @param map The map.
     * @param <K> The key-type.
     * @param <V> The value-type.
     * @return The entry.
     */
    public static <K, V> Map.Entry<K, V> extractEntry(
            final Entry<@NotNull K, @Nullable V> entry, final @NotNull Map<@NotNull K, @Nullable V> map) {
        return new MapEntryCopy<>(entry.getKey(), entry.getValue());
    }

    /**
     * Copy elements from the given map into the given array. If the given array is too small,
     * allocate a new array. This method fulfills the contract of {@link
     * Collection#toArray(Object[])}.
     *
     * @param map The map from which to copy.
     * @param original The array into which to copy.
     * @param extractor The method that extracts the element from the map entries.
     * @param <K> The key-type.
     * @param <V> The value-type.
     * @param <E> The element-type to return.
     * @return Either the original array with not used elements set to {@code null} or a new array,
     *     when the original array was too small.
     */
    public static <K, V, E> @NotNull E @NotNull [] mapToArray(
            final @NotNull Map<@NotNull K, @Nullable V> map,
            final E @NotNull [] original,
            final @NotNull F2<E, @NotNull Entry<K, V>, @NotNull Map<@NotNull K, @Nullable V>> extractor) {
        E @NotNull [] array = original;
        final Iterator<Entry<@NotNull K, @Nullable V>> it = map.entrySet().iterator();
        int i = 0;
        while (it.hasNext()) {
            final Entry<@NotNull K, @Nullable V> entry = it.next();
            final E element = extractor.call(entry, map);
            try {
                array[i++] = element;
            } catch (ArrayIndexOutOfBoundsException e) {
                array = Arrays.copyOf(array, Math.max(i + 8, map.size() + 8));
                array[i++] = element;
            }
        }
        if (i < array.length) {
            //noinspection ConstantConditions
            if (array != original) {
                // If we return a copy, always return a copy that matches exactly.
                return Arrays.copyOf(array, i);
            }
            Arrays.fill(array, i, array.length, null);
        }
        return array;
    }
}
