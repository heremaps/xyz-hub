package com.here.xyz.util.json;

import static com.here.xyz.util.FibMap.EMPTY;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.ILike;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract core implementation of an entry-set.
 */
public abstract class MapEntrySet<K, V, M extends Map<@NotNull K, V>> implements Set<@NotNull Entry<@NotNull K, @Nullable V>> {

  /**
   * Creates a new {@link Set} above the given {@link JsonMap}. Note that creating an iterator above an empty
   *
   * @param map The {@link JsonMap} to create an entry-set for.
   */
  MapEntrySet(@NotNull M map, @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
    this.map = map;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @JsonIgnore
  protected final @NotNull M map;

  @JsonIgnore
  protected final @NotNull Class<K> keyClass;

  @JsonIgnore
  protected final @NotNull Class<V> valueClass;

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof Map.Entry<?, ?> entry) {
      final Object raw_key = entry.getKey();
      final Object value = entry.getValue();
      if (!keyClass.isInstance(raw_key)) { // or raw_key == null!
        return false;
      }
      final K key = keyClass.cast(raw_key);
      return map.containsKey(key) && ILike.equals(value, map.get(key));
    }
    return false;
  }

  @Override
  public @Nullable Object @NotNull [] toArray() {
    return toArray(EMPTY);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable T @NotNull [] toArray(@Nullable T @NotNull [] a) {
    return (T[]) JsonUtils.mapToArray(map, a, (e, m) -> new MapEntry<>(map, e.getKey(), e.getValue()));
  }

  @Override
  public boolean add(@NotNull Entry<@NotNull K, @Nullable V> entry) {
    map.put(entry.getKey(), entry.getValue());
    return true;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof Map.Entry<?, ?> entry && entry.getKey() instanceof CharSequence key) {
      map.remove(key);
      return true;
    }
    return false;
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    for (Object e : c) {
      if (e instanceof Map.Entry<?, ?> entry) {
        final Object raw_key = entry.getKey();
        if (!keyClass.isInstance(raw_key)) {
          return false;
        }
        final K key = keyClass.cast(raw_key);
        if (!map.containsKey(key) || !ILike.equals(entry.getValue(), map.get(key))) {
          return false;
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public boolean addAll(@Nullable Collection<? extends Entry<@NotNull K, @Nullable V>> c) {
    boolean modified = false;
    if (c != null) {
      for (final @Nullable Entry<@NotNull K, @Nullable V> entry : c) {
        if (entry != null) {
          map.put(entry.getKey(), entry.getValue());
          modified = true;
        }
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException("retainAll");
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    boolean modified = false;
    for (final Object o : c) {
      if (o instanceof Map.Entry<?, ?> entry) {
        final Object raw_key = entry.getKey();
        final Object raw_value = entry.getValue();
        if (!keyClass.isInstance(raw_key)) {
          continue;
        }
        if (raw_value != null && !valueClass.isInstance(raw_value)) {
          continue;
        }
        final K key = keyClass.cast(raw_key);
        final V value = valueClass.cast(raw_value);
        if (map.remove(key, value)) {
          modified = true;
        }
      }
    }
    return modified;
  }

  @Override
  public void clear() {
    map.clear();
  }
}
