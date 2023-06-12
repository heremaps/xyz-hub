package com.here.xyz.util;

import static com.here.xyz.util.FibMap.ANY;
import static com.here.xyz.util.FibMap.CONFLICT;
import static com.here.xyz.util.FibMap.EMPTY;
import static com.here.xyz.util.FibMap.UNDEFINED;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A map that uses {@link String} key and arbitrary values. The map is thread safe for concurrent access. All keys are deduplicated as
 * intrinsic feature of the map. This reduces the memory consumption when many instance with the same keys are used.
 *
 * @since 2.0.0
 */
@AvailableSince("2.0.0")
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public class JsonMap implements Map<@NotNull String, @Nullable Object>, Iterable<Map.Entry<@NotNull String, @Nullable Object>> {

  /**
   * Create a new empty map.
   *
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public JsonMap() {
    this.root = EMPTY;
    this.size = 0;
  }

  @JsonIgnore
  protected @Nullable Object @NotNull [] root;

  @JsonIgnore
  protected int size;

  @Override
  public int size() {
    return size;
  }

  @JsonIgnore
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * The method used to intern the keys.
   *
   * @param key The arbitrary key.
   * @return The key as {@link String}.
   */
  protected @NotNull Object intern(Object key) {
    return StringCache.intern(StringCache.toCharSequence(key));
  }

  /**
   * Create a conflict result, this just returns {@link FibMap#CONFLICT}. If more details needed, use
   * {@link FibMapConflict#FibMapConflict(Object, Object, Object, Object)}.
   *
   * @param key            The key, that should be modified.
   * @param expected_value The value expected.
   * @param new_value      The value to be set.
   * @param value          The value found (will not be expected).
   * @return the conflict case.
   */
  protected @NotNull Object conflict(
      @NotNull Object key,
      @Nullable Object expected_value,
      @Nullable Object new_value,
      @Nullable Object value) {
    return CONFLICT;
  }

  /**
   * Returns the root. If the root needed for write, then rather directly use {@link #root}.
   *
   * @return The root, prepared for write.
   */
  protected @Nullable Object @NotNull [] rootMutable() {
    @Nullable Object[] root = this.root;
    if (root != EMPTY) {
      return root;
    }
    root = new Object[1 << FibMap.SEGMENT_bits];
    if (ROOT.compareAndSet(this, EMPTY, root)) {
      return root;
    }
    return (@Nullable Object @NotNull []) ROOT.getVolatile(this);
  }

  /**
   * Returns the first key assigned to the given value.
   *
   * @param value The value to search for.
   * @return The found key; if any.
   */
  public final @Nullable String findValue(@Nullable Object value) {
    return findValue(null, value);
  }

  /**
   * Returns the next key assigned to the given value.
   *
   * @param startKey The key to start the search with, ignoring the value of this key.
   * @param value    The value to search for.
   * @return The next key found; if any.
   */
  public @Nullable String findValue(@Nullable String startKey, @Nullable Object value) {
    final FibMapEntry entry = FibMap.searchValue(startKey, value, root, null);
    return entry != null && entry.getKey() instanceof String stringKey ? stringKey : null;
  }

  @Override
  public boolean containsValue(@Nullable Object value) {
    return findValue(value) != null;
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    if (!(key instanceof CharSequence)) {
      return false;
    }
    final CharSequence name = StringCache.toCharSequence(key);
    return FibMap.containsKey(name, root);
  }

  @Override
  public @Nullable Object get(@Nullable Object key) {
    return getOrDefault(key, null);
  }

  /**
   * Returns the value of the given key or the given alternative value, if no such key exists.
   *
   * @param key         The key to lookup.
   * @param alternative The alternative value to return when the key does not exist.
   * @return Either the value of the key (possibly {@code null}) or the given alternative, if no such key exists.
   */
  @Override
  public @Nullable Object getOrDefault(@Nullable Object key, @Nullable Object alternative) {
    if (key instanceof CharSequence name) {
      if (root == EMPTY) {
        return alternative;
      }
      final Object value = FibMap.get(name, root);
      return value == UNDEFINED ? alternative : value;
    }
    return alternative;
  }

  /**
   * Associates the specified value with the specified key in this map. If the map previously contained a mapping for the key, the old value
   * is replaced by the specified value.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return The previous value; if any.
   */
  public @Nullable Object put(@NotNull CharSequence key, @Nullable Object value) {
    if (value == UNDEFINED) {
      return remove(key);
    }
    final Object old = FibMap.put(key, ANY, value, true, rootMutable(), this::intern, this::conflict);
    if (old == UNDEFINED) {
      SIZE.getAndAdd(this, 1);
      return null;
    }
    return old;
  }

  @JsonAnySetter
  @Override
  public final @Nullable Object put(@NotNull String key, @Nullable Object value) {
    return put((CharSequence) key, value);
  }

  @Override
  public final @Nullable Object putIfAbsent(@NotNull String key, @Nullable Object value) {
    return putIfAbsent((CharSequence) key, value);
  }

  public @Nullable Object putIfAbsent(@NotNull CharSequence key, @Nullable Object value) {
    if (value == UNDEFINED || value == ANY) {
      throw new IllegalArgumentException("value must not be UNDEFINED or ANY");
    }
    final Object old = FibMap.put(key, UNDEFINED, value, true, rootMutable(), this::intern, this::conflict);
    if (old == UNDEFINED) {
      SIZE.getAndAdd(this, -1);
      return null;
    }
    if (old instanceof FibMapConflict conflict) {
      return conflict.value();
    }
    // This must not happen!
    assert false;
    return null;
  }

  @Override
  public @Nullable Object computeIfAbsent(@NotNull String key, @NotNull Function<? super String, ?> fn) {
    Object oldValue = FibMap.get(key, root);
    if (oldValue == UNDEFINED) {
      final Object newValue = fn.apply(key);
      if (newValue != null && newValue != UNDEFINED) {
        oldValue = FibMap.put(key, UNDEFINED, newValue, true, rootMutable(), this::intern, this::conflict);
        if (oldValue instanceof FibMapConflict conflict) {
          return conflict.value();
        }
        assert oldValue == UNDEFINED;
        SIZE.getAndAdd(this, 1);
        return newValue;
      }
      return null;
    }
    return oldValue;
  }

  @Override
  public @Nullable Object computeIfPresent(@NotNull String key, @NotNull BiFunction<? super String, ? super Object, ?> fn) {
    // TODO: Fix for fields
    while (true) {
      Object oldValue = FibMap.get(key, root);
      if (oldValue == null || oldValue == UNDEFINED) {
        return null;
      }
      Object newValue = fn.apply(key, oldValue);
      if (newValue == null) {
        newValue = UNDEFINED;
      }
      final Object result = FibMap.put(key, oldValue, newValue, true, rootMutable(), this::intern, this::conflict);
      if (result instanceof FibMapConflict) {
        continue;
      }
      assert result == oldValue;
      if (newValue == UNDEFINED) {
        SIZE.getAndAdd(this, -1);
        return null;
      }
      return newValue;
    }
  }

  @Override
  public @Nullable Object compute(@NotNull String key, BiFunction<? super String, ? super Object, ?> fn) {
    // TODO: Fix for fields
    while (true) {
      final Object original = FibMap.get(key, root);
      Object oldValue = original == UNDEFINED ? null : original;
      Object newValue = fn.apply(key, oldValue);
      if (newValue == null || newValue == UNDEFINED) {
        if (original == UNDEFINED) {
          return null;
        }
        newValue = UNDEFINED;
      }
      final Object result = FibMap.put(key, original, newValue, true, rootMutable(), this::intern, this::conflict);
      if (result instanceof FibMapConflict) {
        continue;
      }
      assert result == original;
      if (newValue == UNDEFINED) {
        SIZE.getAndAdd(this, -1);
        return null;
      } else if (original == UNDEFINED) {
        SIZE.getAndAdd(this, 1);
      }
      return newValue;
    }
  }

  @Override
  public @Nullable Object merge(
      final @NotNull String key,
      final @Nullable Object value,
      final @NotNull BiFunction<? super Object, ? super Object, ?> fn
  ) {
    // TODO: Fix for fields
    while (true) {
      final Object original = FibMap.get(key, root);
      Object oldValue = original == UNDEFINED ? null : original;
      Object newValue = value;
      if (oldValue != null) {
        newValue = fn.apply(oldValue, value);
      }
      if (newValue == null || newValue == UNDEFINED) {
        if (original == UNDEFINED) {
          return null;
        }
        // Remove the key.
        final Object result = FibMap.put(key, original, UNDEFINED, true, rootMutable(), this::intern, this::conflict);
        if (result instanceof FibMapConflict) {
          continue;
        }
        assert result == UNDEFINED;
        SIZE.getAndAdd(this, -1);
        return null;
      }

      // Set or replace the key.
      final Object result = FibMap.put(key, original, newValue, true, rootMutable(), this::intern, this::conflict);
      if (result instanceof FibMapConflict) {
        continue;
      }
      assert result == original;
      if (original == UNDEFINED) {
        // We added the key.
        SIZE.getAndAdd(this, 1);
      }
      return newValue;
    }
  }

  @Override
  public final boolean replace(@NotNull String key, @Nullable Object oldValue, @Nullable Object newValue) {
    return replace((CharSequence) key, oldValue, newValue);
  }

  /**
   * Replace the given key with the given value, fail if the current value is not the given old value.
   *
   * @param key           The key to update.
   * @param expectedValue The expected old value, may be {@link FibMap#UNDEFINED} if the key should be created.
   * @param newValue      The new value to assign.
   * @return {@code true} if the operation succeeded; {@code false} otherwise.
   */
  public boolean replace(@NotNull CharSequence key, @Nullable Object expectedValue, @Nullable Object newValue) {
    // TODO: Fix for fields
    if (newValue == ANY) {
      throw new IllegalArgumentException("newValue must not be ANY");
    }
    final Object oldValue = FibMap.put(key, expectedValue, newValue, true, rootMutable(), this::intern, this::conflict);
    if (!(oldValue instanceof FibMapConflict)) {
      if (oldValue == UNDEFINED && newValue != UNDEFINED) {
        // Added the key.
        SIZE.getAndAdd(this, 1);
      } else if (oldValue != UNDEFINED && newValue == UNDEFINED) {
        // Removed the key.
        SIZE.getAndAdd(this, -1);
      }
      return true;
    }
    return false;
  }

  @Override
  public final @Nullable Object replace(@NotNull String key, @Nullable Object value) {
    return replace((CharSequence) key, value);
  }

  /**
   * Replaces the entry for the specified key only if it is currently mapped to some value.
   *
   * @param key      The key with which the specified value is associated.
   * @param newValue The value to be associated with the specified key.
   * @return The previous value associated with the specified key, or {@code null} if there was no mapping for the key. A {@code null}
   * return can also indicate that the map previously associated {@code null} with the key, if the implementation supports null values.
   */
  public @Nullable Object replace(@NotNull CharSequence key, @Nullable Object newValue) {
    // TODO: Fix for fields
    final Object oldValue = FibMap.put(key, ANY, newValue, false, rootMutable(), this::intern, this::conflict);
    assert oldValue != UNDEFINED;
    // Note: A conflict should only happen, when the value is undefined, therefore the value in a conflict case must be UNDEFINED!
    return oldValue instanceof FibMapConflict ? null : oldValue;
  }

  @Override
  public final @Nullable Object remove(@Nullable Object key) {
    return removeOrDefault(key, null);
  }

  /**
   * Remove the given key and return the previous value or the given alternative, if no such key exists.
   *
   * @param key         The key to remove.
   * @param alternative The alternative value to return when the key does not exist.
   * @return The previous value or the alternative value, when the key does not exist.
   */
  public @Nullable Object removeOrDefault(@Nullable Object key, @Nullable Object alternative) {
    if (!(key instanceof CharSequence)) {
      return alternative;
    }
    final CharSequence name = StringCache.toCharSequence(key);
    if (root == EMPTY) {
      return alternative;
    }
    final Object old = FibMap.put(name, ANY, UNDEFINED, true, root, this::intern, this::conflict);
    if (old != UNDEFINED) {
      SIZE.getAndAdd(this, -1);
      return old;
    }
    return alternative;
  }

  @Override
  public final boolean remove(@NotNull Object key, @Nullable Object value) {
    return removeOrDefault(key, UNDEFINED) != UNDEFINED;
  }

  @Override
  public void putAll(@NotNull Map<? extends @NotNull String, ?> m) {
    for (final Map.Entry<? extends @NotNull String, ?> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    while (true) {
      final int oldSize = size;
      if (ROOT.compareAndSet(this, root, EMPTY)) {
        SIZE.getAndAdd(this, -oldSize);
        return;
      }
    }
  }

  @Override
  public @NotNull Iterator<Entry<@NotNull String, @Nullable Object>> iterator() {
    return new JsonMapEntryIterator(this);
  }

  private MapKeySet<@NotNull String, Object> keySet;

  @Override
  public @NotNull Set<@NotNull String> keySet() {
    MapKeySet<@NotNull String, Object> keySet = this.keySet;
    if (keySet == null) {
      this.keySet = keySet = new MapKeySet<>(this);
    }
    return keySet;
  }

  private MapValueCollection<@NotNull String, Object> valCol;

  @Override
  public @NotNull Collection<@Nullable Object> values() {
    MapValueCollection<@NotNull String, Object> valCol = this.valCol;
    if (valCol == null) {
      this.valCol = valCol = new MapValueCollection<>(this);
    }
    return valCol;
  }

  @JsonIgnore
  private @Nullable JsonMapEntrySet entrySet;

  @Override
  public @NotNull Set<@NotNull Entry<@NotNull String, @Nullable Object>> entrySet() {
    JsonMapEntrySet entrySet = this.entrySet;
    if (entrySet == null) {
      this.entrySet = entrySet = new JsonMapEntrySet(this);
    }
    return entrySet;
  }

  protected static final @NotNull VarHandle ROOT;
  protected static final @NotNull VarHandle SIZE;
  protected static final @NotNull VarHandle ARRAY;

  static {
    try {
      ROOT = MethodHandles.lookup().in(JsonMap.class).findVarHandle(JsonMap.class, "root", Object[].class);
      SIZE = MethodHandles.lookup().in(JsonMap.class).findVarHandle(JsonMap.class, "size", int.class);
      ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}