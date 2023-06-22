package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.util.FibMap.ANY;
import static com.here.naksha.lib.core.util.FibMap.CONFLICT;
import static com.here.naksha.lib.core.util.FibMap.UNDEFINED;
import static com.here.naksha.lib.core.util.FibMap.VOID;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.util.FibMap;
import com.here.naksha.lib.core.util.StringHelper;
import com.here.naksha.lib.core.util.diff.Patcher;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A merge between a POJO and a hash-map, so fields are exposed next to arbitrary additional
 * properties. In other words, this is a hash-map that can be extended with native POJO members. In
 * that case all Java fields annotated with {@link JsonProperty}, being {@code public} or {@code
 * protected} and <b>not</b> annotated with {@link JsonIgnore} are made available as hash-map
 * key-value pairs. This is necessary for the {@link Patcher} to work. Note that the map will
 * reflect the properties by their annotated name, if annotated differently from the name in the
 * source code.
 *
 * <p><b>WARNING</b>: A problem arises when directly accessing additional properties and adding
 * properties that have the same name as the native fields. Modification of properties should only
 * be done through the exposed Map interface or by directly accessing the native properties, never
 * using the {@link #additionalProperties() additional properties map}!
 *
 * @since 2.0.0
 */
@AvailableSince("2.0.0")
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public class JsonObject
    implements Map<@NotNull String, @Nullable Object>,
        Iterable<Map.Entry<@NotNull String, @Nullable Object>>,
        JsonSerializable {

  /** Create a new empty object. */
  @AvailableSince("2.0.0")
  public JsonObject() {
    additionalProperties = new JsonMap();
  }

  /** The map storing the additional properties. */
  @JsonIgnore
  final @NotNull JsonMap additionalProperties;

  /** A map that stores, which fields where virtually removed. */
  @JsonIgnore
  private long @Nullable [] removed;

  private static final long[] EMPTY_REMOVED = new long[0];

  /**
   * Returns the removed array.
   *
   * @return the removed array.
   */
  protected final long @NotNull [] removed() {
    long[] removed = this.removed;
    if (removed == null) {
      final int bits = getJsonClass().size();
      if (bits == 0) {
        // This is idempotent, we do not care how often this is executed concurrently.
        return this.removed = EMPTY_REMOVED;
      }
      removed = new long[(bits + 63) >>> 6];
      if (REMOVED.compareAndSet(this, null, removed)) {
        return removed;
      }
      // Conflict, some other thread was faster in setting the removed, we know it is only set ones
      //           so lets read it volatile and return it.
      return (long[]) REMOVED.getVolatile(this);
    }
    return removed;
  }

  protected boolean isUndefined(@NotNull JsonField<?, ?> field) {
    final long[] removed = removed();
    final int index = field.index;
    final int i = index >>> 6;
    final long value = 1L << (index & 63);
    // final long mask = ~value;
    // ---------------------------------------------------------
    final long bits = removed[i];
    return (bits & value) == value;
  }

  protected void undefineField(@NotNull JsonField<?, ?> field) {
    final long[] removed = removed();
    final int index = field.index;
    final int i = index >>> 6;
    final long value = 1L << (index & 63);
    final long mask = ~value;
    // ---------------------------------------------------------
    long bits, new_bits;
    do {
      bits = removed[i];
      new_bits = (bits & mask) | value;
    } while ((bits & value) == 0L && !LONG_ARRAY.compareAndSet(removed, i, bits, new_bits));
  }

  protected void defineField(@NotNull JsonField<?, ?> field) {
    final long[] removed = removed();
    final int index = field.index;
    final int i = index >>> 6;
    final long value = 1L << (index & 63);
    final long mask = ~value;
    // ---------------------------------------------------------
    long bits, new_bits;
    do {
      bits = removed[i];
      new_bits = bits & mask;
    } while ((bits & value) == value && !LONG_ARRAY.compareAndSet(removed, i, bits, new_bits));
  }

  /** The cached JSON class reference. */
  @JsonIgnore
  private @Nullable JsonClass<?> jsonClass;

  /**
   * Returns the JSON class of this object.
   *
   * @return the JSON class of this object.
   */
  @JsonIgnore
  public final @NotNull JsonClass<?> getJsonClass() {
    final JsonClass<?> jsonClass = this.jsonClass;
    if (jsonClass == null) {
      // Note: This operation is idempotent, so we don't care about concurrency.
      return this.jsonClass = JsonClass.of(getClass());
    }
    return jsonClass;
  }

  /**
   * Returns only the additional properties not being JAVA fields.
   *
   * <p><b>WARNUNG</b>: Do not use this to modify fields, this is mainly for Jackson to serialize
   * the object! If the content of this map is modified this can lead to severe problems, like
   * duplicate fields with different content.
   *
   * @return the additional properties not being JAVA fields.
   */
  @JsonAnyGetter
  public @NotNull Map<@NotNull String, @Nullable Object> additionalProperties() {
    return additionalProperties;
  }

  @Override
  public int size() {
    int fieldCount = getJsonClass().size();
    final long[] removed = removed();
    for (final long bits : removed) {
      fieldCount -= Long.bitCount(bits);
    }
    return additionalProperties.size() + fieldCount;
  }

  @JsonIgnore
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Returns the current value of the field.
   *
   * @param field the field to read.
   * @return the value of the field, {@link FibMap#UNDEFINED} if no such field exists.
   */
  protected @Nullable Object getFieldValue(@NotNull JsonField<?, ?> field) {
    return isUndefined(field) ? UNDEFINED : field.get(this);
  }

  /**
   * Assigns the given value to the given field. If the value {@link FibMap#UNDEFINED} is given,
   * virtually removing the field.
   *
   * <p>Note that, due to the nature of Java fields, which can't be really removed, we emulate field
   * removal and this is not really totally atomic.
   *
   * @param field the field to set.
   * @param expected_value the existing value that is expected for the operation to succeed; {@link
   *     FibMap#ANY} if any value is okay.
   * @param new_value the value to set; if {@link FibMap#UNDEFINED} removing the field.
   * @param create whether, if no such field exists yet, creating the field ({@code true} or {@code
   *     false}).
   * @return the previously assigned value, {@link FibMap#UNDEFINED}, if the field did not exist and
   *     {@link FibMap#CONFLICT} if the operation failed due to a conflicting situation.
   * @throws NullPointerException if any of the required parameters is {@code null}.
   * @throws IllegalArgumentException if the given array is empty (length of zero), the new value is
   *     {@link FibMap#CONFLICT} or {@link FibMap#ANY}, or the expected value is {@link
   *     FibMap#CONFLICT}.
   */
  protected @Nullable Object setFieldValue(
      @NotNull JsonField<?, ?> field,
      @Nullable Object expected_value,
      @Nullable Object new_value,
      boolean create) {
    if (expected_value == CONFLICT) {
      throw new IllegalArgumentException("expected_value must not be CONFLICT");
    }
    if (new_value == ANY || new_value == VOID || new_value == CONFLICT) {
      throw new IllegalArgumentException("new_value must not be ANY, VOID or CONFLICT");
    }

    final Object value = getFieldValue(field);

    // Field does not exist.
    if (value == UNDEFINED) {
      if (new_value == UNDEFINED) {
        return UNDEFINED;
      }
      if (create
          && (expected_value == UNDEFINED || expected_value == VOID || expected_value == ANY)
          && field.compareAndSwap(this, null, new_value)) {
        defineField(field);
        return UNDEFINED;
      }
      return CONFLICT;
    }

    // Field exists.
    if (expected_value == UNDEFINED) {
      return CONFLICT;
    }
    if (new_value == UNDEFINED) {
      // Remove the field.
      if (expected_value == ANY) {
        field.set(this, null);
        undefineField(field);
        return value;
      }
      if (field.compareAndSwap(this, expected_value == VOID ? null : expected_value, null)) {
        undefineField(field);
        return expected_value == VOID ? null : expected_value;
      }
      return CONFLICT;
    }
    // Set the value.
    if (expected_value == ANY) {
      return field.set(this, new_value);
    }
    // Set the value, if it matches the expected value.
    if (field.compareAndSwap(this, expected_value == VOID ? null : expected_value, new_value)) {
      return expected_value == VOID ? null : expected_value;
    }
    return CONFLICT;
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
   * @param value The value to search for.
   * @return The next key found; if any.
   */
  public @Nullable String findValue(@Nullable String startKey, @Nullable Object value) {
    final JsonClass<?> jsonClass = getJsonClass();
    for (final @NotNull JsonField<?, ?> field : jsonClass.fields) {
      if (startKey != null) {
        if (startKey.equals(field.jsonName)) {
          startKey = null;
        }
        continue;
      }
      final Object fieldValue = getFieldValue(field);
      if (fieldValue != UNDEFINED && Objects.equals(value, fieldValue)) {
        return field.jsonName;
      }
    }
    return additionalProperties.findValue(startKey, value);
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
    final CharSequence name = StringHelper.toCharSequence(key);
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(name);
    if (field != null) {
      return getFieldValue(field) != UNDEFINED;
    }
    return additionalProperties.containsKey(key);
  }

  @Override
  public @Nullable Object get(@Nullable Object key) {
    return getOrDefault(key, null);
  }

  /**
   * Returns the value of the given key or the given alternative value, if no such key exists.
   *
   * @param key The key to lookup.
   * @param alternative The alternative value to return when the key does not exist.
   * @return Either the value of the key (possibly {@code null}) or the given alternative, if no
   *     such key exists.
   */
  @Override
  public @Nullable Object getOrDefault(@Nullable Object key, @Nullable Object alternative) {
    if (key instanceof CharSequence name) {
      final JsonClass<?> jsonClass = getJsonClass();
      final JsonField<?, ?> field = jsonClass.getField(name);
      if (field != null) {
        final Object oldValue = getFieldValue(field);
        return oldValue == UNDEFINED ? alternative : oldValue;
      }
      return additionalProperties.getOrDefault(key, alternative);
    }
    return alternative;
  }

  /**
   * Associates the specified value with the specified key in this map. If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return The previous value; if any.
   */
  public @Nullable Object put(@NotNull CharSequence key, @Nullable Object value) {
    if (value == UNDEFINED) {
      return remove(key);
    }
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      final Object oldValue = setFieldValue(field, ANY, value, true);
      return oldValue == UNDEFINED ? null : oldValue;
    }
    return additionalProperties.put(key, value);
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
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      final Object oldValue = getFieldValue(field);
      if (oldValue == UNDEFINED || field.isNullable && oldValue == null) {
        setFieldValue(field, ANY, value, true);
        return null;
      }
      return oldValue;
    }
    return additionalProperties.putIfAbsent(key, value);
  }

  @Override
  public @Nullable Object computeIfAbsent(@NotNull String key, @NotNull Function<? super String, ?> fn) {
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      Object oldValue = getFieldValue(field);
      if (oldValue == UNDEFINED || oldValue == null) {
        final Object newValue = fn.apply(key);
        if (newValue != UNDEFINED && newValue != null) {
          oldValue = setFieldValue(field, ANY, newValue, true);
          return oldValue;
        }
        return null;
      }
      return oldValue;
    }
    return additionalProperties.computeIfAbsent(key, fn);
  }

  @Override
  public @Nullable Object computeIfPresent(
      @NotNull String key, @NotNull BiFunction<? super String, ? super Object, ?> fn) {
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      Object oldValue = getFieldValue(field);
      if (oldValue == UNDEFINED || oldValue == null) {
        return null;
      }
      final Object newValue = fn.apply(key, oldValue);
      if (newValue != null && newValue != UNDEFINED) {
        setFieldValue(field, ANY, newValue, true);
        return newValue;
      }
      setFieldValue(field, ANY, UNDEFINED, false);
      return null;
    }
    return additionalProperties.computeIfPresent(key, fn);
  }

  @Override
  public @Nullable Object compute(@NotNull String key, @NotNull BiFunction<? super String, ? super Object, ?> fn) {
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      final Object oldValue = getFieldValue(field);
      final Object newValue = fn.apply(key, oldValue == UNDEFINED ? null : oldValue);
      if (newValue != null && newValue != UNDEFINED) {
        setFieldValue(field, ANY, newValue, true);
        return newValue;
      }
      setFieldValue(field, ANY, UNDEFINED, false);
      return null;
    }
    return additionalProperties.compute(key, fn);
  }

  @Override
  public @Nullable Object merge(
      final @NotNull String key,
      final @Nullable Object value,
      final @NotNull BiFunction<? super Object, ? super Object, ?> fn) {
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      final Object oldValue = getFieldValue(field);
      if (oldValue == UNDEFINED || oldValue == null) {
        setFieldValue(field, ANY, value, true);
        return value;
      }
      final Object newValue = fn.apply(value, oldValue);
      if (newValue != null && newValue != UNDEFINED) {
        setFieldValue(field, ANY, newValue, true);
        return newValue;
      }
      setFieldValue(field, ANY, UNDEFINED, false);
      return null;
    }
    return additionalProperties.merge(key, value, fn);
  }

  @Override
  public final boolean replace(@NotNull String key, @Nullable Object oldValue, @Nullable Object newValue) {
    return replace((CharSequence) key, oldValue, newValue);
  }

  /**
   * Replace the given key with the given value, fail if the current value is not the given old
   * value.
   *
   * @param key The key to update.
   * @param expectedValue The expected old value, may be {@link FibMap#UNDEFINED} if the key should
   *     be created.
   * @param newValue The new value to assign.
   * @return {@code true} if the operation succeeded; {@code false} otherwise.
   */
  public boolean replace(@NotNull CharSequence key, @Nullable Object expectedValue, @Nullable Object newValue) {
    if (newValue == ANY) {
      throw new IllegalArgumentException("newValue must not be ANY");
    }
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(key);
    if (field != null) {
      return setFieldValue(field, expectedValue == null ? VOID : expectedValue, newValue, true) != CONFLICT;
    }
    return additionalProperties.replace(key, expectedValue, newValue);
  }

  @Override
  public final @Nullable Object replace(@NotNull String key, @Nullable Object value) {
    return replace((CharSequence) key, value);
  }

  /**
   * Replaces the entry for the specified key only if it is currently mapped to some value.
   *
   * @param key The key with which the specified value is associated.
   * @param newValue The value to be associated with the specified key.
   * @return The previous value associated with the specified key, or {@code null} if there was no
   *     mapping for the key. A {@code null} return can also indicate that the map previously
   *     associated {@code null} with the key, if the implementation supports null values.
   */
  public @Nullable Object replace(@NotNull CharSequence key, @Nullable Object newValue) {
    return replace(key, ANY, newValue);
  }

  @Override
  public final @Nullable Object remove(@Nullable Object key) {
    return removeOrDefault(key, null);
  }

  /**
   * Remove the given key and return the previous value or the given alternative, if no such key
   * exists.
   *
   * @param key The key to remove.
   * @param alternative The alternative value to return when the key does not exist.
   * @return The previous value or the alternative value, when the key does not exist.
   */
  public @Nullable Object removeOrDefault(@Nullable Object key, @Nullable Object alternative) {
    if (!(key instanceof CharSequence)) {
      return alternative;
    }
    final CharSequence name = StringHelper.toCharSequence(key);
    final JsonClass<?> jsonClass = getJsonClass();
    final JsonField<?, ?> field = jsonClass.getField(name);
    if (field != null) {
      final Object value = setFieldValue(field, ANY, UNDEFINED, false);
      return value == UNDEFINED ? alternative : value;
    }
    return additionalProperties.removeOrDefault(key, alternative);
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
    additionalProperties.clear();
    final JsonClass<?> jsonClass = getJsonClass();
    for (final JsonField<?, ?> field : jsonClass.fields) {
      field.set(this, null);
      undefineField(field);
    }
  }

  @Override
  public @NotNull JsonObjectEntryIterator iterator() {
    return new JsonObjectEntryIterator(this);
  }

  @JsonIgnore
  private MapKeySet<@NotNull String, Object> keySet;

  @Override
  public @NotNull Set<@NotNull String> keySet() {
    MapKeySet<@NotNull String, Object> keySet = this.keySet;
    if (keySet == null) {
      this.keySet = keySet = new MapKeySet<>(this);
    }
    return keySet;
  }

  @JsonIgnore
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
  private @Nullable JsonObjectEntrySet entrySet;

  @Override
  public @NotNull Set<@NotNull Entry<@NotNull String, @Nullable Object>> entrySet() {
    JsonObjectEntrySet entrySet = this.entrySet;
    if (entrySet == null) {
      this.entrySet = entrySet = new JsonObjectEntrySet(this);
    }
    return entrySet;
  }

  protected static final @NotNull VarHandle REMOVED;
  protected static final @NotNull VarHandle LONG_ARRAY;

  static {
    try {
      REMOVED = MethodHandles.lookup()
          .in(JsonObject.class)
          .findVarHandle(JsonObject.class, "removed", long[].class);
      LONG_ARRAY = MethodHandles.arrayElementVarHandle(long[].class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
