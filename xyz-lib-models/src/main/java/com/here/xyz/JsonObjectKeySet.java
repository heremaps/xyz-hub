package com.here.xyz;

import com.here.xyz.util.JsonClass;
import com.here.xyz.util.JsonField;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The key-set of all members. This key-set covers the fixed Java members and the dynamic extensions properties.
 */
@SuppressWarnings({"NullableProblems", "unchecked"})
public class JsonObjectKeySet implements Set<@NotNull String> {

  JsonObjectKeySet(final @NotNull JsonObject jsonObject) {
    this.jsonObject = jsonObject;
    jsonClass = JsonClass.of(jsonObject.getClass());
  }

  private final @NotNull JsonObject jsonObject;
  private final @NotNull JsonClass jsonClass;

  private @Nullable Map<@NotNull String, @Nullable Object> mapOrNull() {
    return jsonObject.additionalProperties;
  }

  private @NotNull Map<@NotNull String, @Nullable Object> map() {
    return jsonObject.getAdditionalProperties();
  }

  @Override
  public int size() {
    return jsonObject.size();
  }

  @Override
  public boolean isEmpty() {
    return jsonObject.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof final String key) {
      return jsonClass.hasField(key) || (mapOrNull() != null && mapOrNull().containsKey(key));
    }
    return false;
  }

  @Nonnull
  @Override
  public Iterator<@NotNull String> iterator() {
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    final Iterator<@NotNull String> keysIt = map != null ? map.keySet().iterator() : null;
    return new Iterator<>() {
      int i;

      @Override
      public boolean hasNext() {
        return i < jsonClass.fields.length || (keysIt != null && keysIt.hasNext());
      }

      @Override
      public String next() {
        if (i < jsonClass.fields.length) {
          return jsonClass.fields[i++].jsonName;
        }
        if (keysIt != null) {
          return keysIt.next();
        }
        throw new NoSuchElementException();
      }
    };
  }

  private static final @NotNull Object @NotNull [] EMPTY_OBJECT_ARRAY = new Object[0];

  @Override
  public @NotNull Object @NotNull [] toArray() {
    // Note: We know that the returned array does not contain null values!
    return toArray(EMPTY_OBJECT_ARRAY);
  }

  @Override
  public <T> @Nullable T @NotNull [] toArray(@Nullable T @NotNull [] array) {
    final Class<?> componentType = array.getClass().getComponentType();
    final int size = size();
    if (componentType == String.class) {
      if (array.length < size) {
        array = (T[]) new String[size];
      }
    } else if (componentType == CharSequence.class) {
      array = (T[]) new CharSequence[size];
    } else if (componentType == Object.class) {
      array = (T[]) new Object[size];
    } else {
      throw new IllegalArgumentException("Unsupported target type: " + componentType.getName());
    }
    assert jsonClass.size() <= array.length;
    int i = 0;
    while (i < jsonClass.size()) {
      final JsonField jsonField = jsonClass.getField(i);
      array[i++] = (T) jsonField.jsonName;
    }
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    if (map != null) {
      for (final @NotNull String key : map.keySet()) {
        array[i++] = (T) key;
      }
    }
    while (i < array.length) {
      array[i++] = null;
    }
    return array;
  }

  @Override
  public boolean add(@NotNull String s) {
    if (jsonClass.hasField(s)) {
      return false;
    }
    final Map<@NotNull String, @Nullable Object> map = map();
    if (map.containsKey(s)) {
      return false;
    }
    map.put(s, null);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    if (!(o instanceof final String key)) {
      return false;
    }
    if (jsonClass.hasField(key)) {
      final JsonField field = jsonClass.getField(key);
      assert field != null;
      if (!field.isFinal && field.isNullable) {
        field.setValue(this, null);
        return true;
      }
      return false;
    }
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    if (map != null && map.containsKey(key)) {
      map.remove(key);
      return true;
    }
    return false;
  }

  @Override
  public boolean containsAll(@Nonnull Collection<?> c) {
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    for (final Object o : c) {
      if (!(o instanceof final String key)) {
        return false;
      }
      if (!jsonClass.hasField(key) && map != null && !map.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends @NotNull String> c) {
    boolean modified = false;
    for (final @NotNull String o : c) {
      if (add(o)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    boolean modified = false;
    final HashMap<@NotNull String, @NotNull Boolean> retain = new HashMap<>();
    for (final Object o : c) {
      if (!(o instanceof String key)) {
        continue;
      }
      retain.put(key, Boolean.TRUE);
    }
    for (final JsonField field : jsonClass.fields) {
      if (!field.isFinal && field.isNullable && !retain.containsKey(field.jsonName)) {
        field.setValue(jsonObject, null);
        modified = true;
      }
    }
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    if (map != null) {
      for (final String key : map.keySet()) {
        if (!retain.containsKey(key)) {
          map.remove(key);
          modified = true;
        }
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    boolean modified = false;
    final JsonClass jsonClass = JsonClass.of(this);
    final Map<@NotNull String, @Nullable Object> map = mapOrNull();
    for (final Object o : c) {
      if (!(o instanceof String key)) {
        continue;
      }
      final JsonField jsonField = jsonClass.getField(key);
      if (jsonField != null) {
        if (!jsonField.isFinal && jsonField.isNullable) {
          jsonField.setValue(jsonObject, null);
          modified = true;
        }
      } else if (map != null && map.containsKey(key)) {
        map.remove(key);
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public void clear() {
    jsonObject.additionalProperties = null;
    final JsonClass jsonClass = JsonClass.of(this);
    for (final @NotNull JsonField field : jsonClass.fields) {
      if (!field.isFinal && field.isNullable) {
        field.setValue(jsonObject, null);
      }
    }
  }
}
