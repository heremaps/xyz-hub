/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.util.JsonClass;
import com.here.xyz.util.JsonField;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonInclude(Include.NON_NULL)
public abstract class Extensible<SELF extends Extensible<SELF>> implements XyzSerializable {

  /**
   * Returns this.
   *
   * @return this.
   */
  @SuppressWarnings("unchecked")
  protected final @NotNull SELF self() {
    return (SELF) this;
  }

  @JsonIgnore
  private @Nullable Map<@NotNull String, @Nullable Object> additionalProperties;

  @JsonAnyGetter
  protected final @Nullable Map<@NotNull String, @Nullable Object> jsonAnyGetter() {
    return additionalProperties;
  }

  /**
   * Returns the value of the property with the given JSON name.
   *
   * @param name The name of the properties.
   * @param <V>  The value type.
   * @return The value; {@code null} if no such property exists.
   */
  @SuppressWarnings("unchecked")
  public <V> @Nullable V get(@NotNull String name) {
    final JsonClass jsonClass = JsonClass.of(getClass());
    if (jsonClass.hasField(name)) {
      final JsonField jsonField = jsonClass.getField(name);
      assert jsonField != null;
      return jsonField.getValue(this);
    }
    if (additionalProperties != null && additionalProperties.containsKey(name)) {
      return (V) additionalProperties.get(name);
    }
    return null;
  }

  /**
   * Tests whether this Extensible has a property with the given name.
   *
   * @param name The name of the property to lookup.
   * @return {@code true} if such a property exists; {@code false} if no such property exists.
   */
  public boolean has(@NotNull String name) {
    if (additionalProperties != null && additionalProperties.containsKey(name)) {
      return true;
    }
    final JsonClass jsonClass = JsonClass.of(getClass());
    return jsonClass.hasField(name);
  }

  /**
   * Sets the property with the given name to the given value. If no such property exists, adds it.
   *
   * @param name  The name of the property.
   * @param value The value to set.
   * @return The previous value.
   * @throws ClassCastException       If the returned value is of an unexpected type, or the field value does not match the given value
   *                                  type.
   * @throws IllegalArgumentException If the given value is of the wrong type.
   */
  @JsonAnySetter
  public <V> V put(@NotNull String name, @Nullable Object value) {
    final JsonClass jsonClass = JsonClass.of(getClass());
    if (jsonClass.hasField(name)) {
      final JsonField field = jsonClass.getField(name);
      assert field != null;
      return field.setValue(this, null);
    }
    //noinspection unchecked
    return (V) additionalProperties().put(name, value);
  }

  /**
   * Returns a map of all additional properties, so properties not know to JAVA.
   *
   * @return A map of all additional properties, so properties not know to JAVA.
   */
  public final @NotNull Map<@NotNull String, @Nullable Object> additionalProperties() {
    if (this.additionalProperties == null) {
      this.additionalProperties = new LinkedHashMap<>();
    }
    return additionalProperties;
  }

  @SuppressWarnings("unchecked")
  public @NotNull SELF with(@NotNull String key, @Nullable Object value) {
    put(key, value);
    return (SELF) this;
  }

  @SuppressWarnings("unchecked")
  public @NotNull SELF remove(@NotNull String key) {
    if (additionalProperties != null) {
      additionalProperties.remove(key);
    }
    return (SELF) this;
  }

  /**
   * Returns a key iterator above all keys of the Extensible, this includes the JAVA properties.
   *
   * @return the key iterator.
   */
  public @NotNull ExtensibleKeySet keySet() {
    return new ExtensibleKeySet();
  }

  private static final @NotNull Object @NotNull [] EMPTY_OBJECT_ARRAY = new Object[0];

  /**
   * The key-set of an Extensible. This key-set covers the fixed JAVA properties and the dynamic extensions properties.
   */
  public class ExtensibleKeySet implements Set<@NotNull String> {

    /**
     * Returns the extensible from which this key-set was gathered.
     *
     * @return The extensible from which this key-set was gathered.
     */
    public @NotNull SELF parent() {
      return Extensible.this.self();
    }

    ExtensibleKeySet() {
      jsonClass = JsonClass.of(Extensible.this.getClass());
    }

    private final @NotNull JsonClass jsonClass;

    private @NotNull Map<@NotNull String, @Nullable Object> map() {
      return Extensible.this.additionalProperties();
    }

    @Override
    public int size() {
      return jsonClass.size() + map().size();
    }

    @Override
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
      if (o instanceof String) {
        final String key = (String) o;
        return jsonClass.hasField(key) || map().containsKey(key);
      }
      return false;
    }

    @Nonnull
    @Override
    public Iterator<@NotNull String> iterator() {
      final Map<@NotNull String, @Nullable Object> map = map();
      final Iterator<@NotNull String> keysIt = map.keySet().iterator();
      return new Iterator<String>() {
        int i;

        @Override
        public boolean hasNext() {
          return i < jsonClass.fields.length || keysIt.hasNext();
        }

        @Override
        public String next() {
          if (i < jsonClass.fields.length) {
            return jsonClass.fields[i++].name;
          }
          return keysIt.next();
        }
      };
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public @NotNull Object @NotNull [] toArray() {
      // Note: We know that the returned array does not contain null values!
      return toArray(EMPTY_OBJECT_ARRAY);
    }

    @SuppressWarnings("unchecked")
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
        array[i++] = (T) jsonField.name;
      }
      for (final @NotNull String key : map().keySet()) {
        array[i++] = (T) key;
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
      if (map().containsKey(s)) {
        return false;
      }
      map().put(s, null);
      return true;
    }

    @Override
    public boolean remove(Object o) {
      if (!(o instanceof String)) {
        throw new IllegalArgumentException("The given key is no string");
      }
      final String key = (String) o;
      if (jsonClass.hasField(key)) {
        throw new IllegalStateException("The field " + key + " is not removable.");
      }
      if (map().containsKey(key)) {
        map().remove(key);
        return true;
      }
      return false;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
      final Map<@NotNull String, @Nullable Object> map = map();
      for (final Object o : c) {
        if (!(o instanceof String)) {
          return false;
        }
        final String key = (String) o;
        if (!jsonClass.hasField(key) && !map.containsKey(key)) {
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
      final HashMap<@NotNull String, @NotNull Boolean> retain = new HashMap<>();
      for (final Object o : c) {
        if (!(o instanceof String)) {
          throw new IllegalArgumentException("The given key is no string: " + o);
        }
        retain.put((String) o, Boolean.TRUE);
      }
// Note: We could throw an exception, or we simply ignore hard properties, which seems to be better!
//      for (final JsonField field : jsonClass.fields) {
//        if (!retain.containsKey(field.name)) {
//          throw new IllegalArgumentException("The property " + field.name + " is not removable");
//        }
//      }
      boolean modified = false;
      final Map<@NotNull String, @Nullable Object> map = map();
      for (final String key : map.keySet()) {
        if (!retain.containsKey(key)) {
          map.remove(key);
          modified = true;
        }
      }
      return modified;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
      boolean modified = false;
      final Map<@NotNull String, @Nullable Object> map = map();
      for (final Object o : c) {
        if (!(o instanceof String)) {
          throw new IllegalArgumentException("The given key is no string: " + o);
        }
        // Note: We simply ignore all not removable properties.
        final String key = (String) o;
        if (map.containsKey(key)) {
          map.remove(key);
          modified = true;
        }
      }
      return modified;
    }

    @Override
    public void clear() {
      if (jsonClass.size() != 0) {
        throw new IllegalStateException("Some properties are not removable!");
      }
      map().clear();
    }
  }
}
