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
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.util.JsonClass;
import com.here.xyz.util.JsonField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An object that allows to access Java members with their JSON name, plus arbitrary additional members that are not known at compile time.
 * The class implements a map that allows to access all JSON properties. Apart, it offers a specific method to gain access to only
 * {@link #getAdditionalProperties() additional properties}. To access only the native properties use {@link JsonClass#of(Object)}.
 * <p>
 * Removing a Java member will set the value to {@code null} (or the equivalent), but the size of the object will not change.
 */
@SuppressWarnings({"unchecked", "UnusedReturnValue", "unused"})
@JsonInclude(Include.NON_EMPTY)
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public class JsonObject implements Map<@NotNull String, @Nullable Object>, XyzSerializable {

  @JsonIgnore
  @Nullable Map<@NotNull String, @Nullable Object> additionalProperties;

  @JsonAnyGetter
  @Nullable Map<@NotNull String, @Nullable Object> jsonAnyGetter() {
    return additionalProperties;
  }

  /**
   * Sets the property with the given name to the given value. If no such property exists, adds it.
   *
   * @param name  The name of the property.
   * @param value The value to set.
   * @return The previous value.
   * @throws IllegalArgumentException If the given value is of an illegal type.
   */
  @JsonAnySetter
  public @Nullable Object put(@NotNull String name, @Nullable Object value) {
    final JsonClass jsonClass = JsonClass.of(this);
    if (jsonClass.hasField(name)) {
      final JsonField field = jsonClass.getField(name);
      assert field != null;
      return field.setValue(this, value);
    }
    return getAdditionalProperties().put(name, value);
  }

  /**
   * Sets the property with the given name to the given value. If no such property exists, adds it.
   *
   * @param name  The name of the property.
   * @param value The value.
   * @param <V>   The value-type.
   * @return the previous stored value.
   * @throws ClassCastException       If the returned value is of an unexpected type, or the field value does not match the given value
   *                                  type.
   * @throws IllegalArgumentException If the given value is of the wrong type.
   */
  public final <V> @Nullable V set(@NotNull String name, @Nullable Object value) {
    return (V) put(name, value);
  }

  /**
   * Returns the value of the property with the given name.
   *
   * @param name The name of the properties.
   * @param <V>  The value type.
   * @return The value; {@code null} if no such property exists.
   * @throws NullPointerException if the given name is null.
   */
  public final <V> @Nullable V get(@NotNull String name) {
    return (V) get((Object) name);
  }

  /**
   * Returns the value of the property with the given name.
   *
   * @param name The name of the properties.
   * @return The value; {@code null} if no such property exists, or the value is {@code null}.
   * @throws ClassCastException   if the given name is no string.
   * @throws NullPointerException if the given name is null.
   */
  public @Nullable Object get(@NotNull Object name) {
    if (!(name instanceof String key)) {
      throw new ClassCastException("name must be String");
    }
    final JsonClass jsonClass = JsonClass.of(getClass());
    if (jsonClass.hasField(key)) {
      final JsonField jsonField = jsonClass.getField(key);
      assert jsonField != null;
      return jsonField.getValue(this);
    }
    if (additionalProperties != null && additionalProperties.containsKey(key)) {
      return additionalProperties.get(key);
    }
    return null;
  }

  /**
   * Tests whether this Extensible has a property with the given name.
   *
   * @param name The name of the property to lookup.
   * @return {@code true} if such a property exists; {@code false} if no such property exists.
   */
  public boolean containsKey(@NotNull Object name) {
    if (name instanceof String key) {
      final JsonClass jsonClass = JsonClass.of(getClass());
      if (jsonClass.hasField(key)) {
        return true;
      }
      return additionalProperties != null && additionalProperties.containsKey(key);
    } else {
      throw new ClassCastException("name should be String");
    }
  }

  /**
   * Returns a map of all additional properties, so properties not know to JAVA. If yet no additional properties defined, this will create a
   * hash-map and return the empty hash-map. It is not recommended to call this method without testing for additional properties via
   * {@link #hasAdditionalProperties()}.
   *
   * @return A map of all additional properties, so properties not know to JAVA.
   */
  public final @NotNull Map<@NotNull String, @Nullable Object> getAdditionalProperties() {
    if (this.additionalProperties == null) {
      this.additionalProperties = new LinkedHashMap<>();
    }
    return additionalProperties;
  }

  /**
   * Tests if this object has additional properties.
   *
   * @return {@code true} if this object has additional properties; {@code false} otherwise.
   */
  public final boolean hasAdditionalProperties() {
    return additionalProperties != null && additionalProperties.size() > 0;
  }

  /**
   * Removes the additional properties from this object.
   *
   * @return The removed properties; if any.
   */
  public final @Nullable Map<@NotNull String, @Nullable Object> removeAdditionalProperties() {
    final Map<@NotNull String, @Nullable Object> old = this.additionalProperties;
    this.additionalProperties = null;
    return old;
  }

  @Override
  public Object remove(@NotNull Object name) {
    if (!(name instanceof String key)) {
      throw new ClassCastException("name must be String");
    }
    final JsonClass jsonClass = JsonClass.of(this);
    if (jsonClass.hasField(key)) {
      final JsonField jsonField = jsonClass.getField(key);
      assert jsonField != null;
      return jsonField.setValue(this, null);
    }
    if (additionalProperties != null && additionalProperties.containsKey(key)) {
      return additionalProperties.remove(key);
    }
    return null;
  }

  @Override
  public int size() {
    final JsonClass jsonClass = JsonClass.of(this);
    return jsonClass.size() + (additionalProperties == null ? 0 : additionalProperties.size());
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsValue(@Nullable Object value) {
    final JsonClass jsonClass = JsonClass.of(this);
    for (final JsonField field : jsonClass.fields) {
      final Object v = field.getValue(this);
      if (Objects.equals(v, value)) {
        return true;
      }
    }
    final Map<@NotNull String, @Nullable Object> additionalProperties = this.additionalProperties;
    if (additionalProperties != null) {
      for (final @NotNull Map.Entry<String, Object> entry : additionalProperties.entrySet()) {
        if (Objects.equals(entry.getValue(), value)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void putAll(@NotNull Map<? extends @NotNull String, ?> m) {
    for (final @NotNull Map.Entry<? extends @NotNull String, ?> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    removeAdditionalProperties();
    final JsonClass jsonClass = JsonClass.of(this);
    for (final JsonField field : jsonClass.fields) {
      if (!field.isFinal) {
        field.setValue(this, null);
      }
    }
  }

  /**
   * Returns a key iterator above all keys of the Extensible, this includes the JAVA properties.
   *
   * @return the key iterator.
   */
  public @NotNull JsonObjectKeySet keySet() {
    return new JsonObjectKeySet(this);
  }

  @Override
  public @NotNull Collection<@Nullable Object> values() {
    final ArrayList<@Nullable Object> values = new ArrayList<>(size());
    final JsonClass jsonClass = JsonClass.of(this);
    for (final JsonField field : jsonClass.fields) {
      values.add(field.getValue(this));
    }
    if (additionalProperties != null) {
      values.addAll(additionalProperties.values());
    }
    return values;
  }

  @Override
  public @NotNull Set<@NotNull Entry<@NotNull String, @Nullable Object>> entrySet() {
    throw new UnsupportedOperationException();
  }

}