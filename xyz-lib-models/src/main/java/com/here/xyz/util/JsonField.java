package com.here.xyz.util;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A JSON field.
 */
public class JsonField {

  /**
   * Create a new JSON field with the given name and type {@link Object}.
   *
   * @param name The name of the JSON field.
   */
  public JsonField(@NotNull String name) {
    this.javaField = null;
    this.name = name;
    this.valueClass = Object.class;
  }

  /**
   * Create a new JSON field with the given name and type.
   *
   * @param name       The name of the JSON field.
   * @param valueClass The class of the value-type.
   */
  public JsonField(@NotNull String name, @NotNull Class<?> valueClass) {
    this.javaField = null;
    this.name = name;
    this.valueClass = valueClass;
  }

  /**
   * Create a new JSON field from the given JAVA field.
   *
   * @param javaField The JAVA field.
   */
  public JsonField(@NotNull Field javaField) {
    this.javaField = javaField;
    this.name = javaField.getName();
    this.valueClass = javaField.getType();
  }

  /**
   * Create a new JSON field from the given JAVA field with an alternative JSON name.
   *
   * @param javaField The JAVA field.
   * @param name      The JSON name.
   */
  public JsonField(@NotNull Field javaField, @NotNull String name) {
    this.javaField = javaField;
    this.name = name;
    this.valueClass = javaField.getType();
  }

  /**
   * If the field comes from a POJO.
   */
  public final @Nullable Field javaField;

  /**
   * The JSON name of the field.
   */
  public final @NotNull String name;

  /**
   * The class of the value type.
   */
  public final @NotNull Class<?> valueClass;

  /**
   * Returns the value of the field.
   *
   * @param object The object to query.
   * @param <V>    The value type.
   * @return the value.
   * @throws ClassCastException If the returned value is of an unexpected value.
   */
  @SuppressWarnings("unchecked")
  public <V> @Nullable V getValue(@NotNull Object object) {
    if (javaField == null) {
      return null;
    }
    try {
      return (V) javaField.get(object);
    } catch (IllegalAccessException ignore) {
      return null;
    }
  }

  /**
   * Sets the value of the field.
   *
   * @param object The object to query.
   * @param value  The value to set.
   * @param <V>    The value type.
   * @return the previous value.
   * @throws ClassCastException       If the returned value is of an unexpected type, or the field value does not match the given value
   *                                  type.
   * @throws IllegalArgumentException If the given value is of the wrong type.
   * @throws IllegalStateException    If this field is no JAVA field ({@link #javaField} is {@code null}).
   */
  @SuppressWarnings("unchecked")
  public <V> @Nullable V setValue(@NotNull Object object, @Nullable Object value) {
    if (javaField == null) {
      throw new IllegalStateException("Missing java field");
    }
    if (value == null) {
      if (valueClass.isPrimitive()) {
        throw new IllegalArgumentException("Primitive can't become null");
      }
    } else if (!valueClass.isAssignableFrom(value.getClass())) {
      throw new IllegalArgumentException("The given value is of an incompatible type");
    }
    try {
      Object old;
      try {
        old = javaField.get(object);
        javaField.set(object, null);
      } catch (IllegalAccessException e) {
        javaField.setAccessible(true);
        old = javaField.get(object);
        javaField.set(object, null);
      }
      return (V) old;
    } catch (InaccessibleObjectException | SecurityException | IllegalAccessException e) {
      throw new IllegalStateException("Access denied", e);
    }
  }
}
