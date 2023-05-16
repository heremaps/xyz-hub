package com.here.xyz.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper for a native Java field to be treated like a JSON field.
 */
@SuppressWarnings({"unused", "unchecked"})
public class JsonField {

  /**
   * Create a new JSON field from the given JAVA field.
   *
   * @param javaField The JAVA field.
   */
  JsonField(@NotNull JsonClass jsonClass, @NotNull Field javaField) {
    this.jsonClass = jsonClass;
    this.javaField = javaField;
    this.javaName = javaField.getName();
    this.valueClass = javaField.getType();
    this.isFinal = Modifier.isFinal(javaField.getModifiers());
    javaField.setAccessible(true);

    String defaultValueString = null;
    String name = javaName;
    final JsonProperty jsonProperty = javaField.getAnnotation(JsonProperty.class);
    if (jsonProperty != null) {
      String annotatedName = jsonProperty.value();
      if (!JsonProperty.USE_DEFAULT_NAME.equals(annotatedName)) {
        name = annotatedName;
      }
      if (jsonProperty.defaultValue() != null && jsonProperty.defaultValue().length() > 0) {
        defaultValueString = jsonProperty.defaultValue();
      }
    }
    this.jsonName = name;

    if (valueClass.isPrimitive()) {
      if (valueClass == Boolean.TYPE) {
        defaultValue = "true".equalsIgnoreCase(defaultValueString);
      } else if (valueClass == byte.class) {
        defaultValue = defaultValueString != null ? Byte.parseByte(defaultValueString) : (byte) 0;
      } else if (valueClass == short.class) {
        defaultValue = defaultValueString != null ? Short.parseShort(defaultValueString) : (short) 0;
      } else if (valueClass == int.class) {
        defaultValue = defaultValueString != null ? Integer.parseInt(defaultValueString) : 0;
      } else if (valueClass == long.class) {
        defaultValue = defaultValueString != null ? Long.parseLong(defaultValueString) : 0L;
      } else if (valueClass == float.class) {
        defaultValue = defaultValueString != null ? Float.parseFloat(defaultValueString) : 0f;
      } else if (valueClass == double.class) {
        defaultValue = defaultValueString != null ? Double.parseDouble(defaultValueString) : 0d;
      } else {
        throw new IllegalArgumentException(
            "Invalid default value for field " + jsonClass.javaClass.getName() + "::" + javaField.getName());
      }
    } else if (valueClass == String.class) {
      defaultValue = defaultValueString;
    } else if (defaultValueString != null) {
      throw new IllegalArgumentException(
          "Invalid default value for field " + jsonClass.javaClass.getName() + "::" + javaField.getName());
    } else {
      defaultValue = null;
    }

    this.isNullable = !valueClass.isPrimitive()
        && javaField.getAnnotation(NotNull.class) == null
        && javaField.getAnnotation(javax.annotation.Nonnull.class) == null;

    // Example: "com.here.Foo::transaction{@tx}".
    this.fullName = jsonClass.javaClass.getName() + "::" + javaField.getName() + "{@" + name + "}";
  }

  /**
   * The class to which the field belongs.
   */
  public final @NotNull JsonClass jsonClass;

  /**
   * The native Java field.
   */
  public final @NotNull Field javaField;

  /**
   * The name of the field in JSON documents.
   */
  public final @NotNull String jsonName;

  /**
   * The name of the field in Java.
   */
  public final @NotNull String javaName;

  /**
   * The full qualified name, so the class, field name and JSON name.
   */
  public final @NotNull String fullName;

  /**
   * The class of the value type.
   */
  public final @NotNull Class<?> valueClass;

  /**
   * Flag to signal if the field is final (immutable).
   */
  public final boolean isFinal;

  /**
   * If the field maybe set legally to {@code null}.
   */
  public final boolean isNullable;

  /**
   * The default value.
   */
  public final @Nullable Object defaultValue;

  /**
   * Returns the value of the field.
   *
   * @param object The object to query.
   * @param <V>    The value type.
   * @return the value.
   * @throws ClassCastException If the returned value is of an unexpected value.
   */
  public <V> @Nullable V getValue(@NotNull Object object) {
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
   * @throws IllegalStateException    If this field is final or any other state related error happened.
   */
  public <V> @Nullable V setValue(@NotNull Object object, @Nullable Object value) {
    return setValue(object, value, false);
  }

  /**
   * Sets the value of the field.
   *
   * @param object      The object to query.
   * @param value       The value to set.
   * @param forceUpdate If {@code true}, then even final field value can be set; {@code false} if final field access should be denied.
   * @param <V>         The value type.
   * @return the previous value.
   * @throws ClassCastException       If the returned value is of an unexpected type, or the field value does not match the given value
   *                                  type.
   * @throws IllegalArgumentException If the given value is of the wrong type.
   * @throws IllegalStateException    If this field is final and {@code forceUpdate} is {@code false} or any other state related error
   *                                  happened.
   */
  @SuppressWarnings("unchecked")
  public <V> @Nullable V setValue(@NotNull Object object, @Nullable Object value, boolean forceUpdate) {
    if (!forceUpdate && isFinal) {
      throw new IllegalStateException("Field " + fullName + " is final");
    }
    try {
      final Object old = javaField.get(object);
      if (value == null) {
        if (valueClass.isPrimitive()) {
          if (valueClass == boolean.class) {
            javaField.set(object, Boolean.FALSE);
          } else if (valueClass == byte.class) {
            javaField.set(object, (byte) 0);
          } else if (valueClass == short.class) {
            javaField.set(object, (short) 0);
          } else if (valueClass == int.class) {
            javaField.set(object, 0);
          } else if (valueClass == long.class) {
            javaField.set(object, 0L);
          } else if (valueClass == float.class) {
            javaField.set(object, 0.0f);
          } else if (valueClass == double.class) {
            javaField.set(object, 0.0d);
          } else {
            // Note: Happens when a field defined with type Void!
            throw new IllegalStateException("Invalid primitive type: " + fullName);
          }
        } else {
          javaField.set(object, null);
        }
      } else {
        javaField.set(object, value);
      }
      return (V) old;
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Access denied to " + fullName, e);
    }
  }

}
