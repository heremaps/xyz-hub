package com.here.xyz.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A JSON class created from a Java class.
 */
public class JsonClass {

  private static final JsonField[] EMPTY = new JsonField[0];
  private static final ConcurrentHashMap<@NotNull Class<?>, @NotNull JsonClass> cache = new ConcurrentHashMap<>();

  /**
   * Returns a field list for a Java class and caches the generated list. This method ignores private fields that are not annotated with
   * {@link JsonProperty}.
   *
   * @param javaClass The JAVA class for which to return the JSON field list.
   * @return the field list.
   */
  public static @NotNull JsonClass of(final @NotNull Class<?> javaClass) {
    JsonClass jsonClass = cache.get(javaClass);
    if (jsonClass != null) {
      return jsonClass;
    }
    jsonClass = new JsonClass(javaClass);
    final JsonClass existing = cache.putIfAbsent(javaClass, jsonClass);
    return existing != null ? existing : jsonClass;
  }

  /**
   * Creates the JSON class from the given JAVA class.
   *
   * @param javaClass The JAVA class.
   */
  JsonClass(@NotNull Class<?> javaClass) {
    JsonField[] jsonFields = EMPTY;
    Class<?> theClass = javaClass;
    while (theClass != null && theClass != Object.class) {
      final Field[] fields = javaClass.getDeclaredFields();
      final String[] names = new String[fields.length];
      int field_count = jsonFields.length + fields.length;
      for (int i = 0; i < fields.length; i++) {
        final Field field = fields[i];
        final JsonIgnore jsonIgnore = field.getAnnotation(JsonIgnore.class);
        if (jsonIgnore != null) {
          fields[i] = null;
          field_count--;
          continue;
        }

        final JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
        if (jsonProperty != null) {
          String name = jsonProperty.value();
          if (JsonProperty.USE_DEFAULT_NAME.equals(name)) {
            name = field.getName();
          }
          names[i] = name;
          continue;
        }

        final int fieldModifiers = field.getModifiers();
        if (Modifier.isPrivate(fieldModifiers)) {
          fields[i] = null;
          field_count--;
        } else {
          names[i] = field.getName();
        }
      }
      if (field_count > 0) {
        int newIndex = jsonFields.length;
        jsonFields = Arrays.copyOf(jsonFields, jsonFields.length + field_count);
        for (int i = 0; i < fields.length; i++) {
          if (fields[i] != null) {
            assert names[i] != null;
            jsonFields[newIndex++] = new JsonField(fields[i], names[i]);
          }
        }
      }
      theClass = theClass.getSuperclass();
    }
    this.fields = jsonFields;
    for (final JsonField field : fields) {
      fieldsMap.putIfAbsent(field.name, field);
    }
  }

  /**
   * All JSON fields of the class, those of the super class are at the end.
   *
   * <p><b>WARNING</b>: This array <b>MUST NOT</b> be modified, it is read-only!</p>
   */
  public final @NotNull JsonField @NotNull [] fields;

  private final @NotNull LinkedHashMap<@NotNull String, @NotNull JsonField> fieldsMap = new LinkedHashMap<>();

  /**
   * Returns the field at the given index.
   *
   * @param index The index to query.
   * @return The JSON field.
   * @throws ArrayIndexOutOfBoundsException If the given index is out of bounds (less than zero or more than {@link #size()}.
   */
  public @NotNull JsonField getField(int index) {
    return fields[index];
  }

  /**
   * Returns the JSON field with the given name; if such a field exists.
   *
   * @param name The name of the field.
   * @return the JSON field; {@code null} if no such field exists.
   */
  public @Nullable JsonField getField(@Nullable CharSequence name) {
    if (name instanceof String) {
      return fieldsMap.get((String) name);
    }
    return name != null ? fieldsMap.get(name.toString()) : null;
  }

  /**
   * Tests whether a property with the given name exists.
   *
   * @param name The name to test for.
   * @return {@code true} if such a property exists; {@code false} otherwise.
   */
  public boolean hasField(@Nullable CharSequence name) {
    if (name instanceof String) {
      return fieldsMap.containsKey((String) name);
    }
    return name != null && fieldsMap.containsKey(name.toString());
  }

  /**
   * Tests whether the given field index is valid.
   *
   * @param index The index to test for.
   * @return {@code true} if such a property exists; {@code false} otherwise.
   */
  public boolean hasField(int index) {
    return index >= 0 && index < fields.length;
  }

  /**
   * Returns the amount of fields.
   *
   * @return The amount of fields.
   */
  public int size() {
    return fields.length;
  }
}
