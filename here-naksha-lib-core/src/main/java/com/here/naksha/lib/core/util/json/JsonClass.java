package com.here.naksha.lib.core.util.json;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.util.StringHelper;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A JSON class created from a Java class.
 *
 * @param <CLASS> the object-type.
 */
@SuppressWarnings("unused")
public final class JsonClass<CLASS> {

    private static final JsonField<?, ?>[] EMPTY = new JsonField[0];
    private static final ConcurrentHashMap<@NotNull Class<?>, @NotNull JsonClass<?>> cache = new ConcurrentHashMap<>();

    /**
     * Returns a JSON class for a Java class. This method ignores private fields that are not
     * annotated with {@link JsonProperty}.
     *
     * @param javaClass The JAVA class for which to return the JSON class.
     * @return the JSON class.
     */
    public static <C> @NotNull JsonClass<C> of(final @NotNull Class<C> javaClass) {
        //noinspection unchecked
        JsonClass<C> jsonClass = (JsonClass<C>) cache.get(javaClass);
        if (jsonClass != null) {
            return jsonClass;
        }
        synchronized (JsonClass.class) {
            //noinspection unchecked
            jsonClass = (JsonClass<C>) cache.get(javaClass);
            if (jsonClass != null) {
                return jsonClass;
            }
            jsonClass = new JsonClass<>(javaClass);
            cache.put(javaClass, jsonClass);
            return jsonClass;
        }
    }

    /**
     * Returns a JSON class for a Java object, if the object is a class, the JSON class for this class
     * returned, therefore this is the same as calling {@link #of(Class)}. This method ignores private
     * fields that are not annotated with {@link JsonProperty}.
     *
     * @param object The JAVA object for which to return the JSON class.
     * @return the JSON class.
     * @throws NullPointerException if given object is null.
     */
    public static <C> @NotNull JsonClass<C> of(final @NotNull C object) {
        //noinspection unchecked
        return of((Class<C>) (object instanceof Class ? object : object.getClass()));
    }

    /**
     * Creates the JSON class from the given JAVA class.
     *
     * @param javaClass The JAVA class.
     */
    JsonClass(final @NotNull Class<CLASS> javaClass) {
        this.javaClass = javaClass;
        //noinspection unchecked
        JsonField<CLASS, ?>[] jsonFields = (JsonField<CLASS, ?>[]) EMPTY;
        Class<?> theClass = javaClass;
        while (theClass != null && theClass != Object.class) {
            final Field[] fields = theClass.getDeclaredFields();
            int new_fields_count = fields.length;
            for (int i = 0; i < fields.length; i++) {
                final Field field = fields[i];
                final int fieldModifiers = field.getModifiers();

                // We can't handle static fields.
                if (Modifier.isStatic(fieldModifiers)) {
                    fields[i] = null;
                    new_fields_count--;
                    continue;
                }

                final JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
                // Special handling for fields not explicitly annotated to be JSON properties.
                if (jsonProperty == null) {
                    final JsonIgnore jsonIgnore = field.getAnnotation(JsonIgnore.class);
                    // Remove all properties annotated as NOT being a JSON properties or being private.
                    if (jsonIgnore != null || Modifier.isPrivate(fieldModifiers)) {
                        fields[i] = null;
                        new_fields_count--;
                    }
                }
            }
            if (new_fields_count > 0) {
                int newIndex = jsonFields.length;
                jsonFields = Arrays.copyOf(jsonFields, jsonFields.length + new_fields_count);
                for (final @Nullable Field field : fields) {
                    if (field != null) {
                        //noinspection unchecked
                        jsonFields[newIndex++] = (JsonField<CLASS, ?>) JsonField.construct(this, field, newIndex);
                    }
                }
            }
            theClass = theClass.getSuperclass();
        }
        this.fields = jsonFields;
        for (final JsonField<CLASS, ?> field : fields) {
            fieldsMap.putIfAbsent(field.jsonName, field);
        }
    }

    /**
     * All JSON fields of the class, those of the super class are at the end.
     *
     * <p><b>WARNING</b>: This array <b>MUST NOT</b> be modified, it is read-only!
     */
    public final @NotNull JsonField<CLASS, ?> @NotNull [] fields;

    /** The JAVA class. */
    public final Class<CLASS> javaClass;

    private final @NotNull LinkedHashMap<@NotNull String, @NotNull JsonField<CLASS, ?>> fieldsMap =
            new LinkedHashMap<>();

    /**
     * Returns the field at the given index.
     *
     * @param index The index to query.
     * @return The JSON field.
     * @throws ArrayIndexOutOfBoundsException If the given index is out of bounds (less than zero or
     *     more than {@link #size()}.
     */
    public @NotNull JsonField<CLASS, ?> getField(int index) {
        return fields[index];
    }

    /**
     * Returns the field at the given index.
     *
     * @param index the index to query.
     * @param valueClass the class of the expected value.
     * @return The JSON field.
     * @throws ArrayIndexOutOfBoundsException if the given index is out of bounds (less than zero or
     *     more than {@link #size()}.
     * @throws ClassCastException if the field exists, but is of a different type.
     */
    public <VALUE> @NotNull JsonField<CLASS, VALUE> getField(int index, @NotNull Class<VALUE> valueClass) {
        final JsonField<CLASS, ?> field = fields[index];
        if (!valueClass.isAssignableFrom(field.valueClass)) {
            throw new ClassCastException("type "
                    + valueClass.getName()
                    + " is incompatible with real type of field "
                    + field.javaName
                    + " being "
                    + field.valueClass.getName());
        }
        //noinspection unchecked
        return (JsonField<CLASS, VALUE>) field;
    }

    /**
     * Returns the JSON field with the given name; if such a field exists.
     *
     * @param name the name of the field.
     * @return the JSON field; {@code null} if no such field exists.
     */
    public @Nullable JsonField<CLASS, ?> getField(@Nullable CharSequence name) {
        return name != null ? fieldsMap.get(StringHelper.toString(name)) : null;
    }

    /**
     * Returns the JSON field with the given name; if such a field exists.
     *
     * @param name the name of the field.
     * @param valueClass the class of the expected value.
     * @return the JSON field; {@code null} if no such field exists.
     * @throws ClassCastException if the field exists, but is of a different type.
     */
    public <VALUE> @Nullable JsonField<CLASS, VALUE> getField(
            @Nullable CharSequence name, @NotNull Class<VALUE> valueClass) {
        if (name == null) {
            return null;
        }
        final JsonField<CLASS, ?> field = fieldsMap.get(StringHelper.toString(name));
        if (field == null) {
            return null;
        }
        if (!valueClass.isAssignableFrom(field.valueClass)) {
            throw new ClassCastException("type "
                    + valueClass.getName()
                    + " is incompatible with real type of field "
                    + field.javaName
                    + " being "
                    + field.valueClass.getName());
        }
        //noinspection unchecked
        return (JsonField<CLASS, VALUE>) field;
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
