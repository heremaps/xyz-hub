package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.util.Unsafe.unsafe;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.lambdas.F5;
import com.here.naksha.lib.core.util.StringCache;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper for a native Java field to be treated like a JSON field.
 *
 * @param <OBJECT> the object-type.
 * @param <VALUE> the value-type.
 */
@SuppressWarnings("unused")
public abstract class JsonField<OBJECT, VALUE> implements CharSequence {

    /** A static concurrent map to have special implementations for specific fields. */
    @SuppressWarnings("rawtypes")
    public static final ConcurrentHashMap<Class, F5<JsonField, JsonClass, Field, Integer, String, String>>
            constructors = new ConcurrentHashMap<>();

    /**
     * Register a new JSON field type. Usage:
     *
     * <pre>{@code
     * static {
     *   JsonField.register(MyFieldImpl.class, MyFieldImpl::new);
     * }
     * }</pre>
     *
     * @param valueType the value-type class.
     * @param constructor the constructor of the {@link JsonField}.
     * @param <OBJECT> the JAVA class-type for which the field is.
     * @param <VALUE> the value-type of the field.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <OBJECT, VALUE> void register(
            Class<VALUE> valueType,
            @NotNull F5<? extends JsonField<OBJECT, VALUE>, JsonClass<OBJECT>, Field, Integer, String, String>
                            constructor) {
        constructors.put(valueType, (F5<JsonField, JsonClass, Field, Integer, String, String>) (F5) constructor);
    }

    /**
     * Create the matching JSON field.
     *
     * @param jsonClass the JSON class for which to create the field.
     * @param javaField the JAVA field to create.
     * @param index the index in the field-list.
     * @return the JSON field instance.
     */
    static @NotNull JsonField<?, ?> construct(@NotNull JsonClass<?> jsonClass, @NotNull Field javaField, int index) {
        String defaultValue = null;
        String jsonName = javaField.getName();
        final JsonProperty jsonProperty = javaField.getAnnotation(JsonProperty.class);
        if (jsonProperty != null) {
            String annotatedName = jsonProperty.value();
            if (!JsonProperty.USE_DEFAULT_NAME.equals(annotatedName)) {
                jsonName = annotatedName;
            }
            if (jsonProperty.defaultValue() != null
                    && jsonProperty.defaultValue().length() > 0) {
                defaultValue = jsonProperty.defaultValue();
            }
        }
        final Class<?> valueClass = javaField.getType();
        if (valueClass.isPrimitive()) {
            if (valueClass == boolean.class) {
                return new JsonFieldBool<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == byte.class) {
                return new JsonFieldByte<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == short.class) {
                return new JsonFieldShort<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == char.class) {
                return new JsonFieldChar<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == int.class) {
                return new JsonFieldInt<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == long.class) {
                return new JsonFieldLong<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == float.class) {
                return new JsonFieldFloat<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            if (valueClass == double.class) {
                return new JsonFieldDouble<>(jsonClass, javaField, index, jsonName, defaultValue);
            }
            throw new IllegalArgumentException(
                    "Invalid default value for field " + jsonClass.javaClass.getName() + "::" + javaField.getName());
        }
        //noinspection rawtypes
        final F5<JsonField, JsonClass, Field, Integer, String, String> constructor =
                constructors.get(jsonClass.javaClass);
        if (constructor != null) {
            return constructor.call(jsonClass, javaField, index, jsonName, defaultValue);
        }
        return new JsonFieldObject<>(jsonClass, javaField, index, jsonName, defaultValue);
    }

    /**
     * Create a new JSON field from the given JAVA field.
     *
     * @param jsonClass the JSON class to which the field belongs.
     * @param javaField the JAVA field.
     * @param index the index of the field in the JSON class.
     * @param jsonName the JSON name of the field.
     * @param defaultValue the default value as string.
     */
    JsonField(
            @NotNull JsonClass<OBJECT> jsonClass,
            @NotNull Field javaField,
            int index,
            @NotNull String jsonName,
            @Nullable String defaultValue) {
        this.index = index;
        this.jsonClass = jsonClass;
        this.objectClass = jsonClass.javaClass;
        this.javaField = javaField;
        this.javaName = StringCache.intern(javaField.getName());
        this.jsonName = StringCache.intern(jsonName);
        //noinspection unchecked
        this.valueClass = (Class<VALUE>) javaField.getType();
        this.isFinal = Modifier.isFinal(javaField.getModifiers());
        this.isNullable = !valueClass.isPrimitive()
                && javaField.getAnnotation(NotNull.class) == null
                && javaField.getAnnotation(javax.annotation.Nonnull.class) == null;

        //noinspection StringEquality
        this.fullName = StringCache.intern(jsonClass.javaClass.getName()
                + "::"
                + javaField.getName()
                + (jsonName != javaName ? "@{" + jsonName + "}" : ""));

        // Examples: "com.here.Foo::transaction" or "com.here.Foo::transaction@{tx}", if json-name
        // differs.
        this.offset = unsafe.objectFieldOffset(javaField);
    }

    /** The index of the field in the {@link JsonClass#fields}. */
    public final int index;

    /** The offset of the field in the binary representation. */
    public final long offset;

    /** The class to which the field belongs. */
    public final @NotNull JsonClass<OBJECT> jsonClass;

    /** The native Java field. */
    public final @NotNull Field javaField;

    /** The JAVA class holding the field. */
    public final @NotNull Class<OBJECT> objectClass;

    /** The name of the field in JSON documents. */
    public final @NotNull String jsonName;

    /** The name of the field in Java. */
    public final @NotNull String javaName;

    /** The full qualified name, so the class, field name and JSON name. */
    public final @NotNull String fullName;

    /** The class of the value type. */
    public final @NotNull Class<VALUE> valueClass;

    /** Flag to signal if the field is final (immutable). */
    public final boolean isFinal;

    /** If the field maybe set legally to {@code null}. */
    public final boolean isNullable;

    /**
     * Returns the default value as object.
     *
     * @return the default value as object.
     */
    public abstract VALUE defaultValue();

    /**
     * Returns the {@code null} value as object.
     *
     * @return the {@code null} value as object.
     */
    public abstract VALUE nullValue();

    /**
     * Ensure that the given object is of the required {@link #objectClass class}.
     *
     * @param object the object to verify.
     * @return the given object, guaranteed to be a valid object.
     * @throws IllegalArgumentException if the given value is of an invalid type.
     */
    public @NotNull OBJECT object(@Nullable Object object) {
        if (objectClass.isInstance(object)) {
            return objectClass.cast(object);
        }
        throw new IllegalArgumentException("object is no instance of " + objectClass.getName());
    }

    /**
     * Ensure that the given value is of the required {@link #valueClass type}.
     *
     * @param value the value to verify.
     * @return the given value or an equivalent for {@code null}, in any case guaranteed to be a valid
     *     value for the field.
     * @throws IllegalArgumentException if the given value is of an invalid type.
     */
    public abstract VALUE value(@Nullable Object value);

    /**
     * Returns the value of the field.
     *
     * @param object The object to query.
     * @return the value.
     * @throws ClassCastException If the returned value is of an unexpected value.
     */
    public abstract VALUE _get(@NotNull OBJECT object);

    /**
     * Sets the value of the field.
     *
     * @param object The object to query.
     * @param value The value to set.
     * @throws ClassCastException if the access mode type matches the caller's symbolic type
     *     descriptor, but a reference cast fails.
     */
    public abstract void _put(@NotNull OBJECT object, VALUE value);

    /**
     * Sets the value of the field. When {@code expected} and {@code value} are the same, then still
     * an CAS operation is done.
     *
     * @param object the object to query.
     * @param expected the value that is expected.
     * @param value the value to set.
     * @return {@code true} if the value was set; {@code false} if the current value is not the
     *     expected value.
     * @throws ClassCastException if the access mode type matches the caller's symbolic type
     *     descriptor, but a reference cast fails.
     */
    public abstract boolean _compareAndSwap(@NotNull OBJECT object, VALUE expected, VALUE value);

    /**
     * Returns the value of the field.
     *
     * @param object The object to query.
     * @return the value.
     * @throws ClassCastException If the returned value is of an unexpected value.
     */
    public VALUE get(@NotNull Object object) {
        return _get(object(object));
    }

    /**
     * Sets the value of the field.
     *
     * @param object The object to query.
     * @param value The value to set.
     * @return the previous value.
     * @throws ClassCastException if the access mode type matches the caller's symbolic type
     *     descriptor, but a reference cast fails.
     */
    public VALUE set(@NotNull Object object, Object value) {
        final OBJECT target = object(object);
        final VALUE old = _get(target);
        _put(target, value(value));
        return old;
    }

    /**
     * Sets the value of the field. When {@code expected} and {@code value} are the same, then still
     * an CAS operation is done.
     *
     * @param object the object to query.
     * @param expected the value that is expected.
     * @param value the value to set.
     * @return {@code true} if the value was set; {@code false} if the current value is not the
     *     expected value.
     * @throws ClassCastException if the access mode type matches the caller's symbolic type
     *     descriptor, but a reference cast fails.
     */
    public boolean compareAndSwap(@NotNull Object object, @Nullable Object expected, @Nullable Object value) {
        final OBJECT target = object(object);
        final VALUE expected_value = value(expected);
        final VALUE new_value = value(value);
        return _compareAndSwap(target, expected_value, new_value);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return this == other;
    }

    @Override
    public int hashCode() {
        return fullName.hashCode();
    }

    @Override
    public int length() {
        return fullName.length();
    }

    @Override
    public char charAt(int index) {
        return fullName.charAt(index);
    }

    @Nonnull
    @Override
    public CharSequence subSequence(int start, int end) {
        return fullName.subSequence(start, end);
    }

    @Override
    public @NotNull String toString() {
        return fullName;
    }
}
