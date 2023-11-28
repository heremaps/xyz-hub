/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.StringCache.string;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.lambdas.P1;
import com.here.naksha.lib.core.util.Unsafe;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is a helper that allows to create enumeration for JSON.
 */
@JsonDeserialize(using = JsonEnumDeserializer.class)
@JsonSerialize(using = JsonEnumSerializer.class)
@SuppressWarnings({"unused", "unchecked"})
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class JsonEnum implements CharSequence {

  /**
   * Parses the given JSON string into the given target type.
   *
   * @param targetClass The class of the target-type.
   * @param json        The JSON input string.
   * @param <T>         The target type.
   * @return the parsed target type.
   */
  public static <T extends JsonEnum> @Nullable T deserialize(@NotNull Class<T> targetClass, @Nullable String json) {
    try (final Json j = Json.get()) {
      return j.reader().forType(targetClass).readValue(json);
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  private static final class TheNullValue implements CharSequence {

    private TheNullValue() {}

    @Override
    protected Object clone() throws CloneNotSupportedException {
      return super.clone();
    }

    public boolean equals(@Nullable Object other) {
      return this == other;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public int length() {
      return "null".length();
    }

    @Override
    public char charAt(int index) {
      return "null".charAt(index);
    }

    @NotNull
    @Override
    public CharSequence subSequence(int start, int end) {
      return "null".subSequence(start, end);
    }

    @Override
    public @NotNull String toString() {
      return "null";
    }
  }

  /**
   * Because we can't store {@code null} in a concurrent hash-map as key, we use this alias.
   */
  private static final TheNullValue NULL = new TheNullValue();

  /**
   * All registered values of a namespace. The first level is the namespace (the class that directly extending {@link JsonEnum}), the second
   * level maps values to registered instances.
   */
  static final ConcurrentHashMap<Class<? extends JsonEnum>, ConcurrentHashMap<Object, JsonEnum>> registry =
      new ConcurrentHashMap<>();

  /**
   * Find the root-class of an enumeration class. If the root-class is not yet initialized, invoke the {@link #init()} method and create the
   * namespace for this class.
   *
   * @param enumClass the enumeration class.
   * @return the root-class, basically the class that directly extends {@link JsonEnum}.
   * @throws NullPointerException     if the given enumeration class is null.
   * @throws IllegalArgumentException if the given enumeration class does not extend {@link JsonEnum}.
   * @throws Error                    if creating a new instance of the enumeration class failed.
   */
  @SuppressWarnings("unchecked")
  private static @NotNull Class<? extends JsonEnum> rootClass(@NotNull Class<? extends JsonEnum> enumClass) {
    //noinspection ConstantValue
    if (enumClass == null) {
      throw new NullPointerException();
    }
    if (enumClass == JsonEnum.class) {
      throw new IllegalArgumentException("JsonEnum.class has no root class, it is already the super-root");
    }
    Class<? extends JsonEnum> rootClass = enumClass;
    while (rootClass.getSuperclass() != JsonEnum.class) {
      rootClass = (Class<? extends JsonEnum>) rootClass.getSuperclass();
    }
    // We need to guarantee that every root-class has a value map.
    ConcurrentHashMap<Object, JsonEnum> values = registry.get(rootClass);
    if (values == null) {
      synchronized (JsonEnum.class) {
        values = registry.get(rootClass);
        if (values == null) {
          values = new ConcurrentHashMap<>();
          registry.put(rootClass, values);
          // Initialize the root-class and therefore all children!
          try {
            JsonEnum initInstance = (JsonEnum) Unsafe.unsafe.allocateInstance(rootClass);
            initInstance.init();
          } catch (InstantiationException e) {
            throw new Error(e);
          }
        }
      }
    }
    return rootClass;
  }

  private static @NotNull Object value(@Nullable Object value) {
    if (value == null) {
      return NULL;
    }
    // Note: Byte, Short, Integer will be converted to Long
    //       Float is converted to Double.
    // This simplifies usage and avoids that a number parsed into a short is not found when being pre-defined.
    if (value instanceof Number) {
      final Number number = (Number) value;
      final Class<? extends Number> numberClass = number.getClass();
      if (numberClass == Byte.class || numberClass == Short.class || numberClass == Integer.class) {
        return number.longValue();
      }
      if (numberClass == Float.class) {
        return number.doubleValue();
      }
    }
    if (value instanceof CharSequence) {
      CharSequence chars = (CharSequence) value;
      final String string = string(chars);
      assert string != null;
      return string;
    }
    return value;
  }

  private static @NotNull <T extends JsonEnum> T __new(
      final @NotNull Class<T> enumClass, @Nullable Object value, boolean tryLowerCase) {
    value = value(value);
    final Class<? extends JsonEnum> rootClass = rootClass(enumClass);
    final ConcurrentHashMap<Object, JsonEnum> values = registry.get(rootClass);
    assert values != null;
    {
      final JsonEnum existing = values.get(value);
      if (enumClass.isInstance(existing)) {
        // Fast path, happens when only well-known values are used.
        return enumClass.cast(existing);
      }
    }
    if (tryLowerCase && value instanceof String) {
      final String stringValue = (String) value;
      final JsonEnum existing = values.get(stringValue.toLowerCase(Locale.ROOT));
      if (enumClass.isInstance(existing)) {
        // Fast path, happens when only well-known values are used.
        return enumClass.cast(existing);
      }
    }
    // Slow path, either unknown value or first time we encounter the value.
    try {
      T enumValue = null;
      try {
        enumValue = enumClass.getConstructor(Object.class).newInstance(value);
      } catch (Throwable ignore) {
      }
      if (enumValue == null) {
        try {
          enumValue = enumClass.getConstructor().newInstance();
        } catch (Throwable ignore) {
        }
      }
      if (enumValue == null) {
        enumValue = (T) Unsafe.unsafe.allocateInstance(enumClass);
      }
      enumValue.value = value;
      enumValue.isDefined = false;
      enumValue.string = value.toString();
      return enumValue;
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  /**
   * Defines a new enumeration value.
   *
   * @param enumClass The enumeration class.
   * @param value     The value.
   * @param <T>       The enumeration type.
   * @return the defined instance.
   * @throws Error If another class is already registered for the value (there is a conflict).
   */
  protected static synchronized @NotNull <T extends JsonEnum> T def(
      @NotNull Class<T> enumClass, @Nullable Object value) {
    final T enumValue = __new(enumClass, value, false);
    if (enumValue.isDefined) {
      throw new Error("The value " + value + " is already defined for "
          + enumValue.getClass().getName());
    }
    final Class<? extends JsonEnum> rootClass = rootClass(enumClass);
    final ConcurrentHashMap<Object, JsonEnum> values = registry.get(rootClass);
    assert values != null;
    final JsonEnum existing = values.putIfAbsent(enumValue.value, enumValue);
    if (existing != null) {
      throw new Error("The value " + value + " is already defined as "
          + existing.getClass().getName() + ", failed to define as " + enumClass.getName());
    }
    enumValue.isDefined = true;
    enumValue.onDefined();
    return enumValue;
  }

  /**
   * Defines a new enumeration value that is not case-sensitive. Beware, that the provided value is still used exactly as given when
   * serializing the value.
   *
   * @param enumClass The enumeration class.
   * @param value     The value.
   * @param <T>       The enumeration type.
   * @return the defined instance.
   * @throws Error If another class is already registered for the value (there is a conflict).
   */
  protected static synchronized @NotNull <T extends JsonEnum> T defIgnoreCase(
      @NotNull Class<T> enumClass, @NotNull String value) {
    final T def = def(enumClass, value);
    final String lowerCase = value.toLowerCase(Locale.ROOT);
    if (!lowerCase.equals(value)) {
      def.alias(enumClass, lowerCase);
    }
    return def;
  }

  /**
   * Returns the enumeration value of the given value. This method will first look up in all defined values and only if the value is not
   * registered, create a new instance for the value. This method should be used for manual gathering of JSON enumeration value.
   *
   * @param value     The raw value to turn into an enumeration value.
   * @param enumClass The enumeration class into which to convert.
   * @param <T>       The base type of the enumeration to return.
   * @return the enumeration value or {@code null}, if the given value is neither a text nor an number.
   */
  public static @NotNull <T extends JsonEnum> T get(final @NotNull Class<T> enumClass, final @Nullable Object value) {
    return __new(enumClass, value, true);
  }

  /**
   * Register the given enumeration class and all defined values with Jackson.
   *
   * @param enumClass The enumeration class to register.
   */
  protected static <T extends JsonEnum> void register(@NotNull Class<T> enumClass) {
    // TODO: Replace with MethodHandles.Lookup.ensureInitialized(Class), but as long as we need to stay compatible
    // to Java 8, well.
    //noinspection removal
    Unsafe.unsafe.ensureClassInitialized(enumClass);
  }

  /**
   * The value, either {@link String}, {@link Long}, {@link Double} or {@link Boolean}.
   */
  @SuppressWarnings("NotNullFieldNotInitialized")
  @NotNull
  Object value;

  /**
   * Cached string representation to {@link #toString()}.
   */
  @SuppressWarnings("NotNullFieldNotInitialized")
  @NotNull
  String string;

  /**
   * If the enumeration value is predefined.
   */
  boolean isDefined;

  /**
   * Tests whether this is a defined (well-known) singleton value. Registered values can be compared by reference.
   *
   * @return {@code True} if this enumeration value is defined and can be compared by reference; {@link false} otherwise.
   */
  public boolean isDefined() {
    return isDefined;
  }

  /**
   * Tests if this enumeration value represents {@code null}.
   *
   * @return {@code true} if this enumeration value represents {@code null}; false otherwise.
   */
  public boolean isNull() {
    return value == NULL;
  }

  /**
   * This method is only called ones a valid is defined.
   */
  protected void onDefined() {}

  /**
   * This method is invoked exactly ones per class, when the enumeration class is not yet initialized. It simplifies auto-initialization.
   * Actually, it is required that the root class (the class directly extending the {@link JsonEnum}) implements this method and invokes
   * {@link #register(Class)} for itself and all extending classes, for example, when an enumeration class {@code Vehicle} is created with
   * two extending enumeration classes being {@code Car} and {@code Truck}, then the {@code init()} method of the {@code Vehicle} should
   * do: <pre>{@code
   * @Override
   * protected void init() {
   *   register(Vehicle.class);
   *   register(Car.class);
   *   register(Truck.class);
   * }
   * }</pre>
   * <p>This is needed to resolve the chicken-egg problem of the JAVA class loading mechanism. The order is not relevant.
   *
   * <h3>Details</h3>
   * <p>There is a chicken-egg problem in Java. A class is not loaded before it is needed and even when the class is loaded, it is not
   * directly initialized. In other words, when we create an enumeration class and make constants for possible value, the JAVA runtime will
   * not be aware of them unless we force it to load and initialize the class. This means, unless one of the constants are really used,
   * Jackson or other deserialization tools will not be able to deserialize the value. This can lead to serious errors. This initialization
   * method prevents this kind of error.
   */
  protected abstract void init();

  /**
   * Runs a lambda against this enumeration instance.
   *
   * @param selfClass Reference to the class of this enumeration-type.
   * @param lambda     The lambda to run against this instance.
   * @param <SELF>    The type of this.
   * @return this.
   */
  protected @NotNull <SELF extends JsonEnum> SELF with(@NotNull Class<SELF> selfClass, @NotNull P1<SELF> lambda) {
    lambda.call((SELF) this);
    return (SELF) this;
  }

  /**
   * Can be used with defined values to add aliases.
   *
   * @param selfClass Reference to the class of this enumeration-type.
   * @param value     The additional value to register.
   * @param <SELF>    The type of this.
   * @return this.
   * @throws Error if the given alias is already used.
   */
  protected <SELF extends JsonEnum> SELF alias(@NotNull Class<SELF> selfClass, @Nullable Object value) {
    if (this.getClass() != selfClass) {
      throw new Error("selfClass must refer to this class");
    }
    value = value(value);
    synchronized (JsonEnum.class) {
      final Class<? extends JsonEnum> rootClass = rootClass(selfClass);
      final ConcurrentHashMap<Object, JsonEnum> values = registry.get(rootClass);
      assert values != null;
      final JsonEnum existing = values.get(value);
      if (existing != null) {
        if (this == existing) {
          return (SELF) this;
        }
        throw new Error("The value " + value + " is already registered with another type: "
            + existing.getClass().getName());
      }
      values.put(value, this);
    }
    return (SELF) this;
  }

  /**
   * Can be used with defined values to add aliases that are not case-sensitive.
   *
   * @param selfClass Reference to the class of this enumeration-type.
   * @param value     The additional value to register.
   * @param <SELF>    The type of this.
   * @return this.
   * @throws Error if the given alias is already used.
   */
  protected <SELF extends JsonEnum> SELF aliasIgnoreCase(@NotNull Class<SELF> selfClass, @NotNull String value) {
    alias(selfClass, value);
    final String lowerCase = value.toLowerCase(Locale.ROOT);
    if (!lowerCase.equals(value)) {
      alias(selfClass, lowerCase);
    }
    return (SELF) this;
  }

  /**
   * Returns the POJO value of the enumeration value.
   *
   * @return the POJO value.
   */
  public @Nullable Object value() {
    return value;
  }

  @Override
  public int length() {
    return string.length();
  }

  @Override
  public char charAt(int index) {
    return string.charAt(index);
  }

  @Override
  public @NotNull CharSequence subSequence(int start, int end) {
    return string.subSequence(start, end);
  }

  @Override
  public final int hashCode() {
    return string.hashCode();
  }

  @Override
  public final boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof JsonEnum) {
      final JsonEnum otherEnum = (JsonEnum) other;
      if (otherEnum.getClass() != this.getClass()) {
        return false;
      }
      return Objects.equals(this.value, otherEnum.value);
    }
    return false;
  }

  /**
   * Returns the enumeration integer value. If the enumeration is a string, {@code -1} is returned.
   *
   * @return the enumeration integer value or {@code -1}.
   */
  public long toLong() {
    if (value instanceof Number) {
      Number number = (Number) value;
      return number.longValue();
    }
    return -1L;
  }

  /**
   * Returns the textual representation, works for all values.
   *
   * @return the textual representation, works for all values.
   */
  @Override
  public final @NotNull String toString() {
    return string;
  }
}
