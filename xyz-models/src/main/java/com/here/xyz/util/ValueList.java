package com.here.xyz.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A list implementation that holds mixed values of the type {@link Boolean}, {@link Long}, {@link Double} or {@link String}.
 */
public class ValueList extends ArrayList<@Nullable Object> {

  public ValueList(int initialCapacity) {
    super(initialCapacity);
  }

  public ValueList() {
  }

  public ValueList(@Nonnull Collection<?> c) {
    super(c);
  }

  /**
   * Returns the first value.
   *
   * @return The first value; {@code null} if the parameter does not have a value.
   */
  public @Nullable Object first() {
    return getOrNull(0);
  }

  /**
   * Return the n'th value.
   *
   * @param n n.
   * @return The n'th value; {@code null} if no such value exists.
   */
  public @Nullable Object getOrNull(int n) {
    return n >= 0 && n < size() ? get(n) : null;
  }

  /**
   * Return the n'th value, if it is of the expected type.
   *
   * @param n          The index to query.
   * @param valueClass The class of the return-type.
   * @return Either the value or {@code null}, if the index is out of bounds, or the value is not of the expected type.
   */
  public <T> @Nullable T getAs(int n, @NotNull Class<T> valueClass) {
    final Object value = getOrNull(n);
    if (valueClass.isInstance(value)) {
      return valueClass.cast(value);
    }
    return null;
  }

  /**
   * Return the n'th value if it is of the same type as the given alternative; otherwise the given alternative.
   *
   * @param n           The index to query.
   * @param alternative The alternative value.
   * @return Either the n'th value, if of the same type as the given alternative or the given alternative.
   */
  public <T> @NotNull T getOr(int n, @NotNull T alternative) {
    //noinspection unchecked
    final Class<T> valueClass = (Class<T>) alternative.getClass();
    final Object value = getOrNull(n);
    if (valueClass.isInstance(value)) {
      return valueClass.cast(value);
    }
    return alternative;
  }

  private static boolean isEmptyAndString(@Nullable Object o) {
    return o instanceof String && ((String) o).length() == 0;
  }

  /**
   * Removes empty strings, then return the compacted values list. This will make the values a list that does not have empty strings and
   * therefore may be empty by itself.
   */
  public void removeEmpty() {
    removeIf(ValueList::isEmptyAndString);
  }

  /**
   * Sets the parameter to a single value.
   *
   * @param value The value to set.
   */
  public void set(@NotNull Object value) {
    clear();
    add(value);
  }

  /**
   * Replace the values list.
   *
   * @param values The new values list.
   */
  public void setAll(@NotNull List<@NotNull String> values) {
    clear();
    addAll(values);
  }

  /**
   * Tests whether the n'th parameter is empty (so either {@code null} or an empty string).
   *
   * @param n The parameter position.
   * @return {@code true} if the parameter is empty; {@code false} otherwise.
   */
  public boolean isEmpty(int n) {
    final Object value = getOrNull(n);
    return value == null || ((value instanceof String) && ((String) value).isEmpty());
  }

  /**
   * Test if the value is explicitly a string.
   *
   * @param n The parameter number.
   * @return {@code true} if the value is explicitly a string; {@code false} otherwise.
   */
  public boolean isString(int n) {
    return getOrNull(n) instanceof String;
  }

  public @Nullable String getString(int n) {
    final Object value = getOrNull(n);
    return (String) (value instanceof String ? value : null);
  }

  public @NotNull String getString(int n, @NotNull String defaultValue) {
    final String value = getString(n);
    return value == null ? defaultValue : value;
  }

  /**
   * Test if the value is explicitly a number.
   *
   * @param n The parameter number.
   * @return {@code true} if the value is explicitly a number; {@code false} otherwise.
   */
  public boolean isNumber(int n) {
    return getOrNull(n) instanceof Number;
  }

  public @Nullable Number getNumber(int n) {
    final Object value = getOrNull(n);
    return (Number) (value instanceof Number ? value : null);
  }

  /**
   * Test if the value is explicitly a long.
   *
   * @param n The parameter number.
   * @return {@code true} if the value is explicitly a long; {@code false} otherwise.
   */
  public boolean isLong(int n) {
    return getOrNull(n) instanceof Long;
  }

  public @Nullable Long getLong(int n) {
    final Object value = getOrNull(n);
    return (Long) (value instanceof Long ? value : null);
  }

  public long getLong(int n, long defaultValue) {
    final Long value = getLong(n);
    return value == null ? defaultValue : value;
  }

  /**
   * Test if the value is explicitly a double.
   *
   * @param n The parameter number.
   * @return {@code true} if the value is explicitly a double; {@code false} otherwise.
   */
  public boolean isDouble(int n) {
    return getOrNull(n) instanceof Double;
  }

  public @Nullable Double getDouble(int n) {
    final Object value = getOrNull(n);
    return (Double) (value instanceof Double ? value : null);
  }

  public double getDouble(int n, double defaultValue) {
    final Double value = getDouble(n);
    return value == null ? defaultValue : value;
  }

  /**
   * Test if the value is explicitly boolean.
   *
   * @param n The parameter number.
   * @return {@code true} if the value is explicitly boolean; {@code false} otherwise.
   */
  public boolean isBoolean(int n) {
    return getOrNull(n) instanceof Boolean;
  }

  public @Nullable Boolean getBoolean(int n) {
    final Object value = getOrNull(n);
    return (Boolean) (value instanceof Boolean ? value : null);
  }

  /**
   * Cast the value at the n'th position to boolean.
   *
   * @param n            The position.
   * @param defaultValue The value to return, when a cast is impossible.
   * @return The value.
   */
  public @Nullable Boolean toBoolean(int n, boolean defaultValue) {
    final Object value = getOrNull(n);
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      final String v = (String) value;
      return !"false".equalsIgnoreCase(v);
    }
    if (value instanceof Long) {
      return ((Long) value) != 0L;
    }
    if (value instanceof Double) {
      return ((Double) value) != 0d;
    }
    return defaultValue;
  }

  public boolean getBoolean(int n, boolean defaultValue) {
    final Boolean value = getBoolean(n);
    return value == null ? defaultValue : value;
  }

  /**
   * Cast the values of this parameter into a string list.
   *
   * <p>The method will <b>NOT</b> ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable String> asStringList() {
    return (List<String>) (List) this;
  }

  /**
   * Cast the values of this parameter into a long list.
   *
   * <p>The method will <b>NOT</b> ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Long> asLongList() {
    return (List<Long>) (List) this;
  }

  /**
   * Cast the values of this parameter into a double list.
   *
   * <p>The method will <b>NOT</b> ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Double> asDoubleList() {
    return (List<Double>) (List) this;
  }

  /**
   * Cast the values of this parameter into a boolean list.
   *
   * <p>The method will <b>NOT</b> ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Boolean> asBooleanList() {
    return (List<Boolean>) (List) this;
  }
}
