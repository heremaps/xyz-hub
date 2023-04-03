package com.here.xyz.events;

import static com.here.xyz.events.QueryParameters.DEFAULT_CAPACITY;

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The query parameter as provided in the query string of a URI.
 */
public class QueryParameter extends ArrayList<@Nullable Object> {

  QueryParameter(@NotNull QueryParameters parent, @NotNull String key, @NotNull QueryOperator op, int index) {
    super(DEFAULT_CAPACITY);
    this.parent = parent;
    this.key = key;
    this.op = op;
    this.index = index;
  }

  final int index;
  final @NotNull QueryParameters parent;
  final @NotNull String key;
  @NotNull QueryOperator op;
  @Nullable QueryParameter next;

  /**
   * Returns the query parameters to which this parameter belongs.
   *
   * @return The query parameters to which this parameter belongs.
   */
  public @NotNull QueryParameters parent() {
    return parent;
  }

  /**
   * Returns the index of this query parameter.
   *
   * @return The index of this query parameter.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Returns the key of this query parameter.
   *
   * @return The key of this query parameter.
   */
  public @NotNull String getKey() {
    return key;
  }

  /**
   * Returns the operation of this query parameter.
   *
   * @return The operation of this query parameter.
   */
  public @NotNull QueryOperator getOp() {
    return op;
  }

  /**
   * Returns the first value.
   *
   * @return The first value; {@code null} if the parameter does not have a value.
   */
  public @Nullable Object first() {
    return get(0);
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

  private static boolean emptyAndString(@Nullable Object o) {
    return o instanceof String && ((String) o).length() == 0;
  }

  /**
   * Removes empty strings, then return the compacted values list. This will make the values a list that does not have empty strings and
   * therefore may be empty by itself.
   *
   * @return The values.
   */
  public @NotNull QueryParameter removeEmpty() {
    removeIf(QueryParameter::emptyAndString);
    return this;
  }

  /**
   * Sets the parameter to a single value.
   *
   * @param value The value to set.
   * @return this.
   */
  public @NotNull QueryParameter set(@NotNull Object value) {
    clear();
    add(value);
    return this;
  }

  /**
   * Replace the values list.
   *
   * @param values The new values list.
   * @return this.
   */
  public @NotNull QueryParameter setAll(@NotNull List<@NotNull String> values) {
    clear();
    addAll(values);
    return this;
  }

  /**
   * Set the parameter operation.
   *
   * @param op The parameter operation.
   * @return this.
   */
  public @NotNull QueryParameter setOp(@NotNull QueryOperator op) {
    this.op = op;
    return this;
  }

  /**
   * Tests whether another query parameter with the same {@link #getKey() key} exists.
   *
   * @return True if another query parameter with the same {@link #getKey() key} exists; false otherwise.
   */
  public boolean hasNext() {
    return next != null;
  }

  /**
   * Returns the next query parameter with the same {@link #getKey() key}. To iterate all parameters use {@link }
   *
   * @return The next query parameter with the same {@link #getKey() key}; {@code null} if no parameter with the same key exists.
   */
  public @Nullable QueryParameter next() {
    return next;
  }

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


  public boolean isBoolean(int n) {
    return getOrNull(n) instanceof Boolean;
  }

  public @Nullable Boolean getBoolean(int n) {
    final Object value = getOrNull(n);
    return (Boolean) (value instanceof Boolean ? value : null);
  }

  public boolean getBoolean(int n, boolean defaultValue) {
    final Boolean value = getBoolean(n);
    return value == null ? defaultValue : value;
  }

  /**
   * Cast this parameter list into a string list.
   *
   * <p>The method will not ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable String> asStringList() {
    return (List<String>) (List) this;
  }

  /**
   * Cast this parameter list into a long list.
   *
   * <p>The method will not ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Long> asLongList() {
    return (List<Long>) (List) this;
  }

  /**
   * Cast this parameter list into a long list.
   *
   * <p>The method will not ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Double> asDoubleList() {
    return (List<Double>) (List) this;
  }

  /**
   * Cast this parameter list into a long list.
   *
   * <p>The method will not ensure that the parameters really only contains the required type, therefore it can cause
   * {@link ClassCastException}'s to be thrown!
   *
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public @NotNull List<@Nullable Boolean> asBooleanList() {
    return (List<Boolean>) (List) this;
  }
}