package com.here.naksha.lib.core.models.payload.events;

import com.here.naksha.lib.core.exceptions.ParameterError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Standard implementation to keep track of query parameters.
 *
 * <p><b>Beware that this class is designed for a read-only use case</b>!
 */
public class QueryParameterList implements Iterable<QueryParameter> {

  static final int DEFAULT_CAPACITY = 4;

  /** Creates a new empty query parameter list. */
  public QueryParameterList() {
    map = new LinkedHashMap<>(16);
    list = new ArrayList<>(16);
    UNDEFINED = new QueryParameter(this, "", -1);
  }

  /**
   * Creates a new query parameter list from the given input string.
   *
   * @param query_string the query string without the question mark, for example {@code
   *     "foo=x&bar=y"}.
   * @throws ParameterError If parsing the query string failed.
   */
  public QueryParameterList(@Nullable CharSequence query_string) throws ParameterError {
    this(query_string, 0, query_string != null ? query_string.length() : 0);
  }

  /**
   * Creates a new query parameter list from the given input string.
   *
   * @param query_string the query string without the question mark, for example {@code
   *     "foo=x&bar=y"}.
   * @param decoder the decoder to use.
   * @throws ParameterError If parsing the query string failed.
   */
  public QueryParameterList(@Nullable CharSequence query_string, @Nullable QueryParameterDecoder decoder)
      throws ParameterError {
    this(query_string, 0, query_string != null ? query_string.length() : 0, decoder);
  }

  /**
   * Creates a new query parameter list from the given input string.
   *
   * @param query_string the query string without the question mark, for example {@code
   *     "foo=x&bar=y"}.
   * @param start the index of the first character to parse.
   * @param end the index of the last character to parse.
   * @throws ParameterError If parsing the query string failed.
   */
  public QueryParameterList(@Nullable CharSequence query_string, int start, int end) throws ParameterError {
    this(query_string, start, end, null);
  }

  /**
   * Creates a new query parameter list from the given input string.
   *
   * @param query_string the query string without the question mark, for example {@code
   *     "foo=x&bar=y"}.
   * @param start the index of the first character to parse.
   * @param end the index of the last character to parse.
   * @param decoder the decoder to use.
   * @throws ParameterError If parsing the query string failed.
   */
  public QueryParameterList(
      @Nullable CharSequence query_string, int start, int end, @Nullable QueryParameterDecoder decoder)
      throws ParameterError {
    map = new LinkedHashMap<>(16);
    list = new ArrayList<>(16);
    UNDEFINED = new QueryParameter(this, "", -1);
    if (query_string != null) {
      if (decoder == null) {
        decoder = new QueryParameterDecoder();
      }
      decoder.parse(this, query_string, start, end);
    }
  }

  /**
   * Invoked for every value that should be added to the parameter.
   *
   * @param param The parameter.
   * @param value The value to be added.
   * @param delimiter The delimiter that caused the creation of the value or {@code null}, if the
   *     end of the values reached (last value to be added).
   */
  protected void addValue(@NotNull QueryParameter param, @Nullable Object value, @Nullable QueryDelimiter delimiter) {
    param.values().add(value);
  }

  /** All query parameters as map. */
  public final @NotNull LinkedHashMap<@NotNull String, @NotNull QueryParameter> map;

  /** All query parameters as list. */
  public final @NotNull List<@NotNull QueryParameter> list;

  /** A cache for all joined query parameter. */
  private @Nullable HashMap<@NotNull String, @NotNull QueryParameter> joinedByKey;

  /**
   * The undefined query parameter as a helper, it has an empty key (""), is not part of the
   * parameter list and has the index minus 1.
   */
  public final @NotNull QueryParameter UNDEFINED;

  /**
   * Compacts the query parameters by removing all empty values.
   *
   * @return this.
   */
  public @NotNull QueryParameterList removeAllEmptyValues() {
    for (@NotNull QueryParameter param : this) {
      if (param.hasValues()) {
        param.values().removeEmpty();
      }
    }
    return this;
  }

  /**
   * Returns all values of all parameter with the given key.
   *
   * @param key The key of the parameter to query.
   * @return A list with all values of the parameter; {@code null} if no such parameter exists.
   */
  public @NotNull List<@NotNull Object> collectAllOf(@NotNull String key) {
    return collectAllOf(key, Integer.MAX_VALUE);
  }

  /**
   * Returns all values of all parameter with the given key.
   *
   * @param key The key of the parameter to query.
   * @param limit The maximal amount of parameters to collect from.
   * @return A list with all values of the parameter; {@code null} if no such parameter exists.
   */
  public @NotNull List<@NotNull Object> collectAllOf(@NotNull String key, int limit) {
    final List<@NotNull Object> all = new ArrayList<>(DEFAULT_CAPACITY);
    QueryParameter param = map.get(key);
    int i = 0;
    while (i++ < limit && param != null) {
      if (param.hasValues()) {
        all.addAll(param.values());
      }
      param = param.next;
    }
    return all;
  }

  /**
   * Convert the given name into a key. The default implementation is case-sensitive, but you could
   * for example make it case-insensitive by doing
   *
   * <pre>{@code return name.toLowerCase()}</pre>
   *
   * .
   *
   * @param name the name to convert.
   * @return The key of the given name.
   */
  public @NotNull String nameToKey(@NotNull String name) {
    return name;
  }

  /**
   * Returns the n'th parameter.
   *
   * @param n The parameter number.
   * @return The parameter; {@code null} if no such parameter exists.
   */
  public @Nullable QueryParameter get(int n) {
    if (n < 0 || n >= list.size()) {
      return null;
    }
    return list.get(n);
  }

  /**
   * Returns the first parameter with the given key.
   *
   * @param key The parameter key.
   * @return The first parameter; {@code null} if no such parameter exists.
   */
  public @Nullable QueryParameter get(@NotNull String key) {
    return map.get(key);
  }

  /**
   * Returns a joined query parameter representation. This joins the arguments, and the values of
   * all parameters with the same key. The returned value maybe a new copy, except there is only one
   * parameter with this key, then the parameter is returned.
   *
   * @param key The key of the query parameter to join.
   * @return the joined representation; if any.
   */
  public @Nullable QueryParameter join(@NotNull String key) {
    QueryParameter p;
    if (!contains(key)) {
      return null;
    }

    final HashMap<@NotNull String, @NotNull QueryParameter> cache =
        joinedByKey != null ? joinedByKey : (joinedByKey = new HashMap<>());
    p = cache.get(key);
    if (p != null) {
      // Cache hit.
      return p;
    }
    p = get(key);
    if (p == null) {
      // No such parameter.
      return null;
    }
    if (p.next == null) {
      // Only one parameter with that key, no need to join anything!
      return p;
    }

    final int index = p.index;
    final QueryParameter joined = new QueryParameter(this, key, index);
    while (p != null) {
      if (p.hasValues()) {
        joined.values().add(p.values());
      }
      if (p.hasArguments()) {
        joined.arguments().add(p.arguments());
      }
      p = p.next;
    }
    cache.put(key, joined);
    return joined;
  }

  /**
   * Returns the first parameter with the given name. This methods returns {@link #UNDEFINED} if no
   * such parameter exists, which allows a more simplified usage:
   *
   * <pre>{@code
   * final int value = params.getOrUndefined("foo").getInteger(0, -1);
   * }</pre>
   *
   * @param name The parameter name.
   * @return The first parameter; {@link #UNDEFINED} if no such parameter exists.
   */
  public @NotNull QueryParameter getOrUndefined(@NotNull String name) {
    final QueryParameter param = map.get(name);
    return param != null ? param : UNDEFINED;
  }

  /**
   * Returns the first value of the parameter with the given name.
   *
   * @param name The name of the parameter.
   * @return The first value of the parameter with the given name; {@code null} if no such parameter
   *     exists.
   */
  public @Nullable Object getValue(@NotNull String name) {
    final QueryParameter param = map.get(name);
    return param != null && param.hasValues() ? param.values().first() : null;
  }

  /**
   * Tests whether a parameter with the given key exists.
   *
   * @param key The key of the parameter.
   * @return true if such a parameter exists; false otherwise.
   */
  public boolean contains(@NotNull String key) {
    return map.containsKey(key);
  }

  /**
   * Tests whether any parameter exists.
   *
   * @return true if no parameter exists; false otherwise.
   */
  public boolean isEmpty() {
    return map.isEmpty();
  }

  /**
   * Returns all parameter names.
   *
   * @return All parameter names.
   */
  public @NotNull Set<@NotNull String> keys() {
    return map.keySet();
  }

  /**
   * Returns the total amount of parameters.
   *
   * @return The total amount of parameters.
   */
  public int size() {
    return list.size();
  }

  /**
   * Returns the amount of individual parameters (amount of keys).
   *
   * @return The amount of individual parameters.
   */
  public int keySize() {
    return map.size();
  }

  /**
   * Returns the amount of parameters with the given name.
   *
   * @param name The name of the parameter.
   * @return The amount of such parameters.
   */
  public int count(@NotNull String name) {
    int num = 0;
    QueryParameter queryParameter = map.get(name);
    while (queryParameter != null) {
      num++;
      queryParameter = queryParameter.next;
    }
    return num;
  }

  /**
   * Returns an entry-set above all parameters. The returned set contains the key, and the first
   * parameter for that key.
   *
   * @return The entry set.
   */
  public @NotNull Set<@NotNull Entry<@NotNull String, @NotNull QueryParameter>> entrySet() {
    return map.entrySet();
  }

  /**
   * Returns an iterator above all query parameters.
   *
   * @return The iterator above all query parameters.
   */
  public @NotNull Iterator<@NotNull QueryParameter> iterator() {
    return list.iterator();
  }
}
