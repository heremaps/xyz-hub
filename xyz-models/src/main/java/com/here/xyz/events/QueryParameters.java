package com.here.xyz.events;

import static com.here.xyz.events.QueryDelimiter.AMPERSAND;
import static com.here.xyz.events.QueryDelimiter.COMMA;
import static com.here.xyz.events.QueryDelimiter.EQUAL;
import static com.here.xyz.events.QueryDelimiter.ROUND_BRACKET_CLOSE;
import static com.here.xyz.events.QueryDelimiter.ROUND_BRACKET_OPEN;

import com.here.xyz.exceptions.ParameterFormatError;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.responses.XyzError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Standard implementation to parse query parameters from a query string. Can be used with any application. This implementation supports
 * tripple parameters. The ampersand (&amp;) is the separator, the comma is the value separator, and the colon is the operation separator
 * and the expander character.
 * <p>
 * <b>Beware that this class is designed for a read-only use case</b>!
 */
public class QueryParameters implements Iterable<QueryParameter> {

  /**
   * Helper method to compare two char sequences.
   *
   * @param a The first character sequence.
   * @param b The second character sequence.
   * @return true if they are equal; false otherwise.
   */
  public static boolean equals(@NotNull CharSequence a, @NotNull CharSequence b) {
    final int length = a.length();
    if (length != b.length()) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (a.charAt(i) != b.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  static final int DEFAULT_CAPACITY = 4;

  /**
   * Creates a new query parameters map.
   *
   * @param query the query string without the question mark, for example {@code "foo=x&bar=y"}.
   * @throws XyzErrorException If parsing the query string failed.
   */
  public QueryParameters(@Nullable CharSequence query) throws XyzErrorException {
    this(query, 0, query != null ? query.length() : 0);
  }

  /**
   * Returns the expected value type for the given key or {@link QueryParameterType#ANY}, if any value is acceptable (default).
   *
   * @param key   The key for which to return the expected class of the type.
   * @param index The index of the value, starts with 0 for the first value.
   * @return The value type.
   */
  protected @NotNull QueryParameterType typeOfValue(final @NotNull String key, final int index) {
    return QueryParameterType.ANY;
  }

  /**
   * Invoked after a query parameter has been parsed and before parsing the next one. Can reject a parameter value, for example if only
   * specific values are allows or can set a default value.
   *
   * @param parameter The parameter to validate.
   * @throws XyzErrorException if the validation failed.
   */
  protected void validateAndSetDefault(@NotNull QueryParameter parameter) throws XyzErrorException {
  }

  /**
   * Can be overridden to expand keys and values or implement special handling for delimiters. The method is invoked whenever a not URL
   * encoded delimiter is found in a query string. The string builder will hold the string parsed so without the delimiter character, for
   * example for the query string "&p:foo=hello" the method is invoked when the string builder holds "p". The delimiter is given as
   * argument.
   * <p>
   * If the method returns {@code true} it will abort the query parsing and the content of the string builder, after returning, will be used
   * as value. If the method returns {@code false}, then the parser continues the parsing.
   * <p>
   * <b>NOTE</b>: The parser will <b>NOT</b> add the delimiter to the string builder, no matter what the method returns. Therefore, the
   * method must do this, if wanted; the method may modify the content of the string builder.
   *
   * @param key    The key or {@code null}, if the parser is currently parsing a key.
   * @param op     The operator or {@code null}, if the parser is currently parsing the operator.
   * @param index  The index of the parameter.
   * @param number Only valid when a value is parsed (key and op are not {@code null}), then the value number.
   * @param quoted If the value is single-quoted.
   * @param sb     The string builder with the characters parsed so far (including the delimiter).
   * @return {@code true} to abort the parsing; {@code false} to continue parsing.
   */
  protected boolean stopOnDelimiter(final @Nullable String key, @Nullable QueryOperator op,
      final int index, final int number,
      final boolean quoted, final @NotNull QueryDelimiter delimiter, final @NotNull StringBuilder sb) {
    // Syntax: &{name}[({op}[)]][=[']{value}['][,...]]
    // Examples: &foo(gte)=5
    //           &foo(exists)&...
    //           &foo=5
    //           &foo=5,6
    //           &foo='5',6
    if (key == null) {
      if (delimiter == ROUND_BRACKET_OPEN || delimiter == EQUAL || delimiter == AMPERSAND) {
        return true;
      }
    } else if (op == null) { // &key({op}[)=&]
      if (delimiter == ROUND_BRACKET_CLOSE) {
        // Just remove the closing round bracket, it is syntactic sugar!
        // Therefore: "&foo(gte=5" is the same as "&foo(gte)=5"
        return false;
      }
      if (delimiter == EQUAL || delimiter == AMPERSAND) {
        return true;
      }
    } else { // &key={value}[,{value}...][&]
      if (delimiter == AMPERSAND || delimiter == COMMA) {
        return true;
      }
    }
    sb.append(delimiter.delimiterChar);
    return false;
  }

  /**
   * If the delimiter should be added into the value list of the parameter; defaults to no.
   *
   * @param parameter The parameter.
   * @param delimiter The delimiter.
   * @return {@code true} to add the delimiter into the parameter list; {@code false} to ignore it.
   */
  protected boolean addDelimiter(@NotNull QueryParameter parameter, @NotNull QueryDelimiter delimiter) {
    return false;
  }

  /**
   * Creates a new query parameters map.
   *
   * @param query the query string without the question mark, for example {@code "foo=x&bar=y"}.
   * @param start the index of the first character to parse.
   * @param end   the index of the last character to parse.
   * @throws XyzErrorException If parsing the query string failed.
   */
  public QueryParameters(@Nullable CharSequence query, int start, int end) throws XyzErrorException {
    map = new HashMap<>(16);
    list = new ArrayList<>(16);
    UNDEFINED = new QueryParameter(this, "", QueryOperator.NONE, -1);
    if (query == null) {
      return;
    }
    final QueryUrlDecoder decoder = new QueryUrlDecoder(this, query, start, end);
    int index = 0;
    try {
      while (decoder.hasNext()) {
        final String key = decoder.nextKey(index);

        QueryOperator op = null;
        if (decoder.delimiter == ROUND_BRACKET_OPEN) {
          if (!decoder.hasNext()) { // &key:
            throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Unexpected end of operator declaration for key: " + key);
          }
          final String opText = decoder.nextOp(key, index);
          if (opText.length() == 0) { // Empty operation is assignment, e.g. "&key:={value}".
            op = QueryOperator.ASSIGN;
          } else {
            // &key:{op}={value}
            try {
              op = QueryOperator.get(opText);
            } catch (IllegalArgumentException e) {
              throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Unknown operator '" + opText + "' for key: " + key);
            }
          }
        }

        @NotNull QueryParameter param;
        if (decoder.delimiter == EQUAL) {
          param = new QueryParameter(this, key, op != null ? op : QueryOperator.EQUALS, index);
          if (decoder.hasNext()) {
            do {
              final Object value = decoder.nextValue(key, param.op, index, param.size());
              param.add(value);
              if (decoder.delimiter != null && decoder.delimiter != AMPERSAND && addDelimiter(param, decoder.delimiter)) {
                param.add(decoder.delimiter);
              }
            } while (decoder.hasNext() && decoder.delimiter != AMPERSAND);
          }
        } else if (decoder.hasNext() && decoder.delimiter != AMPERSAND) {
          throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT,
              "Illegal sub-delimiter (" + decoder.delimiter + ") found after key: " + key);
        } else { // For example: "&key", "&key&next", "&key:exists", ...
          param = new QueryParameter(this, key, QueryOperator.NONE, index);
        }

        validateAndSetDefault(param);
        list.add(param);
        QueryParameter existing = map.putIfAbsent(key, param);
        if (existing == null) {
          map.put(key, param);
        } else {
          while (existing.next != null) {
            existing = existing.next;
          }
          existing.next = param;
        }
        index++;
      }
    } catch (ParameterFormatError e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  /**
   * All query parameters as map.
   */
  public final @NotNull HashMap<@NotNull String, @NotNull QueryParameter> map;

  /**
   * All query parameters as list.
   */
  public final @NotNull List<@NotNull QueryParameter> list;

  /**
   * A cache for all joined query parameter.
   */
  private @Nullable HashMap<@NotNull String, @NotNull QueryParameter> joined;

  /**
   * The undefined query parameter as a helper, it has an empty key (""), is not part of the parameter list and has the index minus 1.
   */
  public final @NotNull QueryParameter UNDEFINED;

  /**
   * Compacts the query parameters by removing all empty values.
   *
   * @return this.
   */
  public @NotNull QueryParameters removeAllEmptyValues() {
    for (@NotNull QueryParameter param : this) {
      param.removeEmpty();
    }
    return this;
  }

  /**
   * Returns all values of all parameter with the given name.
   *
   * @param name The name of the parameter to query.
   * @return A list with all values of the parameter; {@code null} if no such parameter exists.
   */
  public @NotNull List<@NotNull Object> collectAllOf(@NotNull String name) {
    return collectAllOf(name, Integer.MAX_VALUE);
  }

  /**
   * Returns all values of all parameter with the given name.
   *
   * @param name  The name of the parameter to query.
   * @param limit The maximal amount of parameters to collect from.
   * @return A list with all values of the parameter; {@code null} if no such parameter exists.
   */
  public @NotNull List<@NotNull Object> collectAllOf(@NotNull String name, int limit) {
    final List<@NotNull Object> all = new ArrayList<>(DEFAULT_CAPACITY);
    QueryParameter param = map.get(name);
    int i = 0;
    while (i++ < limit && param != null) {
      all.addAll(param);
      param = param.next;
    }
    return all;
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
   * Returns the first parameter with the given name.
   *
   * @param name The parameter name.
   * @return The first parameter; {@code null} if no such parameter exists.
   */
  public @Nullable QueryParameter get(@NotNull String name) {
    return map.get(name);
  }

  /**
   * Returns a joined query parameter representation.
   *
   * @param name The name of the query parameter to join.
   * @return the joined representation; if any.
   */
  public @Nullable QueryParameter join(@NotNull String name) {
    QueryParameter p;
    if (joined != null) {
      p = joined.get(name);
      if (p != null) {
        // Cache hit!
        return p;
      }
    }
    p = get(name);
    if (p == null) {
      // No such parameter.
      return null;
    }
    if (p.next == null) {
      // Only one parameter with that name!
      return p;
    }
    // Join the parameter, use the operation of the first parameter.
    final QueryParameter j = new QueryParameter(this, name, p.op, -1);
    while (p != null) {
      j.add(p);
      p = p.next;
    }
    joined.put(j.key, j); // Cache it!
    return j;
  }

  /**
   * Returns the first parameter with the given name. This methods returns {@link #UNDEFINED} if no such parameter exists, which allows
   * a more simplified usage: <pre>{@code
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
   * @return The first value of the parameter with the given name; {@code null} if no such parameter exists.
   */
  public @Nullable Object getValue(@NotNull String name) {
    final QueryParameter param = map.get(name);
    return param != null ? param.first() : null;
  }

  /**
   * Returns the operation of the first parameter with the given name.
   *
   * @param name the name of the parameter.
   * @return The operation; {@code null} if no such parameter exists.
   */
  public @Nullable QueryOperator getOp(@NotNull String name) {
    final QueryParameter param = map.get(name);
    return param != null ? param.getOp() : null;
  }

  /**
   * Tests whether a parameter with the given name exists.
   *
   * @param name The name of the parameter.
   * @return true if such a parameter exists; false otherwise.
   */
  public boolean contains(@NotNull String name) {
    return map.containsKey(name);
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
   * Returns an entry-set above all parameters. The returned set contains the key, and the first parameter for that key.
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