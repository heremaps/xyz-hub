package com.here.xyz.events;

import static com.here.xyz.events.QueryParameterList.DEFAULT_CAPACITY;

import com.here.xyz.util.ValueList;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The query parameter as provided in the query string of a URI. Every query parameter does have a name (which is given by the client), a
 * key (the meaning, which may or may not be case-sensitive), values and arguments. The general syntax, which is modifiable, is
 * {@code &{key|name}[[delimiter][argument]...][[delimiter][[value][delimiter]...]]}, for example {@code &Foo;bar=a,b,c,'3'}, which could
 * result in the name "{@code Foo}", the key "{@code foo}", one argument "{@code bar}" and four string values being
 * {@code "a", "b", "c" and "3"}.
 */
@SuppressWarnings("unused")
public class QueryParameter {

  public QueryParameter(@NotNull QueryParameterList parent, @NotNull String name, int index) {
    this.parent = parent;
    this.name = name;
    this.key = parent.nameToKey(name);
    this.index = index;
  }

  /**
   * The list to which this parameter belongs.
   */
  final @NotNull QueryParameterList parent;
  /**
   * The absolute index in the arguments list.
   */
  final int index;
  /**
   * The parsed arguments.
   */
  private @Nullable ValueList arguments;
  /**
   * The delimiter before each argument.
   */
  private @Nullable List<@NotNull QueryDelimiter> argumentsDelimiter;
  /**
   * The values.
   */
  private @Nullable ValueList values;
  /**
   * The delimiter behind each value.
   */
  private @Nullable List<@NotNull QueryDelimiter> valuesDelimiter;
  /**
   * The name as given by the client.
   */
  private final @NotNull String name;
  /**
   * The key for hash-map access.
   */
  private final @NotNull String key;
  /**
   * The delimiter that splits the name, key and arguments from the values.
   */
  @Nullable QueryDelimiter delimiter;
  /**
   * The next query parameter with the same key.
   */
  @Nullable QueryParameter next;

  /**
   * Returns the query parameter list to which this parameter belongs.
   *
   * @return The query parameter list to which this parameter belongs.
   */
  public @NotNull QueryParameterList parent() {
    return parent;
  }

  /**
   * Returns the index of this query parameter in the parameter list.
   *
   * @return The index of this query parameter in the parameter list.
   */
  public int index() {
    return index;
  }

  /**
   * Returns the name of this query parameter, in the exact notation as it appeared in the query string.
   *
   * @return The name of this query parameter.
   */
  public @NotNull String name() {
    return name;
  }

  /**
   * Returns the key of this query parameter, can differ from the notation in which it appears in the query string. For example, when the
   * parameters should be case-insensitive, then the key maybe in lower or upper case while the name will be the exact form as it appeared
   * in the original query string.
   *
   * @return The key of this query parameter.
   */
  public @NotNull String key() {
    return key;
  }

  /**
   * Returns {@code true} if the query parameter has arguments; {@code false} otherwise.
   *
   * @return {@code true} if the query parameter has arguments; {@code false} otherwise.
   */
  public boolean hasArguments() {
    return arguments != null && arguments.size() > 0;
  }

  /**
   * Returns the arguments.
   *
   * @return The arguments.
   */
  public @NotNull ValueList arguments() {
    ValueList arguments = this.arguments;
    if (arguments == null) {
      this.arguments = arguments = new ValueList(DEFAULT_CAPACITY);
    }
    return arguments;
  }

  /**
   * Returns the delimiters in-front of each argument.
   *
   * @return The delimiters in-front of each argument.
   */
  public @NotNull List<@NotNull QueryDelimiter> argumentsDelimiter() {
    List<@NotNull QueryDelimiter> delimiter = this.argumentsDelimiter;
    if (delimiter == null) {
      this.argumentsDelimiter = delimiter = new ArrayList<>(DEFAULT_CAPACITY);
    }
    return delimiter;
  }

  /**
   * Returns {@code true} if the query parameter has values; {@code false} otherwise.
   *
   * @return {@code true} if the query parameter has values; {@code false} otherwise.
   */
  public boolean hasValues() {
    return values != null && values.size() > 0;
  }

  /**
   * Returns the values.
   *
   * @return The values.
   */
  public @NotNull ValueList values() {
    ValueList values = this.values;
    if (values == null) {
      this.values = values = new ValueList(DEFAULT_CAPACITY);
    }
    return values;
  }

  /**
   * Returns the delimiters behind each value.
   *
   * @return The delimiters behind each value.
   */
  public @NotNull List<@NotNull QueryDelimiter> valuesDelimiter() {
    List<@NotNull QueryDelimiter> delimiter = this.valuesDelimiter;
    if (delimiter == null) {
      this.valuesDelimiter = delimiter = new ArrayList<>(DEFAULT_CAPACITY);
    }
    return delimiter;
  }

  /**
   * Returns the delimiter that separates the name, key and arguments from the values. Returns {@code null}, if no delimiter, which means no
   * values either.
   *
   * @return The delimiter that separates the name, key and arguments from the values.
   */
  public @Nullable QueryDelimiter delimiter() {
    return delimiter;
  }

  /**
   * Tests whether another query parameter with the same {@link #key() key} exists.
   *
   * @return True if another query parameter with the same {@link #key() key} exists; false otherwise.
   */
  public boolean hasNext() {
    return next != null;
  }

  /**
   * Returns the next query parameter with the same {@link #key() key}. To iterate all parameters use {@link }
   *
   * @return The next query parameter with the same {@link #key() key}; {@code null} if no parameter with the same key exists.
   */
  public @Nullable QueryParameter next() {
    return next;
  }
}