package com.here.xyz.models.payload.events;

import static com.here.xyz.models.payload.events.QueryDelimiter.AT;
import static com.here.xyz.models.payload.events.QueryDelimiter.EQUAL;
import static com.here.xyz.models.payload.events.QueryDelimiter.EXCLAMATION_MARK;
import static com.here.xyz.models.payload.events.QueryDelimiter.GREATER;
import static com.here.xyz.models.payload.events.QueryDelimiter.SMALLER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A query operation.
 */
public final class QueryOperation {

  private static final ReentrantLock mutex = new ReentrantLock();
  private static final AtomicInteger maxLength = new AtomicInteger(0);
  private static final ConcurrentHashMap<@NotNull String, @NotNull QueryOperation> opsByName = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<@NotNull String, @NotNull QueryOperation> opsByDelimiters = new ConcurrentHashMap<>();

  private static @NotNull String delimitersToString(@NotNull QueryDelimiter[] delimiters) {
    final char[] d = new char[delimiters.length];
    for (int i = 0; i < delimiters.length; i++) {
      d[i] = delimiters[i].delimiterChar;
    }
    return new String(d);
  }

  /**
   * Creates a new query operation and register it.
   *
   * @param name       The text of the operation, must be unique.
   * @param delimiters The delimiters of the operation.
   */
  private QueryOperation(@NotNull String name, @NotNull QueryDelimiter @NotNull [] delimiters, @NotNull String delimiterString) {
    this.name = name;
    this.delimiters = delimiters;
    this.delimiterString = delimiterString;
  }

  /**
   * Create a new query operation and return it.
   *
   * @param name The name for which to generate the query operation.
   * @return the new query operation.
   * @throws NullPointerException If the name is {@code null}.
   */
  public static @NotNull QueryOperation put(@NotNull String name) {
    return store(name, true);
  }

  /**
   * Create a new query operation and return it.
   *
   * @param name       The name for which to generate the query operation.
   * @param delimiters The delimiter of the operation.
   * @return the new query operation.
   * @throws NullPointerException If the name or any delimiter being {@code null}.
   */
  public static @NotNull QueryOperation put(@NotNull String name, @NotNull QueryDelimiter... delimiters) {
    return store(name, true, delimiters);
  }

  /**
   * Create a new query operation and return it.
   *
   * @param name The name for which to generate the query operation.
   * @return the new query operation.
   * @throws IllegalArgumentException If the given name exists already, but with delimiters defined.
   * @throws NullPointerException     If the name is {@code null}.
   */
  public static @NotNull QueryOperation create(@NotNull String name) {
    return store(name, false);
  }

  /**
   * Create a new query operation and return it.
   *
   * @param name       The name for which to generate the query operation.
   * @param delimiters The delimiters of the operation.
   * @return the new query operation.
   * @throws IllegalArgumentException If the given name exists already, but with different delimiters defined.
   * @throws NullPointerException     If the name or delimiter being {@code null}.
   */
  public static @NotNull QueryOperation create(@NotNull String name, @NotNull QueryDelimiter... delimiters) {
    return store(name, false, delimiters);
  }

  /**
   * Remove the given operation from the supported operations list.
   *
   * @param name the operation to remove.
   * @return {@code true} if the operation removed; {@code false} otherwise.
   * @throws NullPointerException If the given name is null.
   */
  public static boolean remove(@NotNull String name) {
    final QueryOperation op = getByName(name);
    return op != null && remove(op);
  }

  /**
   * Remove the given operation from the supported operations list.
   *
   * @param op the operation to remove.
   * @return {@code true} if the operation removed; {@code false} otherwise.
   * @throws NullPointerException If the given operation is null.
   */
  public static boolean remove(@NotNull QueryOperation op) {
    mutex.lock();
    try {
      final QueryOperation existed = opsByName.remove(op.name);
      opsByDelimiters.remove(op.delimiterString);

      if (existed != null) {
        final Enumeration<@NotNull String> keys = opsByName.keys();
        int max_len = 0;
        while (keys.hasMoreElements()) {
          final String key = keys.nextElement();
          max_len = Math.max(max_len, key.length());
        }
        maxLength.set(max_len);
        return true;
      }
      return false;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Create a new query operation and return it.
   *
   * @param key        The key for which to generate the query operation.
   * @param replace    If an existing operation should be replaced.
   * @param delimiters The delimiters of the operation.
   * @return the new query operation.
   * @throws IllegalArgumentException If the given key exists already, but with different delimiters defined.
   * @throws NullPointerException     If the key or any delimiter being {@code null}.
   */
  private static @NotNull QueryOperation store(@NotNull String key, boolean replace, @NotNull QueryDelimiter... delimiters) {
    for (int i = 0; i < key.length(); i++) {
      final char c = key.charAt(i);
      if (c < 32 || c >= 128) {
        throw new IllegalArgumentException("The key of an operation must be an ASCII character, but no ASCII control character");
      }
    }
    mutex.lock();
    try {
      final String delimiterString = delimitersToString(delimiters);
      QueryOperation op = opsByName.get(key);
      if (op != null) {
        if (op.delimiterString.equals(delimiterString)) {
          return op;
        }
        if (!replace) {
          throw new IllegalArgumentException("The operation '" + key + "' exists already with different delimiters: " + op.delimiterString);
        }
        // Replace.
        opsByName.remove(key);
        opsByDelimiters.remove(delimiterString);
      }
      assert !opsByName.containsKey(key);
      assert !opsByDelimiters.containsKey(delimiterString);
      op = new QueryOperation(key, delimiters, delimiterString);
      opsByName.put(key, op);
      if (delimiterString.length() > 0) {
        opsByDelimiters.put(delimiterString, op);
      }
      maxLength.set(key.length());
      return op;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Returns the operation matching the given text or {@code null}, if no operation matches.
   *
   * @param text The text to match.
   * @return The operation or {@code null}, if no operation matches.
   */
  @JsonCreator
  public static @Nullable QueryOperation getByName(@Nullable String text) {
    return text != null ? opsByName.get(text) : null;
  }

  /**
   * Returns the operation matching the given delimiters or {@code null}, if no operation matches.
   *
   * @param delimiterString The delimiters as string to look-up.
   * @return The operation or {@code null}, if no operation matches.
   */
  public static @Nullable QueryOperation getByDelimiterString(@Nullable String delimiterString) {
    return delimiterString != null ? opsByDelimiters.get(delimiterString) : null;
  }

  /**
   * Returns the maximum length of all known operation texts. This is helpful to limit look ahead implementations.
   *
   * @return the maximum length of all known operation texts.
   */
  public static int maxLength() {
    return Math.max(2, maxLength.get());
  }

  /**
   * Returns an enumeration above the keys of all operations.
   *
   * @return The enumeration above the keys of all operations.
   */
  public static @NotNull Enumeration<@NotNull String> keys() {
    return opsByName.keys();
  }

  /**
   * The name of the query operation.
   */
  public final @NotNull String name;

  /**
   * The query delimiters that represent the operation; may be zero.
   */
  public final @NotNull QueryDelimiter @NotNull [] delimiters;

  /**
   * The query delimiters that represent the operation as string; may be empty.
   */
  public final @NotNull String delimiterString;

  /**
   * Tests if this query operation equals the given text.
   *
   * @param text The text to compare against.
   * @return {@code true} if the given text equals this operation; false otherwise.
   */
  public boolean equals(@NotNull String text) {
    return this.name.equals(text);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @JsonValue
  @Override
  public @NotNull String toString() {
    return name;
  }

  /**
   * Predefined NO operation.
   */
  public static final @NotNull QueryOperation NONE = put("");

  /**
   * Predefined equals operation.
   */
  public static final @NotNull QueryOperation EQUALS = put("eq", EQUAL);

  /**
   * Predefined not equals operation.
   */
  public static final @NotNull QueryOperation NOT_EQUALS = put("ne", EXCLAMATION_MARK, EQUAL);

  /**
   * Predefined greater than operation.
   */
  public static final @NotNull QueryOperation GREATER_THAN = put("gt", GREATER);

  /**
   * Predefined greater than or equals operation.
   */
  public static final @NotNull QueryOperation GREATER_THAN_OR_EQUALS = put("gte", GREATER, EQUAL);

  /**
   * Predefined less than operation.
   */
  public static final @NotNull QueryOperation LESS_THAN = put("lt", SMALLER);

  /**
   * Predefined less than or equals operation.
   */
  public static final @NotNull QueryOperation LESS_THAN_OR_EQUALS = put("lte", SMALLER, EQUAL);

  /**
   * Predefined contains operation.
   */
  public static final @NotNull QueryOperation CONTAINS = put("cs", AT, GREATER); // @>

  /**
   * Predefined is contained in operation.
   */
  public static final @NotNull QueryOperation IN = put("in", SMALLER, AT); // <@
}