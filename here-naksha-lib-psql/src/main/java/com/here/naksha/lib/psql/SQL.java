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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import java.sql.SQLException;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;

/**
 * An SQL query builder for PostgresQL.
 */
@SuppressWarnings("unused")
public class SQL implements CharSequence {

  private static final boolean[] ESCAPE = new boolean[128];

  static {
    Arrays.fill(ESCAPE, true);
    for (int c = '0'; c <= '9'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'a'; c <= 'z'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      ESCAPE[c] = false;
    }
    ESCAPE['_'] = false;
  }

  /**
   * Tests if the given identifier must be escaped.
   *
   * @param id the identifier to test.
   * @return true if the identifier must be escaped; false otherwise.
   */
  public static boolean shouldEscape(@NotNull CharSequence id) {
    for (int i = 0; i < id.length(); i++) {
      final char c = id.charAt(i);
      // We signal that every less than the space must be escaped. The escape method then will throw
      // an SQLException!
      if (c < 32 || c > 126 || ESCAPE[c]) {
        return true;
      }
    }
    return false;
  }

  static void escapeWrite(@NotNull CharSequence chars, @NotNull StringBuilder sb) {
    // See: https://www.asciitable.com/
    // We only allows characters between 32 (space) and 126 (~).
    for (int i = 0; i < chars.length(); i++) {
      final char c = chars.charAt(i);
      if (c < 32 || c > 126) {
        throw unchecked(new SQLException("Illegal character in identifier: " + chars));
      }
      if (c == '"') {
        sb.append('"').append('"');
      } else {
        sb.append(c);
      }
    }
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull String escapeId(@NotNull CharSequence id) {
    return escapeId(new StringBuilder(), id).toString();
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param sb The string builder into which to write the escaped identifier.
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence id) {
    sb.append('"');
    escapeWrite(id, sb);
    sb.append('"');
    return sb;
  }

  /**
   * Escape all given PostgresQL identifiers together, not individually, for example:
   *
   * <pre>{@code
   * String prefix = "hello";
   * String postfix = "world";
   * escapeId(prefix, "_", postfix);
   * }</pre>
   * <p>
   * will result in the code generated being: {@code "hello_world"}.
   *
   * @param ids The identifiers to escape.
   * @return The given string builder.
   * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull String escapeId(@NotNull CharSequence... ids) {
    return escapeId(new StringBuilder(), ids).toString();
  }

  /**
   * Escape all given PostgresQL identifiers together, not individually, for example:
   *
   * <pre>{@code
   * String prefix = "hello";
   * String postfix = "world";
   * escapeId(prefix, "_", postfix);
   * }</pre>
   * <p>
   * will result in the code generated being: {@code "hello_world"}.
   *
   * @param sb  The string builder into which to write the escaped identifiers.
   * @param ids The identifiers to escape.
   * @return The given string builder.
   * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence... ids) {
    sb.append('"');
    for (final CharSequence id : ids) {
      escapeWrite(id, sb);
    }
    sb.append('"');
    return sb;
  }

  /**
   * Create a new SQL builder.
   * @param q The query with which to start the builder.
   */
  public SQL(@NotNull String q) {
    this.sb = new StringBuilder();
    sb.append(q);
  }

  /**
   * Create a new SQL builder.
   * @param sb the string builder to use.
   */
  public SQL(@NotNull StringBuilder sb) {
    this.sb = sb;
  }

  /**
   * Create a new SQL builder using a dedicated string builder.
   */
  public SQL() {
    this.sb = new StringBuilder();
  }

  /**
   * Append some raw SQL statement part.
   * @param cs The characters sequence to append.
   * @return this.
   */
  public @NotNull SQL append(@NotNull CharSequence cs) {
    sb.append(cs);
    return this;
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public @NotNull SQL escape(@NotNull CharSequence id) {
    escapeId(sb, id);
    return this;
  }

  /**
   * Escape all given PostgresQL identifiers together, not individually, for example:
   *
   * <pre>{@code
   * String prefix = "hello";
   * String postfix = "world";
   * escapeId(prefix, "_", postfix);
   * }</pre>
   * <p>
   * will result in the code generated being: {@code "hello_world"}.
   *
   * @param ids The identifiers to escape.
   * @return The given string builder.
   * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
   */
  public @NotNull SQL escape(@NotNull CharSequence... ids) {
    escapeId(sb, ids);
    return this;
  }

  private final @NotNull StringBuilder sb;

  @Override
  public int length() {
    return sb.length();
  }

  @Override
  public char charAt(int index) {
    return sb.charAt(index);
  }

  @NotNull
  @Override
  public CharSequence subSequence(int start, int end) {
    return sb.subSequence(start, end);
  }

  @Override
  public @NotNull String toString() {
    return sb.toString();
  }
}
