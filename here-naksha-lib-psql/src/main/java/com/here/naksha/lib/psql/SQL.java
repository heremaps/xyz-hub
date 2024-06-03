/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.here.naksha.lib.core.util.Hex;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.PrimitiveIterator.OfInt;
import org.jetbrains.annotations.NotNull;

/**
 * An SQL query builder for PostgresQL.
 */
@SuppressWarnings("unused")
public class SQL implements CharSequence {

  private static final boolean[] ESCAPE_IDENT = new boolean[128];
  private static final boolean[] ESCAPE_LITERAL = new boolean[128];

  static {
    Arrays.fill(ESCAPE_IDENT, true);
    for (int c = '0'; c <= '9'; c++) {
      ESCAPE_IDENT[c] = false;
    }
    for (int c = 'a'; c <= 'z'; c++) {
      ESCAPE_IDENT[c] = false;
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      ESCAPE_IDENT[c] = false;
    }
    ESCAPE_IDENT['_'] = false;

    Arrays.fill(ESCAPE_LITERAL, false);
    for (int i = 0; i < 32; i++) {
      ESCAPE_LITERAL[i] = true;
    }
    ESCAPE_LITERAL['\''] = true;
  }

  /**
   * Tests if the given identifier must be escaped.
   *
   * @param id the identifier to test.
   * @return true if the identifier must be escaped; false otherwise.
   */
  public static boolean shouldEscapeIdent(@NotNull CharSequence id) {
    for (int i = 0; i < id.length(); i++) {
      final char c = id.charAt(i);
      // We signal that every less than the space must be escaped. The escape method then will throw
      // an SQLException!
      if (c < 32 || c > 126 || ESCAPE_IDENT[c]) {
        return true;
      }
    }
    return false;
  }

  static void write_ident(@NotNull CharSequence chars, @NotNull StringBuilder sb) {
    // See: https://www.asciitable.com/
    // See: https://www.postgresql.org/docs/16/sql-syntax-lexical.html
    // We only allows characters between 32 (space) and 126 (~).
    for (int i = 0; i < chars.length(); i++) {
      final char c = chars.charAt(i);
      if (c < 32 || c > 126) {
        throw unchecked(new SQLException("Illegal character in identifier: " + chars));
      }
      if (c == '"' || c == '\\') {
        sb.append(c).append(c);
      } else {
        sb.append(c);
      }
    }
  }

  public static void open_literal(@NotNull StringBuilder sb) {
    sb.append('E').append('\'');
  }

  // Requires an opening with E'
  @SuppressWarnings("DuplicatedCode")
  public static void write_literal(@NotNull StringBuilder sb, @NotNull CharSequence chars) {
    // See: https://www.asciitable.com/
    // See: https://www.postgresql.org/docs/16/sql-syntax-lexical.html
    // We do not escape normal ASCII characters (32 (space) to 126 (~)).
    final OfInt it = chars.codePoints().iterator();
    while (it.hasNext()) {
      final int codePoint = it.next();
      if (codePoint == 0) {
        throw new IllegalArgumentException("ASCII zero is not allowed in a literal");
      }
      if (codePoint < 128 && !ESCAPE_LITERAL[codePoint]) {
        sb.append((char) codePoint);
        continue;
      }
      if (codePoint == '\\' || codePoint == '\'') {
        sb.append((char) codePoint).append((char) codePoint);
        continue;
      }
      if (codePoint == '\t') {
        sb.append('\\').append('t');
        continue;
      }
      if (codePoint == '\r') {
        sb.append('\\').append('r');
        continue;
      }
      if (codePoint == '\n') {
        sb.append('\\').append('n');
        continue;
      }
      if (codePoint == '\b') {
        sb.append('\\').append('b');
        continue;
      }
      if (codePoint == '\f') {
        sb.append('\\').append('f');
        continue;
      }
      if (codePoint < 256) {
        sb.append('\\').append('x');
        sb.append(Hex.valueToChar[(codePoint >>> 4) & 15]);
        sb.append(Hex.valueToChar[codePoint & 15]);
        continue;
      }
      if (codePoint < 65536) {
        sb.append('\\').append('u');
        sb.append(Hex.valueToChar[(codePoint >>> 12) & 15]);
        sb.append(Hex.valueToChar[(codePoint >>> 8) & 15]);
        sb.append(Hex.valueToChar[(codePoint >>> 4) & 15]);
        sb.append(Hex.valueToChar[codePoint & 15]);
        continue;
      }
      sb.append('\\').append('U');
      sb.append(Hex.valueToChar[(codePoint >>> 28) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 24) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 20) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 16) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 12) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 8) & 15]);
      sb.append(Hex.valueToChar[(codePoint >>> 4) & 15]);
      sb.append(Hex.valueToChar[codePoint & 15]);
    }
  }

  public static void close_literal(@NotNull StringBuilder sb) {
    sb.append('\'');
  }

  public static void quote_literal(@NotNull StringBuilder sb, @NotNull CharSequence literal) {
    open_literal(sb);
    write_literal(sb, literal);
    close_literal(sb);
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull String quote_ident(@NotNull CharSequence id) {
    return quote_ident(new StringBuilder(), id).toString();
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param sb The string builder into which to write the escaped identifier.
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public static @NotNull StringBuilder quote_ident(@NotNull StringBuilder sb, @NotNull CharSequence id) {
    sb.append('"');
    write_ident(id, sb);
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
  public static @NotNull String quote_ident(@NotNull CharSequence... ids) {
    return quote_ident(new StringBuilder(), ids).toString();
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
  public static @NotNull StringBuilder quote_ident(@NotNull StringBuilder sb, @NotNull CharSequence... ids) {
    sb.append('"');
    for (final CharSequence id : ids) {
      write_ident(id, sb);
    }
    sb.append('"');
    return sb;
  }

  /**
   * Create a new SQL builder.
   *
   * @param q The query with which to start the builder.
   */
  public SQL(@NotNull String q) {
    this.sb = new StringBuilder();
    sb.append(q);
  }

  /**
   * Create a new SQL builder.
   *
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
   * Changes the length.
   *
   * @param length The new length to set.
   * @return this.
   */
  public @NotNull SQL setLength(int length) {
    sb.setLength(length);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param cs The characters sequence to append.
   * @return this.
   */
  public @NotNull SQL add(@NotNull CharSequence cs) {
    sb.append(cs);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param intNumber The integral number.
   * @return this.
   */
  public @NotNull SQL add(int intNumber) {
    sb.append(intNumber);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param character The character to add.
   * @return this.
   */
  public @NotNull SQL add(char character) {
    sb.append(character);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param intNumber The integral number.
   * @return this.
   */
  public @NotNull SQL add(long intNumber) {
    sb.append(intNumber);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param floatNumber The floating point number.
   * @return this.
   */
  public @NotNull SQL add(float floatNumber) {
    sb.append(floatNumber);
    return this;
  }

  /**
   * Append some raw SQL statement part.
   *
   * @param floatNumber The floating point number.
   * @return this.
   */
  public @NotNull SQL add(double floatNumber) {
    sb.append(floatNumber);
    return this;
  }

  /**
   * Escape the given PostgresQL identifier.
   *
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  public @NotNull SQL addIdent(@NotNull CharSequence id) {
    if (!shouldEscapeIdent(id)) {
      sb.append(id);
    } else {
      quote_ident(sb, id);
    }
    return this;
  }

  /**
   * Escape all given PostgresQL identifier parts together as one identifier, not individually, for example:
   *
   * <pre>{@code
   * sql.addIdent("hello", "_", "world");
   * }</pre>
   * <p>
   * will result in the code generated being: {@code "hello_world"}.
   *
   * @param parts The parts of the identifier to escape.
   * @return The given string builder.
   * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
   */
  public @NotNull SQL addIdent(@NotNull CharSequence... parts) {
    quote_ident(sb, parts);
    return this;
  }

  /**
   * Add the given literal, optionally quoted.
   * @param literal The literal to add.
   * @return this.
   */
  public @NotNull SQL addLiteral(@NotNull CharSequence literal) {
    open_literal(sb);
    write_literal(sb, literal);
    close_literal(sb);
    return this;
  }

  /**
   * Add the given literal, optionally quoted.
   * @param parts The literal parts to add as one literal.
   * @return this.
   */
  public @NotNull SQL addLiteral(@NotNull CharSequence... parts) {
    open_literal(sb);
    for (final CharSequence part : parts) {
      write_literal(sb, part);
    }
    close_literal(sb);
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
