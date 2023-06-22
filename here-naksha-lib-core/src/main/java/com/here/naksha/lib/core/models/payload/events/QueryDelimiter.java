package com.here.naksha.lib.core.models.payload.events;

import static com.here.naksha.lib.core.models.payload.events.QueryDelimiterType.GENERAL;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiterType.SUB;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiterType.UNSAFE;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** All delimiters following <a href="https://tools.ietf.org/html/rfc3986#section-2.2"></a>. */
public enum QueryDelimiter {
  // See: https://www.asciitable.com/

  // Delimiters.
  COLON(':', GENERAL),
  SLASH('/', GENERAL),
  QUESTION_MARK('?', GENERAL),
  SHARP('#', GENERAL),
  SQUARE_BRACKET_OPEN('[', GENERAL),
  SQUARE_BRACKET_CLOSE(']', GENERAL),
  AT('@', GENERAL),

  // Sub-Delimiter.
  EXCLAMATION_MARK('!', SUB),
  DOLLAR('$', SUB),
  AMPERSAND('&', SUB),
  SINGLE_QUOTE('\'', SUB),
  ROUND_BRACKET_OPEN('(', SUB),
  ROUND_BRACKET_CLOSE(')', SUB),
  STAR('*', SUB),
  PLUS('+', SUB),
  COMMA(',', SUB),
  SEMICOLON(';', SUB),
  EQUAL('=', SUB),

  // Unsafe
  DOUBLE_QUOTE('"', UNSAFE),
  SMALLER('<', UNSAFE),
  GREATER('>', UNSAFE),
  EXPONENT('^', UNSAFE),
  BACK_QUOTE('`', UNSAFE),
  CURLY_BRACKET_OPEN('{', UNSAFE),
  PIPE('|', UNSAFE),
  CURLY_BRACKET_CLOSE('}', UNSAFE),

  END((char) 0, UNSAFE);

  QueryDelimiter(char c, @NotNull QueryDelimiterType type) {
    this.delimiterChar = c;
    this.delimiterString = String.valueOf(c);
    this.type = type;
  }

  /**
   * The delimiter type following <a href="https://tools.ietf.org/html/rfc3986#section-2.2"></a>.
   */
  public final @NotNull QueryDelimiterType type;

  /** The character that represents this delimiter. */
  public final char delimiterChar;

  /** The character that represents this delimiter as string of length 1. */
  public final @NotNull String delimiterString;

  @Override
  public @NotNull String toString() {
    return delimiterString;
  }

  /**
   * Returns the delimiter for the given character.
   *
   * @param c The character for which to return the delimiter.
   * @return The delimiter or {@code null}, if no such delimiter exists.
   */
  public static @Nullable QueryDelimiter get(char c) {
    // This should cause 31 to become 65535 (basically we roll left) to only use one branch.
    c = (char) (c - 32);
    return (c < byChar.length) ? byChar[c] : null;
  }

  /**
   * Returns the delimiter for the given character.
   *
   * @param c The character for which to return the delimiter.
   * @param alternative The delimiter to return, the given value is no valid delimiter.
   * @return The delimiter or the alternative.
   */
  public static @NotNull QueryDelimiter get(char c, @NotNull QueryDelimiter alternative) {
    final QueryDelimiter d = get(c);
    return d != null ? d : alternative;
  }

  // The first 32 characters are only control characters that we do not support.
  private static final @Nullable QueryDelimiter @NotNull [] byChar = new QueryDelimiter[128 - 32];

  static {
    for (final @NotNull QueryDelimiter delimiter : QueryDelimiter.values()) {
      if (delimiter != END) {
        byChar[delimiter.delimiterChar - 32] = delimiter;
      }
    }
  }
}
