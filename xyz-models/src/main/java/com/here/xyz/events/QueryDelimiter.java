package com.here.xyz.events;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All delimiters following <a href="https://tools.ietf.org/html/rfc3986#section-2.2"></a>.
 */
public enum QueryDelimiter {
  // Delimiters.
  COLON(':', false),
  SLASH('/', false),
  QUESTION_MARK('?', false),
  SHARP('#', false),
  SQUARE_BRACKET_OPEN('[', false),
  SQUARE_BRACKET_CLOSE(']', false),
  AT('@', false),

  // Sub-Delimiter.
  EXCLAMATION_MARK('!', true),
  DOLLAR('$', true),
  AMPERSAND('&', true),
  SINGLE_QUOTE('\'', true),
  ROUND_BRACKET_OPEN('(', true),
  ROUND_BRACKET_CLOSE(')', true),
  STAR('*', true),
  PLUS('+', true),
  COMMA(',', true),
  SEMICOLON(';', true),
  EQUAL('=', true),
  END((char) 0, false);

  QueryDelimiter(char c, boolean isSub) {
    this.delimiterChar = c;
    this.delimiterString = String.valueOf(c);
    this.isSubDelimiter = isSub;
  }

  /**
   * True if this is a sub-delimiter following <a href="https://tools.ietf.org/html/rfc3986#section-2.2"></a>.
   */
  public final boolean isSubDelimiter;

  /**
   * The character that represents this delimiter.
   */
  public final char delimiterChar;

  /**
   * The character that represents this delimiter as string of length 1.
   */
  public final @NotNull String delimiterString;

  @Override
  public @NotNull String toString() {
    return delimiterString;
  }

  private static final @Nullable QueryDelimiter @NotNull [] byChar = new QueryDelimiter[]{
      //0   1    2    3    4    5    6    7
      null, EXCLAMATION_MARK, null, SHARP, DOLLAR, null, AMPERSAND, SINGLE_QUOTE,      // [32 - ]40
      ROUND_BRACKET_OPEN, ROUND_BRACKET_CLOSE, STAR, PLUS, COMMA, null, null, SLASH,   // [40 - ]48
      null, null, null, null, null, null, null, null,                                  // [48 - ]56
      null, null, COLON, SEMICOLON, null, EQUAL, null, QUESTION_MARK,                  // [56 - ]64
      AT, null, null, null, null, null, null, null,                                    // [64 - ]72
      null, null, null, null, null, null, null, null,                                  // [72 - ]80
      null, null, null, null, null, null, null, null,                                  // [80 - ]88
      null, null, null, SQUARE_BRACKET_OPEN, null, SQUARE_BRACKET_CLOSE, null, null    // [88 - ]96
  };

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
   * @param c           The character for which to return the delimiter.
   * @param alternative The delimiter to return, the given value is no valid delimiter.
   * @return The delimiter or the alternative.
   */
  public static @NotNull QueryDelimiter get(char c, @NotNull QueryDelimiter alternative) {
    final QueryDelimiter d = get(c);
    return d != null ? d : alternative;
  }
}