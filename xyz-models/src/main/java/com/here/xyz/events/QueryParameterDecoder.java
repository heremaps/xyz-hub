package com.here.xyz.events;

import static com.here.xyz.events.QueryDelimiter.AMPERSAND;
import static com.here.xyz.events.QueryDelimiter.COMMA;
import static com.here.xyz.events.QueryDelimiter.END;
import static com.here.xyz.events.QueryDelimiter.EQUAL;
import static com.here.xyz.events.QueryDelimiter.SEMICOLON;
import static com.here.xyz.events.QueryParameterType.ANY;
import static com.here.xyz.util.Hex.decode;

import com.here.xyz.exceptions.ParameterError;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The decoder used to generate a {@link QueryParameterList}. This class can be extended, so that other implementations are possible. The
 * default implementation should cover the need of around 99.9% all use-cases and with minor changes around 100% of standard conform
 * use-cases should be parsable.
 */
public class QueryParameterDecoder {

  protected static void assertPercent(char c) throws IllegalArgumentException {
    if (c != '%') {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Helper method to compare two char sequences.
   *
   * @param a The first character sequence.
   * @param b The second character sequence.
   * @return true if they are equal; false otherwise.
   */
  protected static boolean equals(@NotNull CharSequence a, @NotNull CharSequence b) {
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

  /**
   * Parses the given character sequence.
   *
   * <p>
   * The parsed input character sequence must only persist our of ASCII characters, therefore it is totally fine to back this by a simple
   * byte-array or byte-buffer. In fact, this expects a URL encoded UTF-8 byte stream. Generally it can be said, providing just a byte-array
   * wrapper for the bytes read from the socket is totally fine.
   *
   * @param in The character sequence to parse.
   * @return the finished parameter list.
   * @throws ParameterError If any error occurred while parsing.
   */
  public @NotNull QueryParameterList parse(@NotNull CharSequence in) throws ParameterError {
    return parse(new QueryParameterList(), in, 0, in.length());
  }

  /**
   * Parses the given character sequence.
   *
   * <p>
   * The parsed input character sequence must only persist our of ASCII characters, therefore it is totally fine to back this by a simple
   * byte-array or byte-buffer. In fact, this expects a URL encoded UTF-8 byte stream. Generally it can be said, providing just a byte-array
   * wrapper for the bytes read from the socket is totally fine.
   *
   * @param list The parameter list to fill.
   * @param in   The character sequence to parse.
   * @return the finished parameter list.
   * @throws ParameterError If any error occurred while parsing.
   */
  public <L extends QueryParameterList> @NotNull L parse(@NotNull L list, @NotNull CharSequence in) throws ParameterError {
    return parse(list, in, 0, in.length());
  }

  /**
   * Parses the given character sequence.
   *
   * <p>
   * The parsed input character sequence must only persist our of ASCII characters, therefore it is totally fine to back this by a simple
   * byte-array or byte-buffer. In fact, this expects a URL encoded UTF-8 byte stream. Generally it can be said, providing just a byte-array
   * wrapper for the bytes read from the socket is totally fine.
   *
   * @param in    The character sequence to parse.
   * @param start The first character to read from the sequence.
   * @param end   The end of the characters to read, in other words: First character <b>NOT</b> to read from the sequence.
   * @return the finished parameter list.
   * @throws ParameterError If any error occurred while parsing.
   */
  public @NotNull QueryParameterList parse(@NotNull CharSequence in, int start, int end) throws ParameterError {
    return parse(new QueryParameterList(), in, start, end);
  }

  /**
   * Parses the given character sequence.
   *
   * <p>
   * The parsed input character sequence must only persist our of ASCII characters, therefore it is totally fine to back this by a simple
   * byte-array or byte-buffer. In fact, this expects a URL encoded UTF-8 byte stream. Generally it can be said, providing just a byte-array
   * wrapper for the bytes read from the socket is totally fine.
   *
   * @param list  The parameter list to fill.
   * @param in    The character sequence to parse.
   * @param start The first character to read from the sequence.
   * @param end   The end of the characters to read, in other words: First character <b>NOT</b> to read from the sequence.
   * @return the finished parameter list.
   * @throws ParameterError If any error occurred while parsing.
   */
  @SuppressWarnings("unchecked")
  public <L extends QueryParameterList> @NotNull L parse(@NotNull L list, @NotNull CharSequence in, int start, int end)
      throws ParameterError {
    if (start < 0 || start >= in.length() || start >= end) {
      this.originalStart = this.start = this.end = in.length();
      this.sb = null;
    } else {
      if (end > in.length()) {
        end = in.length();
      }
      this.start = this.originalStart = start;
      this.end = end;
      this.sb = new StringBuilder(Math.min(4000, end - start));
    }
    this.in = in;
    this.parameterList = list;
    try {
      while (hasMore()) {
        assert parameter == null;
        parseNext();

        final QueryParameter p = newParameter();
        parameterList.list.add(p);
        QueryParameter existing = parameterList.map.putIfAbsent(p.key(), p);
        if (existing != null) {
          while (existing.next != null) {
            existing = existing.next;
          }
          existing.next = p;
        }
        parameter = p;

        while (hasNextArgument()) {
          assert delimiter != null;
          final QueryDelimiter delimiter = this.delimiter;
          parseNext();
          addArgumentAndDelimiter(delimiter);
        }
        // Normally this should be "=", but it can be modified, therefore we do not assert this!
        assert delimiter != null;
        parameter.delimiter = delimiter;
        while (hasNextValue()) {
          parseNext();
          addValueAndDelimiter(delimiter);
        }
        finishParameter(parameter);
        parameter = null;
      }
      return (L) parameterList;
    } finally {
      this.in = null;
      this.sb = null;
      this.parameterList = null;
      this.parameter = null;
    }
  }

  /**
   * The parameter list currently being parsed.
   */
  protected QueryParameterList parameterList;
  /**
   * The character sequence to read from.
   */
  protected CharSequence in;
  /**
   * The start index.
   */
  protected int originalStart;
  /**
   * The first character to read.
   */
  protected int start;
  /**
   * The first character NOT to read.
   */
  protected int end;

  /**
   * The string builder with the characters parsed.
   */
  protected StringBuilder sb;

  /**
   * The parameter that is currently parsed.
   */
  protected QueryParameter parameter;

  /**
   * Tests whether currently a key is parsed.
   *
   * @return {@code true} if currently a key is parsed; {@code false} otherwise.
   */
  protected boolean isKey() {
    return parameter == null;
  }

  /**
   * Tests whether currently a key argument is parsed.
   *
   * @return {@code true} if currently a key argument is parsed; {@code false} otherwise.
   */
  protected boolean isArgument() {
    return parameter != null && parameter.delimiter == null;
  }

  /**
   * Tests whether currently a key or one of its argument is parsed.
   *
   * @return {@code true} if currently a key or one of its arguments is parsed; {@code false} otherwise.
   */
  protected boolean isKeyOrArgument() {
    return parameter == null || parameter.delimiter == null;
  }

  /**
   * Tests whether currently a value is parsed.
   *
   * @return {@code true} if currently a value is parsed; {@code false} otherwise.
   */
  protected boolean isValue() {
    return parameter != null && parameter.delimiter != null;
  }

  /**
   * The character with which the value was quoted or {@code 0}, if the value was not quoted.
   */
  protected char quote;

  /**
   * How many characters between '0' and '9' are in the string builder.
   */
  protected int numbers;

  /**
   * How many dots characters are in the string builder.
   */
  protected int dot;

  /**
   * How many minus characters are in the string builder.
   */
  protected int minus;

  /**
   * How many “e” or “E” characters are in the string builder.
   */
  protected int e;

  /**
   * If other characters are in the string builder, so not number, dot, minus or “e”. Note that when the first other character is found,
   * counting numbers, dots, minus and “e” stops.
   */
  protected boolean others;

  /**
   * The delimiter that ended the parsing. Should only be {@code null}, if the end of the query string reached.
   */
  protected @Nullable QueryDelimiter delimiter;

  protected boolean hasMore() {
    assert in != null;
    assert sb != null;
    return start < end;
  }

  protected boolean hasNextArgument() {
    assert delimiter != null;
    return delimiter != END && delimiter != EQUAL && delimiter != AMPERSAND;
  }

  protected boolean hasNextValue() {
    assert delimiter != null;
    return delimiter != END && delimiter != AMPERSAND;
  }

  protected void parseNext() throws NoSuchElementException, ParameterError {
    assert in != null;
    assert sb != null;
    assert start < end;
    // Copy references to the stack to allow compiler optimization!
    final CharSequence in = this.in;
    final StringBuilder sb = this.sb;
    QueryParameter parameter = this.parameter;

    quote = 0;
    minus = 0;
    e = 0;
    dot = 0;
    numbers = 0;
    others = parameter == null; // Key are always strings, do not count characters.
    char c = in.charAt(start);
    if (isQuote(c)) {
      quote = c;
      start++;
    }
    delimiter = null;
    sb.setLength(0);
    while (start < end) {
      c = in.charAt(start);

      if (c == quote) {
        // Consume the quote.
        if (++start < end) {
          // Consume the next character, if there is any, which must be a delimiter.
          c = in.charAt(start++);
          delimiter = QueryDelimiter.get(c);
          if (delimiter == null) {
            throw new ParameterError(errorMsg("Found illegal character after closing quote: ", c));
          }
        }
        break;
      }

      final QueryDelimiter delimiter = QueryDelimiter.get(c);
      if (delimiter != null) {
        // Consume the delimiter.
        start++;
        if (stopOnDelimiter(delimiter)) {
          this.delimiter = delimiter;
          break;
        }
        continue;
      }

      if (!others) {
        if (c >= '0' && c <= '9') {
          numbers++;
        } else if (c == '-') {
          minus++;
        } else if (c == '.') {
          dot++;
        } else if (c == 'e' || c == 'E') {
          e++;
        } else {
          others = true;
        }
      }

      int unicode = c;
      if (c == '%') {
        try {
          int i = start + 1;
          // assertPercent(in.charAt(i++));
          unicode = decode(in.charAt(i++), in.charAt(i++));
          // 4 byte (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
          if ((unicode & 248) == 240) {
            unicode &= 7;
            unicode <<= 18;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63) << 12;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63) << 6;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63);
          } else if ((unicode & 240) == 224) {
            // 3 byte (1110xxxx 10xxxxxx 10xxxxxx)
            unicode &= 15;
            unicode <<= 12;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63) << 6;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63);
          } else if ((unicode & 224) == 192) {
            // 2 byte (110xxxxx 10xxxxxx)
            unicode &= 31;
            unicode <<= 6;
            assertPercent(in.charAt(i++));
            unicode += (decode(in.charAt(i++), in.charAt(i++)) & 63);
          } else
          // Single character
          {
            if ((unicode & 128) == 128) {
              throw new ParameterError(errorMsg("Invalid UTF-8 encoding found at position : ", (i - originalStart)));
            }
          }
          start = i;
        } catch (IndexOutOfBoundsException | IllegalArgumentException ignore) {
          unicode = c;
          start++;
        }
      } else {
        start++;
      }

      if (Character.isBmpCodePoint(unicode)) {
        sb.append((char) unicode);
      } else {
        sb.append(Character.highSurrogate(unicode));
        sb.append(Character.lowSurrogate(unicode));
      }
    }
    if (delimiter == null) {
      delimiter = END;
    }
  }

  private StringBuilder err_sb;

  protected @NotNull String errorMsg(@NotNull String reason) {
    return errorMsg(reason, null);
  }

  protected @NotNull String errorMsg(@NotNull String reason, @Nullable Object arg) {
    final StringBuilder sb = err_sb != null ? err_sb : (err_sb = new StringBuilder(256));
    sb.setLength(0);
    if (parameter != null) {
      sb.append("Failed to parse parameter '");
      sb.append(parameter.name());
      sb.append("', ");
      if (parameter.delimiter != null) {
        sb.append("value[");
        if (parameter.hasValues()) {
          sb.append(parameter.values().size());
        } else {
          sb.append('0');
        }
      } else {
        sb.append("matrix-value[");
        if (parameter.hasArguments()) {
          sb.append(parameter.arguments().size());
        } else {
          sb.append('0');
        }
      }
      sb.append("]: ");
    }
    sb.append(reason);
    if (arg != null) {
      if (arg instanceof CharSequence) {
        sb.append((CharSequence) arg);
      } else {
        sb.append(arg);
      }
    }
    return sb.toString();
  }

  /**
   * Standard {@link #sb string-builder} to value converter. This implementation converts the given value in the string builder into a value
   * object.
   *
   * @param type The expected type.
   * @return the value.
   * @throws ParameterError If any error occurred.
   */
  protected @Nullable Object sbToValue(final @NotNull QueryParameterType type) throws ParameterError {
    assert sb != null;

    final StringBuilder sb = this.sb;
    if (quote > 0) {
      return sb.toString();
    }

    char c;
    if (type.nullable) {
      if (sb.length() == 0) {
        return null;
      }
      if (sb.length() == 4) {
        c = sb.charAt(0);
        if (c == 'n' || c == 'N') {
          c = sb.charAt(1);
          if (c == 'u' || c == 'U') {
            c = sb.charAt(2);
            if (c == 'l' || c == 'L') {
              c = sb.charAt(3);
              if (c == 'l' || c == 'L') {
                return null;
              }
            }
          }
        }
      }
    }

    if (type.maybeBoolean) {
      // true
      if (sb.length() == 4) {
        c = sb.charAt(0);
        if (c == 't' || c == 'T') {
          c = sb.charAt(1);
          if (c == 'r' || c == 'R') {
            c = sb.charAt(2);
            if (c == 'u' || c == 'U') {
              c = sb.charAt(3);
              if (c == 'e' || c == 'E') {
                return Boolean.TRUE;
              }
            }
          }
        }
      }
      // false
      if (sb.length() == 5) {
        c = sb.charAt(0);
        if (c == 'f' || c == 'F') {
          c = sb.charAt(1);
          if (c == 'a' || c == 'A') {
            c = sb.charAt(1);
            if (c == 'l' || c == 'L') {
              c = sb.charAt(1);
              if (c == 's' || c == 'S') {
                c = sb.charAt(1);
                if (c == 'e' || c == 'E') {
                  return Boolean.FALSE;
                }
              }
            }
          }
        }
      }
      if (type != ANY) {
        throw new ParameterError(errorMsg("Expected boolean"));
      }
    }

    // TODO: Switch to fast parser: https://github.com/wrandelshofer/FastDoubleParser
    final String string = sb.toString();
    if (type.maybeNumber && !others && numbers > 0) {
      if (type.maybeLong && dot == 0 && e == 0 && minus <= 1) {
        // try long: [+-][0-9]
        try {
          return Long.parseLong(string);
        } catch (NumberFormatException nfe) {
          if (type != ANY) {
            throw new ParameterError(errorMsg("Expected long"), nfe);
          }
        }
      }

      if (type.maybeDouble && dot <= 1 && e <= 1 && minus <= 2) {
        // try double: [-][0-9]e[-][0-9]
        try {
          return Double.parseDouble(string);
        } catch (NumberFormatException nfe) {
          if (type != ANY) {
            throw new ParameterError(errorMsg("Expected double"), nfe);
          }
        }
      }
    }
    if (!type.maybeString) {
      throw new ParameterError(errorMsg("Found string, but expected ", type));
    }
    return string;
  }

  /**
   * Called by the parser to test if a character is a quote. Only called for the first character and used automatically as end character.
   * Escaping is done by double quotation.
   *
   * @param c The character to test.
   * @return {@code true} if the character is a quote; {@code false} if not.
   */
  protected boolean isQuote(char c) {
    return c == QueryDelimiter.SINGLE_QUOTE.delimiterChar;
  }

  /**
   * Invoked after a query parameter has been parsed and before parsing the next one. Can reject a parameter value, for example if only
   * specific values are allows or can set a default value.
   *
   * @param parameter The parameter to validate.
   * @throws ParameterError if any error detected.
   */
  protected void finishParameter(@NotNull QueryParameter parameter) throws ParameterError {
  }

  /**
   * Can be overridden to expand keys and values or implement special handling for delimiters. The method is invoked whenever a not URL
   * encoded delimiter is found in a query string. The {@link #sb string-builder} will hold the string parsed so far, without the delimiter,
   * for example for the query string "&p:foo=hello" the method is invoked when the {@link #sb string-builder} holds "p". The found
   * delimiter is given as argument.
   * <p>
   * If the method returns {@code true} it will abort the query parsing and the content of the {@link #sb string-builder}, after returning,
   * will be used to create the key/name, argument or value. If the method returns {@code false}, then the parser continues the parsing.
   * <p>
   * <b>NOTE</b>: The parser will <b>NOT</b> add the delimiter to the {@link #sb string-builder}, no matter what the method returns.
   * Therefore, this method must do this, if wanted; the method may modify the content of the {@link #sb string-builder} in any way wanted.
   *
   * @param delimiter The delimiter found.
   * @return {@code true} to abort the parsing; {@code false} to continue parsing.
   */
  protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter) {
    if (delimiter == AMPERSAND || delimiter == EQUAL || delimiter == SEMICOLON || delimiter == COMMA) {
      return true;
    }
    // When we do not abort the parsing, add the delimiter as normal character.
    sb.append(delimiter.delimiterChar);
    return false;
  }

  /**
   * Invoked when the parameter name has been parsed and is stored in the {@link #sb string-builder}. This method can assert that the
   * {@link #parameterList parameter-list} is not {@code null}.
   *
   * @return The added query parameter.
   * @throws ParameterError If any error occurred.
   */
  protected @NotNull QueryParameter newParameter() throws ParameterError {
    final String name = (String) sbToValue(QueryParameterType.STRING);
    assert name != null;
    return new QueryParameter(parameterList, name, parameterList.size());
  }

  /**
   * Invoked by the parser when parsing of the next parameter argument done. The method must add the content of the
   * {@link #sb string-builder} into the argument list of the current {@link #parameter parameter}. This method can assert that the
   * {@link #parameter parameter} is not {@code null}.
   *
   * <p><b>Note</b>: The current {@link #delimiter} is the next delimiter, the previous one (that belongs to the argument), is the one
   * given
   * as parameter.
   *
   * @param prefix The delimiter that was parsed before the argument.
   * @throws ParameterError If any error occurred.
   */
  protected void addArgumentAndDelimiter(@NotNull QueryDelimiter prefix) throws ParameterError {
    assert parameter != null;
    final Object value = sbToValue(ANY);
    parameter.arguments().add(value);
    parameter.argumentsDelimiter().add(prefix);
  }

  /**
   * Invoked by the parser when parsing of the next parameter value done. The method must add the content of the {@link #sb string-builder}
   * into the value list of the current {@link #parameter parameter}. This method can assert that the {@link #parameter parameter} is not
   * {@code null}.
   *
   * @param postfix The delimiter after the value.
   * @throws ParameterError If any error occurred.
   */
  protected void addValueAndDelimiter(@NotNull QueryDelimiter postfix) throws ParameterError {
    assert parameter != null;
    final Object value = sbToValue(ANY);
    parameter.values().add(value);
    parameter.valuesDelimiter().add(postfix);
  }

}
