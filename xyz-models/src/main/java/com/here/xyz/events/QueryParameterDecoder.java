package com.here.xyz.events;

import static com.here.xyz.events.QueryDelimiter.AMPERSAND;
import static com.here.xyz.events.QueryDelimiter.DOUBLE_QUOTE;
import static com.here.xyz.events.QueryDelimiter.END;
import static com.here.xyz.events.QueryDelimiter.EQUAL;
import static com.here.xyz.events.QueryDelimiter.SINGLE_QUOTE;
import static com.here.xyz.events.QueryOperation.NONE;
import static com.here.xyz.events.QueryParameterType.ANY;

import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.util.Hex;
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
      this.start = this.pos = this.end = in.length();
      this.sb = null;
    } else {
      if (end > in.length()) {
        end = in.length();
      }
      this.pos = this.start = start;
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

        QueryOperation op;
        while ((op = parseQueryOperation()) == null) {
          assert delimiter != null;
          final QueryDelimiter delimiter = this.delimiter;
          parseNext();
          addArgumentAndDelimiter(delimiter);
        }
        assert delimiter != null;
        parameter.op = op;
        while (hasMoreValues()) {
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
  protected int start;

  /**
   * The current position to read.
   */
  protected int pos;

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
   * Tests whether currently the key parsed.
   *
   * @return {@code true} if currently the key parsed; {@code false} otherwise.
   */
  protected boolean parsingKey() {
    return parameter == null;
  }

  /**
   * Tests whether currently the key or one of its argument parsed.
   *
   * @return {@code true} if currently the key or one of its arguments parsed; {@code false} otherwise.
   */
  protected boolean parsingKeyOrArgument() {
    return parameter == null || parameter.op == null;
  }

  /**
   * Tests whether currently a value parsed.
   *
   * @return {@code true} if currently a value parsed; {@code false} otherwise.
   */
  protected boolean parsingValue() {
    return parameter != null && parameter.op != null;
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

  /**
   * The “extended” unicode code point that was read as delimiter. Will be zero, if the end of the {@link #in input-stream} is reached.
   */
  protected int delimiter_ext_unicode;

  protected boolean hasMore() {
    assert in != null;
    assert sb != null;
    return pos < end;
  }

  protected @Nullable QueryOperation parseQueryOperation() throws ParameterError {
    assert parameter != null;
    assert delimiter != null;
    if (delimiter == END || delimiter == AMPERSAND) {
      // Like "&key".
      return NONE;
    }

    // Read ahead.
    final CharSequence in = this.in;
    int pos = this.pos;
    final int end = this.end;
    final StringBuilder sb = this.sb;
    sb.setLength(0);
    sb.append(delimiter.delimiterChar);
    final int max_length = QueryOperation.maxLength() + 2; // ={op}=
    int equal_index = -1; // The index of the next equal sign (not taking the first delimiter into account).
    int delimiters = 1; // Amount of continues head delimiters (at least one, the current one).
    while (pos < end && sb.length() < max_length && delimiters > equal_index) {
      // The last condition (delimiters > equal_index) is reached, when we encounter a named operation and is a performance optimization.
      // The reason is that for example for "=gt=", we find the equal sign at index 3, but have only one continues head delimiter (=).
      // Effectively we stop as soon as we have parsed the maximal length of operation names, or we found a valid operation name.
      // Apart from this, we collect all leading delimiters for the delimiter based operation detection, for example ">=".
      final char c = in.charAt(pos);
      if (c < 32 || c > 128 || c == '%') {
        break;
      }
      // Stop when we hit a quote, for example &foo!='bar', we need to limit the capturing to "!=".
      if (isQuote(c)) {
        break;
      }
      if (sb.length() == delimiters) {
        // Test for a continues delimiter.
        final QueryDelimiter delimiter = QueryDelimiter.get(c);
        if (delimiter != null) {
          delimiters++;
        }
      }
      if (c == '=' && equal_index < 0) {
        equal_index = sb.length();
      }
      sb.append(c);
      pos++;
    }

    if (delimiter == EQUAL && equal_index > 0) {
      // Named operation like "&key={op}={value}"
      // =ne=?
      // 01234
      // In this case "equal_index" should be three (so we need to extract "ne").
      final String opName = sb.substring(1, equal_index);
      final QueryOperation op = QueryOperation.getByName(opName);
      if (op == null) {
        throw new ParameterError(errorMsg("Unknown operation name: ", opName));
      }
      // Seek forward to the end of the operation name, so that pos refers to the first character after the second equal sign.
      this.pos += equal_index;
      return op;
    }

    // Fetch the operation by delimiter string, for example for "&key>=5" by ">=" or for "&key>5" by ">".
    final String delimitersString = sb.substring(0, delimiters);
    final QueryOperation op = QueryOperation.getByDelimiterString(delimitersString);
    if (op != null) {
      // Note: The first delimiter skipped already.
      this.pos += op.delimiters.length - 1;
      return op;
    }
    return null;
  }

  protected boolean hasMoreValues() {
    assert delimiter != null;
    return delimiter != END && delimiter != AMPERSAND;
  }

  /**
   * Create an “extended” unicode code point.
   *
   * @param unicode       The unicode code point to be encoded.
   * @param size          The amount of origin characters (from the input-stream) used to decode this code point.
   * @param wasUrlEncoded If the code point was URL encoded.
   * @return The “extended” unicode code point.
   */
  protected static int ext_make(int unicode, int size, boolean wasUrlEncoded) {
    return (unicode & 0x00ff_ffff) | ((size & 0x7f) << 24) | (wasUrlEncoded ? 0x8000_0000 : 0);
  }

  /**
   * Extracts the unicode code point from an “extended” unicode code point.
   *
   * @param ext_unicode The “extended” unicode code point.
   * @return The unicode code point.
   */
  protected static int ext_get_unicode(int ext_unicode) {
    return (int) (ext_unicode & 0x00ff_ffffL);
  }

  /**
   * Extracts if the code point was URL encoded in the origin from an “extended” unicode code point.
   *
   * @param ext_unicode The “extended” unicode code point.
   * @return {@code true} if the code point was originally URL encoded; {@code false} otherwise.
   */
  protected static boolean ext_was_url_encoded(int ext_unicode) {
    return (ext_unicode & 0x8000_0000) == 0x8000_0000;
  }

  /**
   * Extracts the amount of characters processed to decode the code point from an “extended” unicode code point. A value between 1 and 12 is
   * to be expected (12 for the worst case UTF-8 URL encoded variant).
   *
   * @param ext_unicode The “extended” unicode code point.
   * @return the amount of characters processed to decode the code point.
   */
  protected static int ext_get_size(int ext_unicode) {
    return (ext_unicode >>> 24) & 0x7f;
  }

  /**
   * Decode the character that {@link #pos} currently refers to and increment {@link #pos} so that it points to the next character after the
   * one just read. Performs URL and UTF-8 decoding and adds a flag to signal if a character was URL encoded.
   *
   * @return an “extended” unicode code point.
   * @throws ParameterError if decoding failed.
   */
  protected final int read() throws ParameterError {
    assert in != null;
    assert pos < end;
    final int mark = pos;
    final char c = in.charAt(pos);
    int unicode = c;
    boolean wasUrlEncoded = false;
    if (c == '%') {
      try {
        int i = pos + 1;
        // assertPercent(in.charAt(i++));
        unicode = Hex.decode(in.charAt(i++), in.charAt(i++));
        // 4 byte (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
        if ((unicode & 248) == 240) {
          unicode &= 7;
          unicode <<= 18;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63) << 12;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63) << 6;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63);
        } else if ((unicode & 240) == 224) {
          // 3 byte (1110xxxx 10xxxxxx 10xxxxxx)
          unicode &= 15;
          unicode <<= 12;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63) << 6;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63);
        } else if ((unicode & 224) == 192) {
          // 2 byte (110xxxxx 10xxxxxx)
          unicode &= 31;
          unicode <<= 6;
          assertPercent(in.charAt(i++));
          unicode += (Hex.decode(in.charAt(i++), in.charAt(i++)) & 63);
        } else
        // Single character
        {
          if ((unicode & 128) == 128) {
            throw new ParameterError(errorMsg("Invalid UTF-8 encoding found at position : ", (i - start)));
          }
        }
        wasUrlEncoded = true;
        pos = i;
      } catch (IndexOutOfBoundsException | IllegalArgumentException ignore) {
        unicode = c;
        pos++;
      }
    } else {
      pos++;
    }
    return ext_make(unicode, pos - mark, wasUrlEncoded);
  }

  protected void parseNext() throws NoSuchElementException, ParameterError {
    assert in != null;
    assert sb != null;
    assert pos < end;
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
    if (isQuote(in.charAt(pos))) {
      // When explicitly quoted, it must always be a string, for example "&key='5'", so do not detect numbers.
      others = true;
      quote = in.charAt(pos++);
    }
    delimiter = null;
    delimiter_ext_unicode = 0;
    sb.setLength(0);
    while (pos < end) {
      // UnicodeUrlReader
      // UnicodeBuilder
      int ext_unicode = read();
      int unicode = ext_get_unicode(ext_unicode);

      if (!ext_was_url_encoded(ext_unicode) && unicode == quote) {
        if (!hasMore()) {
          break;
        }

        // If another character exists, it must be a delimiter.
        final int mark = pos;
        ext_unicode = read();
        unicode = ext_get_unicode(ext_unicode);
        if (Character.isBmpCodePoint(unicode)) {
          delimiter = QueryDelimiter.get((char) unicode);
          if (delimiter != null && stopOnDelimiter(delimiter, ext_was_url_encoded(ext_unicode))) {
            delimiter_ext_unicode = ext_unicode;
            break;
          }
        }
        if (ext_was_url_encoded(ext_unicode)) {
          // We know that at mark there must be a percent character.
          final char c1 = in.charAt(mark + 1);
          final char c2 = in.charAt(mark + 2);
          throw new ParameterError(errorMsg("Found illegal character after closing quote: %", c1, c2));
        } else {
          throw new ParameterError(errorMsg("Found illegal character after closing quote: ", (char) unicode));
        }
      }

      if (quote == 0 && Character.isBmpCodePoint(unicode)) {
        final QueryDelimiter delimiter = QueryDelimiter.get((char) unicode);
        if (delimiter != null) {
          if (stopOnDelimiter(delimiter, ext_was_url_encoded(ext_unicode))) {
            // Should the stopOnDelimiter set the delimiter, we keep it; otherwise we store the accepted one.
            if (this.delimiter == null) {
              this.delimiter = delimiter;
            }
            if (delimiter_ext_unicode == 0) {
              delimiter_ext_unicode = ext_unicode;
            }
            break;
          }
          assert this.delimiter == null;
          continue;
        }
      }

      if (!others) {
        if (unicode >= '0' && unicode <= '9') {
          numbers++;
        } else if (unicode == '-') {
          minus++;
        } else if (unicode == '.') {
          dot++;
        } else if (unicode == 'e' || unicode == 'E') {
          e++;
        } else {
          others = true;
        }
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
      delimiter_ext_unicode = 0;
    }
  }

  private StringBuilder err_sb;

  protected @NotNull String errorMsg(@NotNull String reason, @Nullable Object... args) {
    final StringBuilder sb = err_sb != null ? err_sb : (err_sb = new StringBuilder(256));
    sb.setLength(0);
    if (parameter != null) {
      sb.append("Failed to parse parameter '");
      sb.append(parameter.name());
      sb.append("', ");
      if (parameter.op != null) {
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
    for (final Object arg : args) {
      if (arg instanceof Character) {
        sb.append((char) (Character) arg);
      } else if (arg instanceof CharSequence) {
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
    return c == SINGLE_QUOTE.delimiterChar || c == DOUBLE_QUOTE.delimiterChar;
  }

  /**
   * Invoked after a query parameter has been parsed and before parsing the next one. Can reject a parameter value, for example if only
   * specific values are allows or can adjust the parameter.
   *
   * @param parameter The parameter to finish.
   * @throws ParameterError if any error detected.
   */
  protected void finishParameter(@NotNull QueryParameter parameter) throws ParameterError {
  }

  /**
   * Can be overridden to expand keys and values or implement special handling for delimiters. The method is invoked whenever a delimiter
   * character is found in a query string. The {@link #sb string-builder} will hold the string parsed so far, without the delimiter, for
   * example for the query string "&p:foo=hello" the method is invoked when the {@link #sb string-builder} holds "p". The found delimiter is
   * given as argument.
   * <p>
   * If the method returns {@code true} it will abort the query parsing and the content of the {@link #sb string-builder}, after returning,
   * will be used to create the key/name, argument or value. If the method returns {@code false}, then the parser continues the parsing.
   * <p>
   * <b>NOTE</b>: The parser will <b>NOT</b> add the delimiter to the {@link #sb string-builder}, no matter what the method returns.
   * Therefore, this method must do this. The default behavior is to only add the delimiter to the {@link #sb string-builder}, if the
   * delimiter does not stop the parsing, so when returning {@code false}.
   *
   * @param delimiter     The delimiter found.
   * @param wasUrlEncoded If the delimiter was URL encoded.
   * @return {@code true} to abort the parsing; {@code false} to continue parsing.
   * @throws ParameterError If any error occurred.
   */
  protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter, boolean wasUrlEncoded) throws ParameterError {
    // We stop on all not URL encoded delimiters.
    if (!wasUrlEncoded) {
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
   * given as parameter.
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
