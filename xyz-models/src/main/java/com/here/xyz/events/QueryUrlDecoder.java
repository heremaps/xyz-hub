package com.here.xyz.events;

import static com.here.xyz.events.QueryDelimiter.SINGLE_QUOTE;
import static com.here.xyz.events.QueryParameterType.ANY;
import static com.here.xyz.util.Hex.decode;

import com.here.xyz.exceptions.ParameterFormatError;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class QueryUrlDecoder {

  private static void assertPercent(char c) throws IllegalArgumentException {
    if (c != '%') {
      throw new IllegalArgumentException();
    }
  }

  QueryUrlDecoder(@NotNull QueryParameters parameters, @NotNull CharSequence in, int start, int end) {
    if (start < 0 || start >= in.length() || start >= end) {
      this.start = this.end = in.length();
      this.sb = null;
    } else {
      if (end > in.length()) {
        end = in.length();
      }
      this.start = start;
      this.end = end;
      this.sb = new StringBuilder();
    }
    this.in = in;
    this.values = new ArrayList<>(16);
    this.parameters = parameters;
  }

  final @NotNull QueryParameters parameters;
  final @NotNull CharSequence in;
  final StringBuilder sb;
  final @NotNull List<@NotNull Object> values;
  int start;
  int end;

  boolean hasNext() {
    return start < end;
  }

  /**
   * Should only be {@code null}, if the end of the query string reached.
   */
  @Nullable QueryDelimiter delimiter;

  @NotNull String nextKey(int index) throws ParameterFormatError {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final Object string = next(null, null, index, -1);
    assert string instanceof String;
    return (String) string;
  }

  @NotNull String nextOp(@NotNull String key, int index) throws ParameterFormatError {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final Object string = next(key, null, index, -1);
    assert string instanceof String;
    return (String) string;
  }

  @Nullable Object nextValue(@NotNull String key, @NotNull QueryOperator op, int index, int number)
      throws NoSuchElementException, ParameterFormatError {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next(key, op, index, number);
  }

  private @Nullable Object next(
      final @Nullable String key,
      final @Nullable QueryOperator op,
      final int index,
      final int number
  ) throws NoSuchElementException, ParameterFormatError {
    assert sb != null;
    assert start < end;
    final CharSequence in = this.in;

    char quote = 0;
    int minus = 0;
    int e = 0;
    int dot = 0;
    int numbers = 0;
    int others = key == null ? 1 : 0; // Key are always strings, do not count numbers.
    char c = in.charAt(start);
    if (c == SINGLE_QUOTE.delimiterChar) {
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
            final String errorMsg = "Found illegal character '" + c + "' after closing quote";
            if (key == null) {
              throw new ParameterFormatError(errorMsg);
            }
            throw new ParameterFormatError(errorMsg + " for key " + key);
          }
        }
        break;
      }

      final QueryDelimiter d = QueryDelimiter.get(c);
      if (d != null) {
        // Consume the delimiter.
        start++;
        if (parameters.stopOnDelimiter(key, op, index, number, quote != 0, d, sb)) {
          delimiter = d;
          break;
        }
        continue;
      }

      if (others == 0) {
        if (c >= '0' && c <= '9') {
          numbers++;
        } else if (c == '-') {
          minus++;
        } else if (c == '.') {
          dot++;
        } else if (c == 'e' || c == 'E') {
          e++;
        } else {
          others++;
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
              if (key == null) {
                throw new ParameterFormatError("Invalid UTF-8 encoding found in query key: " + sb);
              } else {
                throw new ParameterFormatError("Invalid UTF-8 encoding found in query key: " + key + ", value[" + index + "]: " + sb);
              }
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

    if (key == null || quote > 0) {
      return sb.toString();
    }

    final QueryParameterType type = parameters.typeOfValue(key, index);
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
      if (sb.length() == 0) {
        // Note: An empty string is true, because adding a boolean parameter will by default make it true.
        // Example: "&gzip=true" should be the same as "&gzip".
        return Boolean.TRUE;
      }
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
        throw new ParameterFormatError("Expected boolean for key " + key + ", value[" + index + "]");
      }
    }

    // TODO: Switch to fast parser: https://github.com/wrandelshofer/FastDoubleParser
    final String string = sb.toString();
    if (type.maybeNumber && others == 0 && numbers > 0) {
      if (type.maybeLong && dot == 0 && e == 0 && minus <= 1) {
        // try long: [+-][0-9]
        try {
          return Long.parseLong(string);
        } catch (NumberFormatException nfe) {
          if (type != ANY) {
            throw new ParameterFormatError("Failed to parse long value[" + index + "] of key " + key, nfe);
          }
        }
      }

      if (type.maybeDouble && dot <= 1 && e <= 1 && minus <= 2) {
        // try double: [-][0-9]e[-][0-9]
        try {
          return Double.parseDouble(string);
        } catch (NumberFormatException nfe) {
          if (type != ANY) {
            throw new ParameterFormatError("Failed to parse double value[" + index + "] of key " + key, nfe);
          }
        }
      }
    }
    if (!type.maybeString) {
      throw new ParameterFormatError("Failed to parse value[" + index + "] of key " + key + ", found string, but expected " + type);
    }
    return string;
  }
}
