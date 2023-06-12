package com.here.xyz.util;

import static com.here.xyz.NakshaLogger.currentLogger;
import static com.here.xyz.util.FibMap.CONFLICT;
import static com.here.xyz.util.FibMap.UNDEFINED;
import static com.here.xyz.util.FibMap.VOID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.ILike;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This implements a high concurrent intern string cache. Basically this saves memory, when parsers use this cache to de-duplicate strings
 * while parsing JSON, XML and alike. Therefore, it should be used for all character sequences that are used in code, basically everything
 * should be registered in this cache and then kept in memory.
 *
 * @since 2.0.0
 */
@SuppressWarnings("unused")
@AvailableSince("2.0.0")
public final class StringCache extends WeakReference<String> implements CharSequence, ILike {

  private StringCache(@NotNull String string) {
    super(string);
    this.hashCode = string.hashCode();
  }

  @JsonIgnore
  private final int hashCode;

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return this == o;
  }

  @AvailableSince("2.0.0")
  @Override
  public boolean isLike(@Nullable Object o) {
    if (o instanceof StringCache weakString) {
      return this == o;
    }
    // Note: We use WeakString as key and when the string was garbage collected, we want to reuse the slot for a new
    //       WeakString instance with the same hash-code, otherwise we would get a memory leak!
    if (o instanceof String string) {
      final String thisString = this.get();
      if (thisString != null) {
        return thisString.equals(string);
      }
      return hashCode == string.hashCode();
    }
    if (o instanceof CharSequence chars) {
      final String thisString = this.get();
      if (thisString != null) {
        return StringCache.equals(thisString, chars);
      }
      return hashCode == chars.hashCode();
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    final String s = get();
    return s != null ? s : "";
  }

  /**
   * Method to convert any object into a string, automatically de-referencing references. If the given character sequence is {@code null},
   * returning an empty string.
   *
   * @param o the object to convert to a string.
   * @return the string.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static @NotNull String toString(@Nullable Object o) {
    while (o instanceof Reference<?> ref) {
      o = ref.get();
    }
    if (o instanceof String string) {
      return string;
    }
    if (o == null || (o instanceof CharSequence chars && chars.length() == 0)) {
      return "";
    }
    return o.toString();
  }

  /**
   * Method to convert any object into a character sequence, automatically de-referencing references. If the given object is {@code null},
   * returning an empty string.
   *
   * @param o the object to convert to a character sequence.
   * @return the string.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static @NotNull CharSequence toCharSequence(@Nullable Object o) {
    while (o instanceof Reference<?> ref) {
      if (o instanceof CharSequence chars) {
        return chars;
      }
      o = ref.get();
    }
    if (o == null) {
      return "";
    }
    if (o instanceof CharSequence cs) {
      return cs;
    }
    return o.toString();
  }

  @Override
  public int length() {
    return toString().length();
  }

  @Override
  public char charAt(int index) {
    return toString().charAt(index);
  }

  @Override
  public @NotNull CharSequence subSequence(int start, int end) {
    return toString().subSequence(start, end);
  }

  // --------------------------------------------------------------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------------

  /**
   * Compare the two character sequences using the UTF-16 characters. The returned value signals if “a” is less than (&lt; 0), equal (0) to
   * or more than (&gt; 0) “b”. This will compare the two character sequences and stop at the first character where the sequences differ.
   * When hitting a difference, the difference between the character value read from “a” to the character read from “b” is returned (a-b).
   * When both character sequences are equal, but not of equal length, the difference in length is returned (a.length() - b.length()).
   * Character sequences being {@code null} are treated like empty strings.
   *
   * @param a the first character sequence to compare.
   * @param b the second character sequence to compare.
   * @return less than 0 when “a” is less than “b”; 0 when both are equal; greater than 0 when “a” is greater than “b”.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static int compare(final @Nullable CharSequence a, final @Nullable CharSequence b) {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -b.length();
    }
    if (b == null) {
      return a.length();
    }
    final int aLen = a.length();
    final int bLen = b.length();
    final int lim = Math.min(aLen, bLen);
    for (int k = 0; k < lim; k++) {
      char c1 = a.charAt(k);
      char c2 = b.charAt(k);
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return aLen - bLen;
  }

  /**
   * Test if two character sequences are equal, ignoring the casing. This method uses UNICODE and is therefore slower than a simple equals!
   *
   * @param a the first character sequence.
   * @param b the second character sequence.
   * @return true if both character sequences are equal; false otherwise.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static boolean equalsIgnoreCase(@Nullable CharSequence a, @Nullable CharSequence b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    // Note: As we convert UTF-16 to Unicode, we can't be 100% sure that an upper case character
    // uses the same amount of
    //       UTF-16 characters than the lower cased, therefore we need to compare all characters.
    int ai = 0;
    final int a_len = a.length();
    int bi = 0;
    final int b_len = b.length();
    while (ai < a_len && bi < b_len) {
      int a_codePoint = a.charAt(ai++);
      int b_codePoint = b.charAt(bi++);
      if (Character.isHighSurrogate((char) a_codePoint) && (ai < a_len)) {
        final char low = a.charAt(ai);
        if (Character.isLowSurrogate(low)) {
          ai++;
          a_codePoint = Character.toCodePoint((char) a_codePoint, low);
        }
      }
      if (Character.isHighSurrogate((char) b_codePoint) && (bi < b_len)) {
        final char low = b.charAt(bi);
        if (Character.isLowSurrogate(low)) {
          bi++;
          b_codePoint = Character.toCodePoint((char) b_codePoint, low);
        }
      }
      // Note: Some exotic characters will be equal upper-cased, but not lower cased (and vice
      // versa)!
      if (Character.toUpperCase(a_codePoint) != Character.toUpperCase(b_codePoint)
          || Character.toLowerCase(a_codePoint) != Character.toLowerCase(b_codePoint)) {
        return false;
      }
    }
    return ai == a_len && bi == b_len;
  }

  /**
   * Test if two character sequences are equal, adding special handling for JsonString instances.
   *
   * @param a the first character sequence.
   * @param b the second character sequence.
   * @return true if both character sequences are equal; false otherwise.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static boolean equals(@Nullable CharSequence a, @Nullable CharSequence b) {
    // If a and b are NString instances, and they are equal, then this will catch this case!
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    // Unwrap NString instance “a”.
    if (a instanceof StringCache an) {
      // Note: We know that instances of NString can be compared by reference, and this has been done already above!
      if (b instanceof StringCache) {
        return false;
      }
      a = an.toString();
      if (a == b) {
        return true;
      }
    }
    // Unwrap NString instance “b”.
    if (b instanceof StringCache bn) {
      b = bn.toString();
      if (a == b) {
        return true;
      }
    }
    final int len;
    if ((len = a.length()) != b.length() || a.hashCode() != b.hashCode()) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (a.charAt(i) != b.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  // ----------------------------------------------------------------------------------------------

  /**
   * Tries to find a cached {@link String} for the given character sequence, but does not create a new {@link StringCache} internally, if
   * the character sequence is not already in the cache. This method can be used to prevent a cache pollution.
   *
   * @param chars the character sequence to search for.
   * @return the {@link String} if any is cached, or {@code null}, if not found.
   * @throws NullPointerException if the character sequence is null.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static @Nullable String get(final @NotNull CharSequence chars) {
    return _cache(chars, false);
  }

  /**
   * Returns the {@link String} instance for the given character sequence. The method looks the character sequence up in a cache table and
   * if an existing instance found, it returns this one. If not, it creates a new {@link String} instance from the given character sequence
   * and returns it. This method is thread safe, but not as fast as directly using a string, because it needs to lookup in a cache table.
   * The advantage of the method is that it guarantees that the same character sequence always returns the same {@link StringCache}
   * instance, therefore comparing the strings can be done by reference. Apart it guarantees that only one version of the string is
   * eventually kept in memory.
   *
   * @param chars the characters to intern.
   * @return the interned {@link StringCache}.
   * @throws NullPointerException if the given character sequence is null.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static @NotNull String intern(final @NotNull CharSequence chars) {
    final String string = _cache(chars, true);
    assert string != null;
    return string;
  }

  /**
   * Calculates the default Java string hash. The hash code of an empty string is zero.
   *
   * @param c the character to hash.
   * @return the initial hash-code.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static int javaHash(char c) {
    return c;
  }

  /**
   * Calculates the default Java string hash.
   *
   * @param c        the character to hash.
   * @param hashCode the previously calculated hash-code.
   * @return the new hash-code.
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static int javaHash(char c, int hashCode) {
    return hashCode * 31 + c;
  }

  // This means that at level 6 (max-depth) we have up to 256^6 (281,474,976,710,656, ~280 trillion) distinct possible keys. If that does
  // not lead to a unique key hit, we are biting the carpet and use linear probing with array copy.
  private static final int FACTOR_bits = 8;
  private static final int MAX_DEPTH = 6;
  private static final int FACTOR = 1 << FACTOR_bits;
  private static final Object[] root = new Object[FACTOR];

  /**
   * The empty string ({@code ""}).
   *
   * @since 2.0.0
   */
  @AvailableSince("2.0.0")
  public static final @NotNull String EMPTY = "";

  private static String _cache(final @NotNull CharSequence chars, boolean create) {
    assert !(chars instanceof StringCache);

    //noinspection ConstantConditions
    if (chars == null || chars.length() == 0) {
      return EMPTY;
    }

    final Object[] root = StringCache.root;
    int hashCode = chars.hashCode();
    StringCache weakString = null;
    String string = null;
    while (true) {
      final Object existing_value = FibMap._get(chars, hashCode, root, 0);
      if (existing_value instanceof final StringCache ws) {
        final String s = ws.get();
        if (s != null) {
          return s;
        }
      }

      if (!create) {
        // Note: This is subject to a bug in hashCode() of the given character sequence, but we can't solve all problems and at
        //       this point, performance is more important to us!
        return null;
      }

      if (weakString == null) {
        string = chars.toString();
        if (hashCode != string.hashCode()) {
          // Fatal error, the character sequence did not return a valid hash!
          currentLogger().warn("The given character sequence calculates an invalid hash: {}, should be: {}",
              hashCode, string.hashCode(), new IllegalStateException());
          // Fix the hash-code and retry step one, so this behaves the same as if we had a conflict.
          hashCode = string.hashCode();
          continue;
        }
        weakString = new StringCache(string);
        assert hashCode == weakString.hashCode();
        // We keep the reference to the string, to guarantee that our weak-reference stays alive.
      }
      // TODO: Replace by implementing a FibSet, so a Fibonacci map without values.
      Object result = FibMap._put(weakString, hashCode, VOID, weakString, true, root, 0, StringCache::__intern, StringCache::__conflict);
      if (result != CONFLICT) {
        //noinspection ConstantConditions,StringEquality
        assert string != null && string == weakString.get();
        return string;
      }
    }
  }

  private static @NotNull Object __conflict(
      @NotNull Object key,
      @Nullable Object expected_value,
      @Nullable Object new_value,
      @Nullable Object value) {
    return CONFLICT;
  }

  private static @NotNull Object __intern(@NotNull Object key) {
    return key;
  }
}