package com.here.naksha.lib.core.util;

import java.lang.ref.Reference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class StringHelper {

    /**
     * Method to convert any object into a string, automatically de-referencing references. If the
     * given character sequence is {@code null}, returning an empty string.
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
     * Method to convert any object into a character sequence, automatically de-referencing
     * references. If the given object is {@code null}, returning an empty string.
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

    /**
     * Compare the two character sequences using the UTF-16 characters. The returned value signals if
     * “a” is less than (&lt; 0), equal (0) to or more than (&gt; 0) “b”. This will compare the two
     * character sequences and stop at the first character where the sequences differ. When hitting a
     * difference, the difference between the character value read from “a” to the character read from
     * “b” is returned (a-b). When both character sequences are equal, but not of equal length, the
     * difference in length is returned (a.length() - b.length()). Character sequences being {@code
     * null} are treated like empty strings.
     *
     * @param a the first character sequence to compare.
     * @param b the second character sequence to compare.
     * @return less than 0 when “a” is less than “b”; 0 when both are equal; greater than 0 when “a”
     *     is greater than “b”.
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
     * Test if two character sequences are equal, ignoring the casing. This method uses UNICODE and is
     * therefore slower than a simple equals!
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
            // Note: We know that instances of NString can be compared by reference, and this has been
            // done already above!
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

    public static boolean equals(
            @NotNull CharSequence a, int aStart, int aEnd, @NotNull CharSequence b, int bStart, int bEnd) {
        if (aStart > aEnd || aStart < 0 || aStart > a.length() || aEnd > a.length()) return false;
        if (bStart > bEnd || bStart < 0 || bStart > b.length() || aEnd > b.length()) return false;
        if ((aEnd - aStart) != (bEnd - bStart)) return false;
        while (aStart < aEnd) {
            final char ac = a.charAt(aStart++);
            final char bc = b.charAt(bStart++);
            if (ac != bc) return false;
        }
        return true;
    }
}
