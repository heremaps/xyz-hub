package com.here.xyz.util;

import static com.here.xyz.NakshaLogger.currentLogger;
import static com.here.xyz.util.FibMap.CONFLICT;
import static com.here.xyz.util.FibMap.VOID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.lang.ref.WeakReference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This implements a high concurrent intern string cache. Basically this saves memory, when parsers
 * use this cache to de-duplicate strings while parsing JSON, XML and alike. Therefore, it should be
 * used for all character sequences that are used in code, basically everything should be registered
 * in this cache and then kept in memory.
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
        // Note: We use WeakString as key and when the string was garbage collected, we want to reuse
        // the slot for a new
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
                return StringHelper.equals(thisString, chars);
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

    /**
     * Tries to find a cached {@link String} for the given character sequence, but does not create a
     * new {@link StringCache} internally, if the character sequence is not already in the cache. This
     * method can be used to prevent a cache pollution.
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
     * Returns the {@link String} instance for the given character sequence. The method looks the
     * character sequence up in a cache table and if an existing instance found, it returns this one.
     * If not, it creates a new {@link String} instance from the given character sequence and returns
     * it. This method is thread safe, but not as fast as directly using a string, because it needs to
     * lookup in a cache table. The advantage of the method is that it guarantees that the same
     * character sequence always returns the same {@link String} instance, therefore comparing the
     * strings can be done by reference. Apart it guarantees that only one version of the string is
     * kept in memory.
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
     * @param c the character to hash.
     * @param hashCode the previously calculated hash-code.
     * @return the new hash-code.
     * @since 2.0.0
     */
    @AvailableSince("2.0.0")
    public static int javaHash(char c, int hashCode) {
        return hashCode * 31 + c;
    }

    // This means that at level 6 (max-depth) we have up to 256^6 (281,474,976,710,656, ~280 trillion)
    // distinct possible keys. If that does
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
                // Note: This is subject to a bug in hashCode() of the given character sequence, but we
                // can't solve all problems and at
                //       this point, performance is more important to us!
                return null;
            }

            if (weakString == null) {
                string = chars.toString();
                if (hashCode != string.hashCode()) {
                    // Fatal error, the character sequence did not return a valid hash!
                    currentLogger()
                            .warn(
                                    "The given character sequence calculates an invalid hash: {}, should be: {}",
                                    hashCode,
                                    string.hashCode(),
                                    new IllegalStateException());
                    // Fix the hash-code and retry step one, so this behaves the same as if we had a conflict.
                    hashCode = string.hashCode();
                    continue;
                }
                weakString = new StringCache(string);
                assert hashCode == weakString.hashCode();
                // We keep the reference to the string, to guarantee that our weak-reference stays alive.
            }
            // TODO: Replace by implementing a FibSet, so a Fibonacci map without values.
            Object result = FibMap._put(
                    weakString,
                    hashCode,
                    VOID,
                    weakString,
                    true,
                    root,
                    0,
                    StringCache::__intern,
                    StringCache::__conflict);
            if (result != CONFLICT) {
                //noinspection ConstantConditions,StringEquality
                assert string != null && string == weakString.get();
                return string;
            }
        }
    }

    private static @NotNull Object __conflict(
            @NotNull Object key, @Nullable Object expected_value, @Nullable Object new_value, @Nullable Object value) {
        return CONFLICT;
    }

    private static @NotNull Object __intern(@NotNull Object key) {
        return key;
    }
}
