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
package com.here.naksha.lib.core.util;

import static com.here.naksha.lib.core.util.fib.FibSet.SOFT;
import static com.here.naksha.lib.core.util.fib.FibSet.STRONG;
import static com.here.naksha.lib.core.util.fib.FibSet.WEAK;
import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;
import static com.here.naksha.lib.core.util.fib.FibSetOp.REMOVE;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.fib.FibEntry;
import com.here.naksha.lib.core.util.fib.FibRefType;
import com.here.naksha.lib.core.util.fib.FibSet;
import com.here.naksha.lib.core.util.fib.FibSetOp;
import com.here.naksha.lib.core.util.json.JsonEnum;
import java.text.Normalizer;
import java.text.Normalizer.Form;
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
@AvailableSince(NakshaVersion.v2_0_0)
public final class StringCache {

  private StringCache() {}

  /**
   * A helper method to convert a character sequence, which may be {@code null}, into a string or {@code null again}.
   *
   * @param chars The character sequence to convert.
   * @return the string representing the character sequence or {@code null}, if the character sequence was {@code null}.
   */
  @SuppressWarnings("DataFlowIssue")
  public static @Nullable String string(@Nullable CharSequence chars) {
    return string(chars, null);
  }

  /**
   * A helper method to convert a character sequence, which may be {@code null}, into a string.
   *
   * @param chars       The character sequence to convert.
   * @param alternative The string to return, when the character sequence is {@code null}.
   * @return the string representing the character sequence or the given alternative.
   */
  public static @NotNull String string(@Nullable CharSequence chars, @NotNull String alternative) {
    if (chars == null) {
      return alternative;
    }
    if (chars instanceof String) {
      return (String) chars;
    }
    if (chars instanceof JsonEnum) {
      // We know, the values of JSON enumerations are already interned!
      return ((JsonEnum) chars).toString();
    }
    // Technically, we assume there are now only very few cases left, where a CharSequence is already interned.
    return intern(chars);
  }

  /**
   * Tries to find a cached {@link String} for the given character sequence, but does not create a new {@link StringCache} internally, if
   * the character sequence is not already in the cache. This method can be used to prevent a cache pollution.
   *
   * @param chars the character sequence to search for.
   * @return the {@link String} if any is cached, or {@code null}, if not found.
   * @throws NullPointerException if the character sequence is null.
   * @since 2.0.0
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public static @Nullable String get(final @NotNull CharSequence chars) {
    return cache(GET, chars, STRONG);
  }

  /**
   * Removes the given string from the string cache.
   *
   * @param string the string to remove.
   * @return {@code true} if the string removed; false if it was not cached.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static boolean remove(final @NotNull String string) {
    return cache(REMOVE, string, STRONG) != null;
  }

  /**
   * Returns the {@link String} instance for the given character sequence.
   *
   * <p>The method looks the character sequence up in a cache table and if an existing instance found, it returns this one. If not, it
   * creates a new {@link String} instance from the given character sequence and returns it.
   * <p>This method is thread safe, but not as fast as directly using a string, because it needs to lookup in a cache table. The advantage
   * of the method is that it guarantees that the same character sequence always returns the same {@link String} instance, therefore
   * comparing the strings can be done by reference. Apart it guarantees that only one version of the string kept in memory.
   *
   * <p>This adds a strong reference to the string into the cache.
   *
   * @param chars the characters to intern.
   * @return the interned string.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public static @NotNull String intern(final @NotNull CharSequence chars) {
    final String string = cache(PUT, chars, STRONG);
    assert string != null;
    return string;
  }

  /**
   * Returns the {@link String} instance for the given character sequence.
   *
   * <p>The method looks the character sequence up in a cache table and if an existing instance found, it returns this one. If not, it
   * creates a new {@link String} instance from the given character sequence and returns it.
   * <p>This method is thread safe, but not as fast as directly using a string, because it needs to lookup in a cache table. The advantage
   * of the method is that it guarantees that the same character sequence always returns the same {@link String} instance, therefore
   * comparing the strings can be done by reference. Apart it guarantees that only one version of the string kept in memory.
   *
   * <p>This adds a weak-reference to the string into the cache, so that is automatically removed from the cache, when no longer reachable.
   *
   * @param chars the characters to intern.
   * @return the interned string.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static @NotNull String internWeak(final @NotNull CharSequence chars) {
    final String string = cache(PUT, chars, WEAK);
    assert string != null;
    return string;
  }

  /**
   * Returns the {@link String} instance for the given character sequence.
   *
   * <p>The method looks the character sequence up in a cache table and if an existing instance found, it returns this one. If not, it
   * creates a new {@link String} instance from the given character sequence and returns it.
   * <p>This method is thread safe, but not as fast as directly using a string, because it needs to lookup in a cache table. The advantage
   * of the method is that it guarantees that the same character sequence always returns the same {@link String} instance, therefore
   * comparing the strings can be done by reference. Apart it guarantees that only one version of the string kept in memory.
   *
   * <p>This adds a soft-reference to the string into the cache, so that is automatically removed from the cache, when not used for some
   * time (at least ZGC will tack access to the string and when no access has been done for some time, it removes the reference).
   *
   * @param chars the characters to intern.
   * @return the interned string.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static @NotNull String internSoft(final @NotNull CharSequence chars) {
    final String string = cache(PUT, chars, SOFT);
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
  @AvailableSince(NakshaVersion.v2_0_0)
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
  @AvailableSince(NakshaVersion.v2_0_0)
  public static int javaHash(char c, int hashCode) {
    return hashCode * 31 + c;
  }

  static final FibSet<CharSequence, FibEntry<CharSequence>> cache = new FibSet<>(FibEntry::new);

  /**
   * The empty string ({@code ""}).
   *
   * @since 2.0.0
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public static final @NotNull String EMPTY = "";

  static String cache(
      final @NotNull FibSetOp op, final @NotNull CharSequence chars, final @NotNull FibRefType refType) {
    //noinspection ConstantConditions
    if (chars == null || chars.length() == 0) {
      return EMPTY;
    }
    final FibSet<CharSequence, FibEntry<CharSequence>> cache = StringCache.cache;

    // Cheap try.
    FibEntry<CharSequence> entry = cache.execute(GET, chars, refType);
    if (entry != null) {
      assert entry.key instanceof String;
      return (String) entry.key;
    }
    // We need everything to be eventually NFKC normalized (Compatibility decomposition, followed by canonical
    // composition)!
    final String string = Normalizer.normalize(chars, Form.NFKC);
    entry = cache.execute(GET, string, refType);
    if (entry != null) {
      assert entry.key instanceof String;
      return (String) entry.key;
    }
    if (op == GET) {
      // Key does not exist, and we should not create it.
      return null;
    }
    assert op == PUT || op == REMOVE;
    entry = cache.execute(op, string, refType);
    if (entry == null) {
      assert op == REMOVE;
      return null;
    }
    assert entry.key instanceof String;
    return (String) entry.key;
  }
}
