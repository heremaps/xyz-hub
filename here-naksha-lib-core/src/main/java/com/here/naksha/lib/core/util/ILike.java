package com.here.naksha.lib.core.util;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * An interface to support a like equals. Two objects are like equal, when at least one of them
 * implements the {@link ILike} interface and at least one of them returns <code>true</code> for
 * {@link #isLike(Object) isLike(other)}. For more details see {@link #isLike(Object)}.
 *
 * @since 2.0.0
 */
public interface ILike {

  /**
   * Compare object “a” logically with an object “b”, adding special handling if any of them
   * implements the {@link ILike} or both implement the {@link CharSequence} interface.
   *
   * @param a the object to compare with “b”.
   * @param b the object to compare with “a”.
   * @return true if “a” and “b” are like equal; false otherwise.
   */
  static boolean equals(@Nullable Object a, @Nullable Object b) {
    if (a == b) {
      return true;
    }
    if (a instanceof ILike al && al.isLike(b)) {
      return true;
    }
    if (b instanceof ILike bl && bl.isLike(a)) {
      return true;
    }
    if (a instanceof CharSequence a_chars && b instanceof CharSequence b_chars) {
      return StringHelper.equals(a_chars, b_chars);
    }
    return Objects.equals(a, b);
  }

  /**
   * Compare this object logically with another one. Like is a not reversible equals. So, {@code
   * a.like(b)} must not be the same as {@code b.like(a)}. Still the meaning is that both objects
   * are logically the same, when either of the compares is {@code true}. For example, if an XML or
   * JSON node implements like, it can be compared against a string using {@code ILike.equals(node,
   * string)}. Comparing the string against the JSON node directly using {@code string.equals(node)}
   * will return {@code false}, but {@code ILike.equals(node, string)} may return {@code true}, when
   * the node somehow thinks it is equals to the given string, maybe when a text node compares the
   * stored text against the given string.
   *
   * <p><b>In a nutshell</b>: Two objects “a” and “b” are like equal, if either {@code a.isLike(b)}
   * <b>or</b> {@code b.isLike(a)} or {@code a == b} <b>or</b> {@code Objects.equals(a, b)}
   * <b>or</b> “a” and “b” are both {@link CharSequence character sequences} and {@code
   * StringCache.equals(a, b)} returns {@code true}, which simply treats the same character sequence
   * as equal, even when they are totally different classes.
   *
   * @param other the object to compare against; may be {@code null} and even allows to return
   *     {@code true}, when being {@code null}.
   * @return {@code true}, if this object is like the given one.
   */
  boolean isLike(@Nullable Object other);
}
