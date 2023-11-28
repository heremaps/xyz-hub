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
    if (a instanceof ILike && ((ILike) a).isLike(b)) {
      return true;
    }
    if (b instanceof ILike && ((ILike) b).isLike(a)) {
      return true;
    }
    if (a instanceof CharSequence && b instanceof CharSequence) {
      return StringHelper.equals((CharSequence) a, (CharSequence) b);
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
