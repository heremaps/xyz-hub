/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.core.exceptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper for checked exceptions.
 */
public final class UncheckedException extends RuntimeException {

  /**
   * Returns an unchecked exception for the given one, optionally being a checked exception.
   *
   * @param t The exception for which to return an unchecked version.
   * @return Either the given throwable or an unchecked wrapper exception.
   */
  public static @NotNull RuntimeException unchecked(@NotNull Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    }
    if (t instanceof IOException) {
      return new UncheckedIOException((IOException) t);
    }
    return new UncheckedException(t);
  }

  /**
   * Extract the checked exception, if the given one is a well-known unchecked exception that just wraps a checked exception.
   * @param t The exception to review.
   * @return The checked exception, if {@code t} is an envelope exception.
   */
  public static @NotNull Throwable causeOf(@NotNull Throwable t) {
    Throwable cause = t;
    while (true) {
      t = t.getCause();
      if (t == null) {
        return cause;
      }
      cause = t;
    }
  }

  /**
   * Extract the desired exception, if the given one is a well-known unchecked exception that just wraps a checked exception.
   * @param t The exception to review.
   * @param exceptionClass The class of the exception type to return.
   * @param <T> The exception-type to look for.
   * @return The exception if the given exception is either such an exception or a wrapped exception containing this exception as cause.
   */
  public static <T extends Throwable> @Nullable T causeOf(@NotNull Throwable t, @NotNull Class<T> exceptionClass) {
    while (t != null) {
      if (exceptionClass.isInstance(t)) {
        return exceptionClass.cast(t);
      }
      t = t.getCause();
    }
    return null;
  }

  /**
   * Extract the desired exception, if the given one wraps the searched exception as cause.
   * @param t The exception to review.
   * @param exceptionClass The class of the exception type to return.
   * @param <T> The exception-type to look for.
   * @return The exception if the given exception is either such an exception or a wrapped exception containing this exception as cause.
   * @throws RuntimeException If the given exception does not have the searched exception as cause or is it.
   */
  public static <T extends Throwable> @NotNull T rethrowExcept(
      final @NotNull Throwable t, final @NotNull Class<T> exceptionClass) {
    Throwable e = t;
    while (e != null) {
      if (exceptionClass.isInstance(e)) {
        return exceptionClass.cast(e);
      }
      e = e.getCause();
    }
    throw unchecked(t);
  }

  /**
   * Returns the original exception, the root-cause.
   *
   * @param t The exception.
   * @return The original error cause, removing all unnecessary wrappers.
   */
  public static @NotNull Throwable cause(@NotNull Throwable t) {
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t;
  }

  /**
   * Wraps the given cause.
   *
   * @param cause the cause.
   */
  public UncheckedException(@NotNull Throwable cause) {
    super(cause);
  }
}
