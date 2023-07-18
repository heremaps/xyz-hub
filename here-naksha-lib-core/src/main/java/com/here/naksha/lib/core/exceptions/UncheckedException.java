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
package com.here.naksha.lib.core.exceptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.jetbrains.annotations.NotNull;

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
    if (t instanceof RuntimeException e) {
      return e;
    }
    if (t instanceof IOException e) {
      return new UncheckedIOException(e);
    }
    return new UncheckedException(t);
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
