/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.exceptions;

import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An exception, which will cause the connector to respond with an ErrorResponse object.
 */
public class XyzErrorException extends Exception {

  private ErrorResponse errorResponse;

  public XyzErrorException(@NotNull Throwable reason) {
    super(reason.getMessage(), reason);
    this.xyzError = XyzError.EXCEPTION;
  }

  public XyzErrorException(@NotNull XyzError xyzError, @NotNull String errorMessage) {
    this(xyzError, errorMessage, null);
  }

  public XyzErrorException(@NotNull XyzError xyzError, @NotNull Throwable reason) {
    super(reason.getMessage(), reason);
    this.xyzError = xyzError;
  }

  public XyzErrorException(@NotNull XyzError xyzError, @NotNull String errorMessage, @Nullable Throwable reason) {
    super(errorMessage, reason);
    this.xyzError = xyzError;
  }

  /**
   * The XYZ error to return.
   */
  public final @NotNull XyzError xyzError;

  public @NotNull ErrorResponse toErrorResponse(@NotNull String streamId) {
    return new ErrorResponse()
        .withStreamId(streamId)
        .withError(xyzError)
        .withErrorMessage(getMessage());
  }
}
