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

package com.here.xyz.connectors;

import static com.here.xyz.responses.XyzError.EXCEPTION;

import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.ErrorResponse;

/**
 * An exception, which will cause the connector to respond with an ErrorResponse object.
 */
public class ErrorResponseException extends Exception {

  private ErrorResponse errorResponse;

  public ErrorResponseException(XyzError xyzError, String errorMessage) {
    super(errorMessage);
    createErrorResponse(xyzError, errorMessage);
  }

  public ErrorResponseException(Exception cause) {
    super(cause);
    createErrorResponse(EXCEPTION, cause.getMessage());
  }

  public ErrorResponseException(XyzError xyzError, Exception cause) {
    super(cause);
    createErrorResponse(xyzError, cause.getMessage());
  }

  public ErrorResponseException(XyzError xyzError, String errorMessage, Exception cause) {
    super(errorMessage, cause);
    createErrorResponse(xyzError, errorMessage);
  }

  /**
   * @deprecated Please use constructor without stream ID (see above).
   *  The stream ID will be attached just before the ErrorResponse is being sent.
   * @param streamId
   * @param xyzError
   * @param errorMessage
   */
  @Deprecated
  public ErrorResponseException(String streamId, XyzError xyzError, String errorMessage) {
    super(errorMessage);
    createErrorResponse(xyzError, errorMessage);
  }

  private void createErrorResponse(XyzError xyzError, String errorMessage) {
    this.errorResponse = new ErrorResponse()
        .withError(xyzError)
        .withErrorMessage(errorMessage);
  }

  public ErrorResponse getErrorResponse() {
    return errorResponse;
  }
}
