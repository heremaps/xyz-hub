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

package com.here.xyz.connectors;

import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.ErrorResponse;

/**
 * An exception, which will cause the connector to respond with an ErrorResponse object.
 */
public class ErrorResponseException extends Exception {

  private ErrorResponse errorResponse;

  public ErrorResponseException(XyzError xyzError, String errorMessage) {
    this(null, xyzError, errorMessage);
  }

  public ErrorResponseException(String streamId, XyzError xyzError, String errorMessage) {
    super(errorMessage);
    createErrorResponse(streamId, xyzError, errorMessage);
  }

  public ErrorResponseException(String streamId, XyzError xyzError, Exception e) {
    super(e);
    createErrorResponse(streamId, xyzError, e.getMessage());
  }

  private void createErrorResponse(String streamId, XyzError xyzError, String errorMessage) {
    this.errorResponse = new ErrorResponse()
        .withStreamId(streamId)
        .withError(xyzError)
        .withErrorMessage(errorMessage);
  }

  public ErrorResponse getErrorResponse() {
    return errorResponse;
  }
}
