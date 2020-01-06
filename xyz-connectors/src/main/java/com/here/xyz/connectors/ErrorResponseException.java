/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
 * An exception, which contains an ErrorResponse object.
 */
public class ErrorResponseException extends Exception {

  private ErrorResponse errorResponse;

  public ErrorResponseException(String streamId, XyzError xyzError, String errorMessage) {
    super(errorMessage);
    this.errorResponse = new ErrorResponse()
        .withStreamId(streamId)
        .withError(xyzError)
        .withErrorMessage(errorMessage);
  }

  public ErrorResponseException(String streamId, XyzError xyzError, Exception e) {
    super(e);
    this.errorResponse = new ErrorResponse()
        .withStreamId(streamId)
        .withError(xyzError)
        .withErrorMessage(e.getMessage());
  }

  public ErrorResponse getErrorResponse() {
    return errorResponse;
  }
}
