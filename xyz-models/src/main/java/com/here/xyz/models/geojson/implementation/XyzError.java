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

package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.responses.ErrorResponse;

/**
 * An enumeration of all possible error codes that can happen while processing a request. Be aware that the XYZ Hub itself will respond
 * already with an HTTP status code
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum XyzError {
  /**
   * An event that was sent to a remote function which is required to process the request failed, because it does not support this event.
   * Details may be found in the {@link ErrorResponse#getErrorMessage()}.
   *
   * This will lead to a HTTP 502 Bad Gateway response.
   */
  NOT_IMPLEMENTED("NotImplemented"),

  /**
   * The tile level is not supported by a remove function required to process the request (for example the storage connector) or it is
   * generally invalid (for example less than zero or not well formed).
   *
   * This will lead to a HTTP 400 Bad Request response.
   */
  INVALID_TILE_LEVEL("InvalidTileLevel"),

  /**
   * A provided argument is invalid or missing.
   *
   * This will lead to a HTTP 400 Bad Request response.
   */
  ILLEGAL_ARGUMENT("IllegalArgument"),

  /**
   * Any service or remote function required to process the request was not reachable. Details about which remote function failed will be
   * found in the {@link ErrorResponse#getErrorMessage()}.
   *
   * This will lead to a HTTP 502 Bad Gateway response.
   */
  BAD_GATEWAY("BadGateway"),

  /**
   * The request was aborted due to a timeout.
   *
   * This will lead to a HTTP 504 Gateway Timeout response.
   */
  TIMEOUT("Timeout"),

  /**
   * An unexpected error (not further specified) happened while processing the request. Details will be found in the {@link
   * ErrorResponse#getErrorMessage()}.
   *
   * This can lead to different HTTP status codes, for example 500 Internal Server Error or 502 Bad Gateway, dependent on what was the
   * source of the error.
   */
  EXCEPTION("Exception");

  /**
   * The error code.
   */
  @JsonValue
  public final String value;

  /**
   * Create a new error code.
   *
   * @param error the error code value.
   */
  XyzError(String error) {
    value = error;
  }

  /**
   * Returns the enumeration item for the given value or null, when the given string is no valid error code.
   *
   * @param error the error code as string.
   * @return the error code as enumeration value.
   */
  public static XyzError forValue(String error) {
    if (error == null) {
      return null;
    }
    for (final XyzError e : XyzError.values()) {
      if (e.value.equalsIgnoreCase(error)) {
        return e;
      }
    }
    return null;
  }
}
