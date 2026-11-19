/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.iceberg;

import static com.here.xyz.models.hub.IcebergLocation.ERROR_DETAIL_COMPLETE;
import static com.here.xyz.models.hub.IcebergLocation.ERROR_DETAIL_LIMIT;
import static com.here.xyz.models.hub.IcebergLocation.ERROR_DETAIL_MAX_BYTES;
import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;

import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.XyzError;
import java.util.Map;

/**
 * Stops a scan with a specific error, e.g. when a limit of the request was hit.
 * It is unchecked, so it can be thrown from inside Iceberg's and Parquet's readers.
 */
class ScanException extends RuntimeException {

  private final XyzError error;
  private final Map<String, Object> details;

  ScanException(XyzError error, String message) {
    this(error, message, null);
  }

  private ScanException(XyzError error, String message, Map<String, Object> details) {
    super(message);
    this.error = error;
    this.details = details;
  }

  /**
   * A PAYLOAD_TO_LARGE error whose {@link #details()} name the limit which was hit, so that the Hub can tell a too large response
   * from a read which would read too much.
   *
   * @param limit {@link IcebergLocation#LIMIT_RESPONSE_BYTES} or {@link IcebergLocation#LIMIT_READ_BYTES}
   * @param maxBytes The value of the limit
   */
  static ScanException tooLarge(String limit, long maxBytes, String message) {
    return new ScanException(PAYLOAD_TO_LARGE, message, Map.of(ERROR_DETAIL_LIMIT, limit, ERROR_DETAIL_MAX_BYTES, maxBytes));
  }

  /**
   * A PAYLOAD_TO_LARGE error of a limit with the detail whether the read has all its matches.
   *
   * @param complete See {@link IcebergLocation#ERROR_DETAIL_COMPLETE}
   */
  static ScanException tooLarge(String limit, long maxBytes, boolean complete, String message) {
    return new ScanException(PAYLOAD_TO_LARGE, message, Map.of(ERROR_DETAIL_LIMIT, limit, ERROR_DETAIL_MAX_BYTES, maxBytes,
        ERROR_DETAIL_COMPLETE, complete));
  }

  XyzError error() {
    return error;
  }

  /**
   * @return The errorDetails of the connector's error response, or null
   */
  Map<String, Object> details() {
    return details;
  }
}
