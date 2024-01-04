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
package com.here.naksha.lib.core.models;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An enumeration of all possible error codes that can happen while processing a request.
 */
@AvailableSince(NakshaVersion.v2_0_0)
public class XyzError extends JsonEnum {

  /**
   * Returns the XYZ error for the given character sequence.
   *
   * @param chars The character sequence.
   * @return The XYZ error for this.
   */
  public static @NotNull XyzError get(@Nullable CharSequence chars) {
    return JsonEnum.get(XyzError.class, chars);
  }

  /**
   * The storage is not initialized.
   */
  public static final XyzError STORAGE_NOT_INITIALIZED = defIgnoreCase(XyzError.class, "StorageNotInitialized");

  /**
   * The collection accessed does not exist.
   */
  public static final XyzError COLLECTION_NOT_FOUND = defIgnoreCase(XyzError.class, "CollectionNotFound");

  /**
   * An unexpected error (not further specified) happened while processing the request.
   *
   * <p>This can result in a 500 Internal Server Error.
   */
  public static final XyzError EXCEPTION = defIgnoreCase(XyzError.class, "Exception");

  /**
   * An event that was sent to the connector failed, because the connector cannot process it.
   *
   * <p>This will result in an 501 Not Implemented response.
   */
  public static final XyzError NOT_IMPLEMENTED = defIgnoreCase(XyzError.class, "NotImplemented");

  /**
   * A conflict occurred when updating a feature.
   *
   * <p>This will result in an 409 Conflict response.
   */
  public static final XyzError CONFLICT = defIgnoreCase(XyzError.class, "Conflict");

  /**
   * Indicates an authorization error.
   *
   * <p>This will result in a 401 Unauthorized response.
   */
  public static final XyzError UNAUTHORIZED = defIgnoreCase(XyzError.class, "Unauthorized");

  /**
   * Indicates an authorization error.
   *
   * <p>This will result in an 403 Forbidden response.
   */
  public static final XyzError FORBIDDEN = defIgnoreCase(XyzError.class, "Forbidden");

  /**
   * The connector cannot handle the request due to a processing limitation in an upstream service or a database.
   *
   * <p>This will result in an 429 Too Many Requests response.
   */
  public static final XyzError TOO_MANY_REQUESTS = defIgnoreCase(XyzError.class, "TooManyRequests");

  /**
   * A provided argument is invalid or missing.
   *
   * <p>This will lead to a HTTP 400 Bad Request response.
   */
  public static final XyzError ILLEGAL_ARGUMENT = defIgnoreCase(XyzError.class, "IllegalArgument");

  /**
   * Any service or remote function required to process the request was not reachable.
   *
   * <p>This will result in a 502 Bad Gateway response.
   */
  public static final XyzError BAD_GATEWAY = defIgnoreCase(XyzError.class, "BadGateway");

  /**
   * The request was aborted due to a timeout.
   *
   * <p>This will result in a HTTP 504 Gateway Timeout response.
   */
  public static final XyzError TIMEOUT = defIgnoreCase(XyzError.class, "Timeout");

  /**
   * The request was aborted due to PayloadTooLarge.
   *
   * <p>This will result in a HTTP 513 response.
   */
  public static final XyzError PAYLOAD_TOO_LARGE = defIgnoreCase(XyzError.class, "PayloadTooLarge");

  /**
   * The requested feature was not available.
   *
   * <p>This will result in a HTTP 404 response.
   */
  public static final XyzError NOT_FOUND = defIgnoreCase(XyzError.class, "NotFound");

  @Override
  protected void init() {
    register(XyzError.class);
  }
}
