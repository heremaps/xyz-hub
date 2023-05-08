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

package com.here.xyz.hub.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;

import com.here.xyz.hub.Service;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Api {

  private static final Logger logger = LogManager.getLogger();

  public static final int MAX_SERVICE_RESPONSE_SIZE = (Service.configuration == null ? 0 :  Service.configuration.MAX_SERVICE_RESPONSE_SIZE);
  public static final int MAX_HTTP_RESPONSE_SIZE = (Service.configuration == null ? 0 :Service.configuration.MAX_HTTP_RESPONSE_SIZE);
  public static final HttpResponseStatus RESPONSE_PAYLOAD_TOO_LARGE = new HttpResponseStatus(513, "Response payload too large");
  public static final String RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE =
      "The response payload was too large. Please try to reduce the expected amount of data.";
  public static final HttpResponseStatus CLIENT_CLOSED_REQUEST = new HttpResponseStatus(499, "Client closed request");
  private static final String DEFAULT_GATEWAY_TIMEOUT_MESSAGE = "The storage connector exceeded the maximum time";
  private static final String DEFAULT_BAD_GATEWAY_MESSAGE = "The storage connector failed to execute the request";

  /**
   * Converts the given response into a {@link HttpException}.
   *
   * @param response the response to be converted.
   * @return the {@link HttpException} that reflects the response best.
   */
  public static HttpException responseToHttpException(final XyzResponse response) {
    if (response instanceof ErrorResponse) {
      return new HttpException(BAD_GATEWAY, ((ErrorResponse) response).getErrorMessage());
    }
    return new HttpException(BAD_GATEWAY, "Received invalid response of type '" + response.getClass().getSimpleName() + "'");
  }

  public static class HeaderValues {

    public static final String STREAM_ID = "Stream-Id";
    public static final String STREAM_INFO = "Stream-Info";
    public static final String STRICT_TRANSPORT_SECURITY = "Strict-Transport-Security";
    public static final String APPLICATION_GEO_JSON = "application/geo+json";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";
    public static final String APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST = "application/vnd.here.feature-modification-list";
    public static final String APPLICATION_VND_HERE_CHANGESET_COLLECTION = "application/vnd.here.changeset-collection";
    public static final String APPLICATION_VND_HERE_COMPACT_CHANGESET = "application/vnd.here.compact-changeset";
  }

  private static class XYZHttpContentCompressor extends HttpContentCompressor {

    private static final XYZHttpContentCompressor instance = new XYZHttpContentCompressor();

    static boolean isCompressionEnabled(String acceptEncoding) {
      if (acceptEncoding == null) {
        return false;
      }

      final ZlibWrapper wrapper = instance.determineWrapper(acceptEncoding);
      return wrapper == ZlibWrapper.GZIP || wrapper == ZlibWrapper.ZLIB;
    }
  }

}
