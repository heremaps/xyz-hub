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
package com.here.naksha.storage.http;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Builds a {@link Result} from {@link HttpResponse}
 */
class PrepareResult {

  static Result prepareResult(List<XyzFeature> featureList) {
    return createHttpResultFromFeatureList(featureList);
  }

  static <T extends Typed> Result prepareResult(
      HttpResponse<byte[]> httpResponse,
      Class<T> httpResponseType,
      Function<T, List<XyzFeature>> typedResponseToFeatureList) {

    XyzError error = mapHttpStatusToErrorOrNull(httpResponse.statusCode());
    if (error != null) return new ErrorResult(error, "Response http status code: " + httpResponse.statusCode());

    T resultFeatures = JsonSerializable.deserialize(prepareBody(httpResponse), httpResponseType);
    return prepareResult(typedResponseToFeatureList.apply(resultFeatures));
  }

  private static String prepareBody(HttpResponse<byte[]> response) {
    List<String> contentEncodingList = response.headers().allValues("content-encoding");
    if (contentEncodingList.isEmpty()) return new String(response.body(), StandardCharsets.UTF_8);
    if (contentEncodingList.size() > 1)
      throw new IllegalArgumentException("There are more than one Content-Encoding value in response");
    String contentEncoding = contentEncodingList.get(0);

    if (contentEncoding.equalsIgnoreCase("gzip")) return gzipDecode(response.body());
    else throw new IllegalArgumentException("Encoding " + contentEncoding + " not recognized");
  }

  private static String gzipDecode(byte[] encoded) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(encoded);
        GZIPInputStream gis = new GZIPInputStream(bis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      gis.transferTo(bos);
      return bos.toString();
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  static HttpSuccessResult<XyzFeature, XyzFeatureCodec> createHttpResultFromFeatureList(
      final @NotNull List<XyzFeature> features) {
    // Create ForwardCursor with input features
    final List<XyzFeatureCodec> codecs = new ArrayList<>();
    final XyzFeatureCodecFactory codecFactory = XyzFeatureCodecFactory.get();
    for (final XyzFeature feature : features) {
      final XyzFeatureCodec codec = codecFactory.newInstance();
      codec.setOp(EExecutedOp.READ);
      codec.setFeature(feature);
      codec.setId(feature.getId());
      codecs.add(codec);
    }

    final HeapCacheCursor<XyzFeature, XyzFeatureCodec> cursor = new HeapCacheCursor<>(codecFactory, codecs, null);
    return new HttpSuccessResult<>(cursor);
  }

  /**
   * @return null if http status is success (200-299)
   */
  private static @Nullable XyzError mapHttpStatusToErrorOrNull(final int httpStatus) {
    if (httpStatus >= 200 && httpStatus <= 299) return null;
    return switch (httpStatus) {
      case HttpURLConnection.HTTP_INTERNAL_ERROR -> XyzError.EXCEPTION;
      case HttpURLConnection.HTTP_NOT_IMPLEMENTED -> XyzError.NOT_IMPLEMENTED;
      case HttpURLConnection.HTTP_BAD_REQUEST -> XyzError.ILLEGAL_ARGUMENT;
      case HttpURLConnection.HTTP_ENTITY_TOO_LARGE -> XyzError.PAYLOAD_TOO_LARGE;
      case HttpURLConnection.HTTP_BAD_GATEWAY -> XyzError.BAD_GATEWAY;
      case HttpURLConnection.HTTP_CONFLICT -> XyzError.CONFLICT;
      case HttpURLConnection.HTTP_UNAUTHORIZED -> XyzError.UNAUTHORIZED;
      case HttpURLConnection.HTTP_FORBIDDEN -> XyzError.FORBIDDEN;
      case 429 -> XyzError.TOO_MANY_REQUESTS;
      case HttpURLConnection.HTTP_GATEWAY_TIMEOUT -> XyzError.TIMEOUT;
      case HttpURLConnection.HTTP_NOT_FOUND -> XyzError.NOT_FOUND;
      default -> throw new IllegalArgumentException("Not known http error status returned: " + httpStatus);
    };
  }
}
