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
package com.here.naksha.storage.http;

import static com.here.naksha.common.http.apis.ApiParamsConst.*;

import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HttpStorageReadExecute {

  private static final Logger log = LoggerFactory.getLogger(HttpStorageReadExecute.class);

  @NotNull
  static Result execute(ReadFeaturesProxyWrapper request, RequestSender sender) {

    return switch (request.getReadRequestType()) {
      case GET_BY_ID -> executeFeatureById(request, sender);
      case GET_BY_IDS -> executeFeaturesById(request, sender);
      case GET_BY_BBOX -> executeFeatureByBBox(request, sender);
      case GET_BY_TILE -> executeFeaturesByTile(request, sender);
    };
  }

  private static Result executeFeatureById(ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String featureId = readRequest.getQueryParameter(FEATURE_ID);

    HttpResponse<String> response =
        requestSender.sendRequest(String.format("/%s/features/%s", baseEndpoint(readRequest), featureId));

    return prepareResult(response, XyzFeature.class, List::of);
  }

  private static Result executeFeaturesById(ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    List<String> featureIds = readRequest.getQueryParameter(FEATURE_IDS);
    String queryParamsString = FEATURE_IDS + "=" + String.join(",", featureIds);

    HttpResponse<String> response = requestSender.sendRequest(
        String.format("/%s/features?%s", baseEndpoint(readRequest), queryParamsString));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static Result executeFeatureByBBox(ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String queryParamsString = keysToKeyValuesStrings(readRequest, WEST, NORTH, EAST, SOUTH, LIMIT);

    warnOnUnsupportedQueryParam(readRequest, PROPERTY_SEARCH_OP);

    HttpResponse<String> response =
        requestSender.sendRequest(String.format("/%s/bbox?%s", baseEndpoint(readRequest), queryParamsString));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static Result executeFeaturesByTile(ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String queryParamsString = keysToKeyValuesStrings(readRequest, MARGIN, LIMIT);
    String tileType = readRequest.getQueryParameter(TILE_TYPE);
    String tileId = readRequest.getQueryParameter(TILE_ID);

    if (tileType != null && !tileType.equals(TILE_TYPE_QUADKEY))
      return new ErrorResult(XyzError.NOT_IMPLEMENTED, "Tile type other than " + TILE_TYPE_QUADKEY);
    warnOnUnsupportedQueryParam(readRequest, PROPERTY_SEARCH_OP);

    HttpResponse<String> response = requestSender.sendRequest(
        String.format("/%s/quadkey/%s?%s", baseEndpoint(readRequest), tileId, queryParamsString));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static <T extends Typed> Result prepareResult(
      HttpResponse<String> httpResponse,
      Class<T> httpResponseType,
      Function<T, List<XyzFeature>> typedResponseToFeatureList) {

    XyzError error = mapHttpStatusToErrorOrNull(httpResponse.statusCode());
    if (error != null) return new ErrorResult(error, "Response http status code: " + httpResponse.statusCode());

    T resultFeatures = JsonSerializable.deserialize(httpResponse.body(), httpResponseType);
    return createHttpResultFromFeatureList(typedResponseToFeatureList.apply(resultFeatures));
  }

  private static void warnOnUnsupportedQueryParam(ReadFeaturesProxyWrapper readRequest, String key) {
    if (readRequest.getQueryParameter(key) != null)
      log.warn("The " + key + " query param for " + readRequest.getReadRequestType()
          + " is not supported yet and will be ignored.");
  }

  /**
   * Only for keys with string values
   */
  private static String keysToKeyValuesStrings(ReadFeaturesProxyWrapper readRequest, String... key) {
    return Arrays.stream(key)
        .map(k -> k + "=" + readRequest.getQueryParameter(k))
        .collect(Collectors.joining("&"));
  }

  private static HttpSuccessResult<XyzFeature, XyzFeatureCodec> createHttpResultFromFeatureList(
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

  private static String baseEndpoint(ReadFeaturesProxyWrapper request) {
    return request.getCollections().get(0);
  }
}
