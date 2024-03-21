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
import static com.here.naksha.storage.http.PrepareResult.prepareResult;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.storage.*;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HttpStorageReadExecute {

  private static final Logger log = LoggerFactory.getLogger(HttpStorageReadExecute.class);
  private static final String HDR_STREAM_ID = "Stream-Id";

  @NotNull
  static Result execute(@NotNull NakshaContext context, ReadFeaturesProxyWrapper request, RequestSender sender) {

    return switch (request.getReadRequestType()) {
      case GET_BY_ID -> executeFeatureById(context, request, sender);
      case GET_BY_IDS -> executeFeaturesById(context, request, sender);
      case GET_BY_BBOX -> executeFeatureByBBox(context, request, sender);
      case GET_BY_TILE -> executeFeaturesByTile(context, request, sender);
    };
  }

  private static Result executeFeatureById(
      @NotNull NakshaContext context, ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String featureId = readRequest.getQueryParameter(FEATURE_ID);

    HttpResponse<byte[]> response = requestSender.sendRequest(
        String.format("/%s/features/%s", baseEndpoint(readRequest), featureId),
        Map.of(HDR_STREAM_ID, context.getStreamId()));

    if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      // For Error 404 (not found) on single feature GetById request, we need to return empty result
      return prepareResult(Collections.emptyList());
    }
    return prepareResult(response, XyzFeature.class, List::of);
  }

  private static Result executeFeaturesById(
      @NotNull NakshaContext context, ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    List<String> featureIds = readRequest.getQueryParameter(FEATURE_IDS);
    String queryParamsString = FEATURE_IDS + "=" + String.join(",", featureIds);

    HttpResponse<byte[]> response = requestSender.sendRequest(
        String.format("/%s/features?%s", baseEndpoint(readRequest), queryParamsString),
        Map.of(HDR_STREAM_ID, context.getStreamId()));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static Result executeFeatureByBBox(
      @NotNull NakshaContext context, ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String queryParamsString = keysToKeyValuesStrings(readRequest, WEST, NORTH, EAST, SOUTH, LIMIT);

    HttpResponse<byte[]> response = requestSender.sendRequest(
        String.format(
            "/%s/bbox?%s%s", baseEndpoint(readRequest), queryParamsString, getPOpQueryOrEmpty(readRequest)),
        Map.of(HDR_STREAM_ID, context.getStreamId()));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static Result executeFeaturesByTile(
      @NotNull NakshaContext context, ReadFeaturesProxyWrapper readRequest, RequestSender requestSender) {
    String queryParamsString = keysToKeyValuesStrings(readRequest, MARGIN, LIMIT);
    String tileType = readRequest.getQueryParameter(TILE_TYPE);
    String tileId = readRequest.getQueryParameter(TILE_ID);

    if (tileType != null && !tileType.equals(TILE_TYPE_QUADKEY))
      return new ErrorResult(XyzError.NOT_IMPLEMENTED, "Tile type other than " + TILE_TYPE_QUADKEY);

    HttpResponse<byte[]> response = requestSender.sendRequest(
        String.format(
            "/%s/quadkey/%s?%s%s",
            baseEndpoint(readRequest), tileId, queryParamsString, getPOpQueryOrEmpty(readRequest)),
        Map.of(HDR_STREAM_ID, context.getStreamId()));

    return prepareResult(response, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  /**
   * @return either POp query string starting with "&" or an empty string if the POp is null
   */
  private static String getPOpQueryOrEmpty(ReadFeaturesProxyWrapper readRequest) {
    POp pOp = readRequest.getQueryParameter(PROPERTY_SEARCH_OP);
    return pOp == null ? "" : "&" + POpToQueryConverter.p0pToQuery(pOp);
  }

  /**
   * Only for keys with string values
   */
  private static String keysToKeyValuesStrings(ReadFeaturesProxyWrapper readRequest, String... key) {
    return Arrays.stream(key)
        .map(k -> k + "=" + readRequest.getQueryParameter(k))
        .collect(Collectors.joining("&"));
  }
  ;

  private static String baseEndpoint(ReadFeaturesProxyWrapper request) {
    return request.getCollections().get(0);
  }
}
