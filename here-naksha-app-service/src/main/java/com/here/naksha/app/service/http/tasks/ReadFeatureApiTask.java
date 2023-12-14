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
package com.here.naksha.app.service.http.tasks;

import static com.here.naksha.app.service.http.apis.ApiParams.*;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.app.service.http.apis.ApiUtil;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SOp;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFeatureApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final Logger logger = LoggerFactory.getLogger(ReadFeatureApiTask.class);
  private final @NotNull ReadFeatureApiReqType reqType;

  public enum ReadFeatureApiReqType {
    GET_BY_ID,
    GET_BY_IDS,
    GET_BY_BBOX,
    GET_BY_TILE,
    SEARCH
  }

  public ReadFeatureApiTask(
      final @NotNull ReadFeatureApiReqType reqType,
      final @NotNull NakshaHttpVerticle verticle,
      final @NotNull INaksha nakshaHub,
      final @NotNull RoutingContext routingContext,
      final @NotNull NakshaContext nakshaContext) {
    super(verticle, nakshaHub, routingContext, nakshaContext);
    this.reqType = reqType;
  }

  /**
   * Initializes this task.
   */
  @Override
  protected void init() {}

  /**
   * Execute this task.
   *
   * @return the response.
   */
  @Override
  protected @NotNull XyzResponse execute() {
    // TODO : Add custom execute logic to process input API request based on reqType
    try {
      return switch (this.reqType) {
        case GET_BY_ID -> executeFeatureById();
        case GET_BY_IDS -> executeFeaturesById();
        case GET_BY_BBOX -> executeFeaturesByBBox();
        case GET_BY_TILE -> executeFeaturesByTile();
        case SEARCH -> executeSearch();
        default -> executeUnsupported();
      };
    } catch (XyzErrorException ex) {
      return verticle.sendErrorResponse(routingContext, ex.xyzError, ex.getMessage());
    } catch (Exception ex) {
      // unexpected exception
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeFeaturesById() {
    // Parse parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParameters = queryParamsFromRequest(routingContext);
    final List<String> featureIds = extractParamAsStringList(queryParameters, FEATURE_IDS);

    // Validate parameters
    if (featureIds == null || featureIds.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing id parameter");
    }
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdsRequest(spaceId, featureIds);

    // Forward request to NH Space Storage writer instance
    try (Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      // transform Result to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(result, XyzFeature.class);
    }
  }

  private @NotNull XyzResponse executeFeatureById() {
    // Parse and validate Path parameters
    final String spaceId = extractMandatoryPathParam(routingContext, SPACE_ID);
    final String featureId = extractMandatoryPathParam(routingContext, FEATURE_ID);

    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(spaceId, featureId);

    // Forward request to NH Space Storage writer instance
    try (Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      // transform Result to Http XyzFeature response
      return transformReadResultToXyzFeatureResponse(result, XyzFeature.class);
    }
  }

  private @NotNull XyzResponse executeFeaturesByBBox() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    if (queryParams == null || queryParams.size() <= 0) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing mandatory parameters");
    }
    final double west = ApiParams.extractQueryParamAsDouble(queryParams, WEST, true);
    final double north = ApiParams.extractQueryParamAsDouble(queryParams, NORTH, true);
    final double east = ApiParams.extractQueryParamAsDouble(queryParams, EAST, true);
    final double south = ApiParams.extractQueryParamAsDouble(queryParams, SOUTH, true);
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;
    ApiParams.validateParamRange(WEST, west, -180, 180);
    ApiParams.validateParamRange(NORTH, north, -90, 90);
    ApiParams.validateParamRange(EAST, east, -180, 180);
    ApiParams.validateParamRange(SOUTH, south, -90, 90);

    // Prepare read request based on parameters supplied
    final SOp bboxOp = ApiUtil.buildOperationForBBox(west, south, east, north);
    final POp tagsOp = ApiUtil.buildOperationForTagsQueryParam(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(bboxOp);
    if (tagsOp != null) rdRequest.setPropertyOp(tagsOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }

  private @NotNull XyzResponse executeFeaturesByTile() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final String tileType = ApiParams.extractMandatoryPathParam(routingContext, TILE_TYPE);
    final String tileId = ApiParams.extractMandatoryPathParam(routingContext, TILE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    // NOTE : queryParams can be null, but that is acceptable. We will move on with default values.
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final SOp geoOp = ApiUtil.buildOperationForTile(tileType, tileId);
    final POp tagsOp = ApiUtil.buildOperationForTagsQueryParam(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(geoOp);
    if (tagsOp != null) rdRequest.setPropertyOp(tagsOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }

  private @NotNull XyzResponse executeSearch() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    if (queryParams == null || queryParams.size() <= 0) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing mandatory parameters");
    }
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final POp tagsOp = ApiUtil.buildOperationForTagsQueryParam(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId);
    if (tagsOp != null) rdRequest.setPropertyOp(tagsOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }
}
