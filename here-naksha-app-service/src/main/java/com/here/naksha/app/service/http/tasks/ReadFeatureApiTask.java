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
import static com.here.naksha.lib.core.models.storage.transformation.BufferTransformation.bufferInMeters;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.app.service.http.ops.PropertyUtil;
import com.here.naksha.app.service.http.ops.SpatialUtil;
import com.here.naksha.app.service.http.ops.TagsUtil;
import com.here.naksha.app.service.models.IterateHandle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.util.storage.ResultHelper;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    SEARCH,
    ITERATE,
    GET_BY_RADIUS,
    GET_BY_RADIUS_POST
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
    try {
      return switch (this.reqType) {
        case GET_BY_ID -> executeFeatureById();
        case GET_BY_IDS -> executeFeaturesById();
        case GET_BY_BBOX -> executeFeaturesByBBox();
        case GET_BY_TILE -> executeFeaturesByTile();
        case SEARCH -> executeSearch();
        case ITERATE -> executeIterate();
        case GET_BY_RADIUS -> executeFeaturesByRadius();
        case GET_BY_RADIUS_POST -> executeFeaturesByRadiusPost();
        default -> executeUnsupported();
      };
    } catch (Exception ex) {
      if (ex instanceof XyzErrorException xyz) {
        logger.warn("Known exception while processing request. ", ex);
        return verticle.sendErrorResponse(routingContext, xyz.xyzError, xyz.getMessage());
      } else {
        logger.error("Unexpected error while processing request. ", ex);
        return verticle.sendErrorResponse(
            routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
      }
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

    // Forward request to NH Space Storage reader instance
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

    // Forward request to NH Space Storage reader instance
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
    final SOp bboxOp = SpatialUtil.buildOperationForBBox(west, south, east, north);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertyUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(bboxOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

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
    final long margin = ApiParams.extractQueryParamAsLong(queryParams, MARGIN, false);
    ApiParams.validateParamRange(MARGIN, margin, 0, Integer.MAX_VALUE);
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final SOp geoOp = SpatialUtil.buildOperationForTile(tileType, tileId, (int) margin);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertyUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(geoOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }

  private @NotNull XyzResponse executeSearch() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    if (queryParams == null || queryParams.size() <= 0) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing mandatory query parameters");
    }
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertyUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId);
    if (tagsOp == null && propSearchOp == null) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Atleast Tags or Prop search parameters required.");
    }
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }

  private @NotNull XyzResponse executeIterate() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);

    // Note : subsequent steps need to support queryParams being null

    // extract limit parameter
    long offset = 0;
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // extract handle parameter
    IterateHandle handle = ApiParams.extractQueryParamAsIterateHandle(queryParams, HANDLE);
    // create new "handle" if not already provided, or overwrite parameters based on "handle"
    if (handle == null) {
      handle = new IterateHandle().withLimit(limit);
    }
    offset = handle.getOffset();
    limit = handle.getLimit();
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response,
    // restricted by given feature limit and by adding "handle" attribute to support subsequent iteration
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, offset, limit, handle);
  }

  private @NotNull XyzResponse executeFeaturesByRadius() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    if (queryParams == null || queryParams.size() <= 0) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing mandatory parameters");
    }
    final double lat = ApiParams.extractQueryParamAsDouble(queryParams, LAT, false, NULL_COORDINATE);
    final double lon = ApiParams.extractQueryParamAsDouble(queryParams, LON, false, NULL_COORDINATE);
    final String refSpaceId = ApiParams.extractParamAsString(queryParams, REF_SPACE_ID);
    final String refFeatureId = ApiParams.extractParamAsString(queryParams, REF_FEATURE_ID);
    final long radius = ApiParams.extractQueryParamAsLong(queryParams, RADIUS, false, 0);
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;
    ApiParams.validateLatLon(lat, lon);
    ApiParams.validateParamRange(RADIUS, radius, 0, Long.MAX_VALUE);

    // Obtain reference geometry based on given coordinates or feature reference
    final XyzGeometry refGeometry = obtainReferenceGeometry(lat, lon, refSpaceId, refFeatureId);

    // Prepare read request based on parameters supplied
    final SOp radiusOp =
        (radius > 0) ? SOp.intersects(refGeometry, bufferInMeters(radius)) : SOp.intersects(refGeometry);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertyUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(radiusOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }

  private @NotNull XyzGeometry obtainReferenceGeometry(
      final double lat,
      final double lon,
      final @Nullable String refSpaceId,
      final @Nullable String refFeatureId) {
    // if both lan and lon provided, then prepare Point geometry
    if (lat != NULL_COORDINATE && lon != NULL_COORDINATE) {
      return new XyzPoint(lon, lat);
    }

    // Validate that both refSpaceId and refFeatureId provided and not just one
    if (refSpaceId == null || refSpaceId.isEmpty()) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing %s param".formatted(REF_SPACE_ID));
    } else if (refFeatureId == null || refFeatureId.isEmpty()) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing %s param".formatted(REF_FEATURE_ID));
    }

    // Find geometry by querying referenced feature
    XyzFeature feature = null;
    // Forward Read request to NHSpaceStorage instance
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(refSpaceId, refFeatureId);
    try (final Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      if (result instanceof SuccessResult) {
        feature = ResultHelper.readFeatureFromResult(result, XyzFeature.class);
      } else if (result instanceof ErrorResult er) {
        throw new XyzErrorException(er.reason, er.message);
      } else {
        throw new XyzErrorException(
            XyzError.EXCEPTION, "Unexpected result while retrieving referenced feature");
      }
    }
    if (feature == null) {
      throw new XyzErrorException(
          XyzError.NOT_FOUND,
          "No feature found for given spaceId %s and featureId %s".formatted(refSpaceId, refFeatureId));
    } else if (feature.getGeometry() == null) {
      throw new XyzErrorException(XyzError.NOT_FOUND, "Missing geometry for referenced feature");
    }

    return feature.getGeometry();
  }

  private @NotNull XyzResponse executeFeaturesByRadiusPost() throws Exception {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    // NOTE : queryParams can be null. Subsequent steps should respect the same.
    final long radius = ApiParams.extractQueryParamAsLong(queryParams, RADIUS, false, 0);
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;
    ApiParams.validateParamRange(RADIUS, radius, 0, Long.MAX_VALUE);

    // Obtain reference geometry based on given coordinates or feature reference
    final XyzGeometry refGeometry = parseRequestBodyAs(XyzGeometry.class);

    // Prepare read request based on parameters supplied
    final SOp radiusOp =
        (radius > 0) ? SOp.intersects(refGeometry, bufferInMeters(radius)) : SOp.intersects(refGeometry);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertyUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(radiusOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, limit);
  }
}
