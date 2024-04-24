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

import static com.here.naksha.app.service.http.apis.ApiParams.extractMandatoryPathParam;
import static com.here.naksha.app.service.http.apis.ApiParams.extractParamAsStringList;
import static com.here.naksha.app.service.http.apis.ApiParams.queryParamsFromRequest;
import static com.here.naksha.common.http.apis.ApiParamsConst.CLIP_GEO;
import static com.here.naksha.common.http.apis.ApiParamsConst.DEF_FEATURE_LIMIT;
import static com.here.naksha.common.http.apis.ApiParamsConst.EAST;
import static com.here.naksha.common.http.apis.ApiParamsConst.FEATURE_ID;
import static com.here.naksha.common.http.apis.ApiParamsConst.FEATURE_IDS;
import static com.here.naksha.common.http.apis.ApiParamsConst.HANDLE;
import static com.here.naksha.common.http.apis.ApiParamsConst.LAT;
import static com.here.naksha.common.http.apis.ApiParamsConst.LIMIT;
import static com.here.naksha.common.http.apis.ApiParamsConst.LON;
import static com.here.naksha.common.http.apis.ApiParamsConst.MARGIN;
import static com.here.naksha.common.http.apis.ApiParamsConst.NORTH;
import static com.here.naksha.common.http.apis.ApiParamsConst.NULL_COORDINATE;
import static com.here.naksha.common.http.apis.ApiParamsConst.PROPERTY_SEARCH_OP;
import static com.here.naksha.common.http.apis.ApiParamsConst.RADIUS;
import static com.here.naksha.common.http.apis.ApiParamsConst.REF_FEATURE_ID;
import static com.here.naksha.common.http.apis.ApiParamsConst.REF_SPACE_ID;
import static com.here.naksha.common.http.apis.ApiParamsConst.SOUTH;
import static com.here.naksha.common.http.apis.ApiParamsConst.SPACE_ID;
import static com.here.naksha.common.http.apis.ApiParamsConst.TILE_ID;
import static com.here.naksha.common.http.apis.ApiParamsConst.TILE_TYPE;
import static com.here.naksha.common.http.apis.ApiParamsConst.WEST;
import static com.here.naksha.lib.core.models.storage.transformation.BufferTransformation.bufferInMeters;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.app.service.http.ops.PropertySearchUtil;
import com.here.naksha.app.service.http.ops.PropertySelectionUtil;
import com.here.naksha.app.service.http.ops.SpatialUtil;
import com.here.naksha.app.service.http.ops.TagsUtil;
import com.here.naksha.app.service.models.IterateHandle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadFeaturesProxyWrapper;
import com.here.naksha.lib.core.models.storage.ReadFeaturesProxyWrapper.ReadRequestType;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SOp;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.util.storage.ResultHelper;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
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
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParameters);

    // Validate parameters
    if (featureIds == null || featureIds.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing id parameter");
    }
    final ReadFeaturesProxyWrapper rdRequest = RequestHelper.readFeaturesByIdsRequest(spaceId, featureIds)
        .withReadRequestType(ReadRequestType.GET_BY_IDS)
        .withQueryParameters(Map.of(FEATURE_IDS, featureIds));

    // Forward request to NH Space Storage reader instance
    try (Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      final F1<XyzFeature, XyzFeature> preResponseProcessing =
          standardReadFeaturesPreResponseProcessing(propPaths, false, null);
      // transform Result to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(result, XyzFeature.class, preResponseProcessing);
    }
  }

  private @NotNull XyzResponse executeFeatureById() {
    // Parse and validate Path parameters
    final String spaceId = extractMandatoryPathParam(routingContext, SPACE_ID);
    final String featureId = extractMandatoryPathParam(routingContext, FEATURE_ID);
    final QueryParameterList queryParameters = queryParamsFromRequest(routingContext);
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParameters);

    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(spaceId, featureId)
        .withReadRequestType(ReadRequestType.GET_BY_ID)
        .withQueryParameters(Map.of(FEATURE_ID, featureId));

    // Forward request to NH Space Storage reader instance
    try (Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      final F1<XyzFeature, XyzFeature> preResponseProcessing =
          standardReadFeaturesPreResponseProcessing(propPaths, false, null);
      // transform Result to Http XyzFeature response
      return transformReadResultToXyzFeatureResponse(result, XyzFeature.class, preResponseProcessing);
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
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);
    final boolean clip = ApiParams.extractQueryParamAsBoolean(queryParams, CLIP_GEO, false);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;
    ApiParams.validateParamRange(WEST, west, -180, 180);
    ApiParams.validateParamRange(NORTH, north, -90, 90);
    ApiParams.validateParamRange(EAST, east, -180, 180);
    ApiParams.validateParamRange(SOUTH, south, -90, 90);

    // Prepare read request based on parameters supplied
    final Geometry bbox = RequestHelper.createBBoxEnvelope(west, south, east, north);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertySearchUtil.buildOperationForPropertySearchParams(queryParams);

    final Map<String, Object> queryParamsMap = new HashMap<>();
    queryParamsMap.put(WEST, west);
    queryParamsMap.put(NORTH, north);
    queryParamsMap.put(EAST, east);
    queryParamsMap.put(SOUTH, south);
    queryParamsMap.put(LIMIT, limit);
    if (propSearchOp != null) {
      queryParamsMap.put(PROPERTY_SEARCH_OP, propSearchOp);
    }

    final ReadFeatures rdRequest = new ReadFeaturesProxyWrapper()
        .withReadRequestType(ReadRequestType.GET_BY_BBOX)
        .withQueryParameters(queryParamsMap)
        .withLimit(limit)
        .addCollection(spaceId)
        .withSpatialOp(SOp.intersects(bbox));
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    // we will also apply response preprocessing (like property selection and geometry clipping)
    // if any of the options is enabled
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, clip, bbox);
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, 0, limit, null, preResponseProcessing);
  }

  private @NotNull XyzResponse executeFeaturesByTile() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final String tileType = ApiParams.extractMandatoryPathParam(routingContext, TILE_TYPE);
    final String tileId = ApiParams.extractMandatoryPathParam(routingContext, TILE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    // NOTE : queryParams can be null, but that is acceptable. We will move on with default values.
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);
    final boolean clip = ApiParams.extractQueryParamAsBoolean(queryParams, CLIP_GEO, false);
    final long margin = ApiParams.extractQueryParamAsLong(queryParams, MARGIN, false);
    ApiParams.validateParamRange(MARGIN, margin, 0, Integer.MAX_VALUE);
    long limit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final Geometry geo = SpatialUtil.buildGeometryForTile(tileType, tileId, (int) margin);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertySearchUtil.buildOperationForPropertySearchParams(queryParams);

    final Map<String, Object> queryParamsMap = new HashMap<>();
    queryParamsMap.put(MARGIN, margin);
    queryParamsMap.put(LIMIT, limit);
    queryParamsMap.put(TILE_TYPE, tileType);
    queryParamsMap.put(TILE_ID, tileId);
    if (propSearchOp != null) {
      queryParamsMap.put(PROPERTY_SEARCH_OP, propSearchOp);
    }

    final ReadFeatures rdRequest = new ReadFeaturesProxyWrapper()
        .withReadRequestType(ReadRequestType.GET_BY_TILE)
        .withQueryParameters(queryParamsMap)
        .withLimit(limit)
        .addCollection(spaceId)
        .withSpatialOp(SOp.intersects(geo));
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    // we will also apply response preprocessing (like property selection and geometry clipping)
    // if any of the options is enabled
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, clip, geo);
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, 0, limit, null, preResponseProcessing);
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
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;

    // Prepare read request based on parameters supplied
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertySearchUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withLimit(limit);
    if (tagsOp == null && propSearchOp == null) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Atleast Tags or Prop search parameters required.");
    }
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, false, null);
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, 0, limit, null, preResponseProcessing);
  }

  private @NotNull XyzResponse executeIterate() {
    // Parse and validate Path parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    // Parse and validate Query parameters
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);

    // Parse property selection
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);

    // Note : subsequent steps need to support queryParams being null

    // extract limit parameter
    long offset = 0;
    long clientLimit = ApiParams.extractQueryParamAsLong(queryParams, LIMIT, false, DEF_FEATURE_LIMIT);
    // extract handle parameter
    IterateHandle handle = ApiParams.extractQueryParamAsIterateHandle(queryParams, HANDLE);
    // create new "handle" if not already provided, or overwrite parameters based on "handle"
    if (handle == null) {
      handle = new IterateHandle().withLimit(clientLimit);
    }
    offset = handle.getOffset();
    clientLimit = handle.getLimit();
    clientLimit = (clientLimit < 0 || clientLimit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : clientLimit;
    final Map<String, Object> queryParamsMap = Map.of(LIMIT, clientLimit);

    // Prepare read request based on parameters supplied
    final ReadFeatures rdRequest = new ReadFeaturesProxyWrapper()
        .withReadRequestType(ReadRequestType.ITERATE)
        .withQueryParameters(queryParamsMap)
        .withLimit(clientLimit + offset)
        .addCollection(spaceId);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, false, null);
    // transform Result to Http FeatureCollection response,
    // restricted by given feature limit and by adding "handle" attribute to support subsequent iteration
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, offset, clientLimit, handle, preResponseProcessing);
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
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);
    final boolean clip = ApiParams.extractQueryParamAsBoolean(queryParams, CLIP_GEO, false);
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
    final POp propSearchOp = PropertySearchUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(radiusOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // TODO pass the correct transformed geometry into this method call, also use the boolean clip
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, false, radiusOp.getGeometry());
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, 0, limit, null, preResponseProcessing);
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
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(refSpaceId, refFeatureId)
        .withReadRequestType(ReadRequestType.GET_BY_ID)
        .withQueryParameters(Map.of(FEATURE_ID, refFeatureId));
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
    final Set<String> propPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(queryParams);
    // validate values
    limit = (limit < 0 || limit > DEF_FEATURE_LIMIT) ? DEF_FEATURE_LIMIT : limit;
    ApiParams.validateParamRange(RADIUS, radius, 0, Long.MAX_VALUE);

    // Obtain reference geometry based on given coordinates or feature reference
    final XyzGeometry refGeometry = parseRequestBodyAs(XyzGeometry.class);

    // Prepare read request based on parameters supplied
    final SOp radiusOp =
        (radius > 0) ? SOp.intersects(refGeometry, bufferInMeters(radius)) : SOp.intersects(refGeometry);
    final POp tagsOp = TagsUtil.buildOperationForTagsQueryParam(queryParams);
    final POp propSearchOp = PropertySearchUtil.buildOperationForPropertySearchParams(queryParams);
    final ReadFeatures rdRequest = new ReadFeatures().addCollection(spaceId).withSpatialOp(radiusOp);
    RequestHelper.combineOperationsForRequestAs(rdRequest, OpType.AND, tagsOp, propSearchOp);

    // Forward request to NH Space Storage reader instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // TODO pass the correct transformed geometry into this method call, also use the boolean clip
    final F1<XyzFeature, XyzFeature> preResponseProcessing =
        standardReadFeaturesPreResponseProcessing(propPaths, false, radiusOp.getGeometry());
    // transform Result to Http FeatureCollection response, restricted by given feature limit
    return transformReadResultToXyzCollectionResponse(
        result, XyzFeature.class, 0, limit, null, preResponseProcessing);
  }
}
