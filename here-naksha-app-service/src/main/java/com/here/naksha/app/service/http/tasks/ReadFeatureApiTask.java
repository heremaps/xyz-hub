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

import static com.here.naksha.app.service.http.apis.ApiParams.FEATURE_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.FEATURE_IDS;
import static com.here.naksha.app.service.http.apis.ApiParams.SPACE_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.pathParam;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
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
    GET_BY_IDS
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
        default -> executeUnsupported();
      };
    } catch (Exception ex) {
      // unexpected exception
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeFeaturesById() {
    // Parse parameters
    final String spaceId = pathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    final List<String> featureIds =
        (queryParams != null) ? queryParams.collectAllOf(FEATURE_IDS, String.class) : null;

    // Validate parameters
    if (spaceId == null || spaceId.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing spaceId parameter");
    }
    if (featureIds == null || featureIds.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing id parameter");
    }
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdsRequest(spaceId, featureIds);

    // Forward request to NH Space Storage writer instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http FeatureCollection response
    return transformReadResultToXyzCollectionResponse(result, XyzFeature.class);
  }

  private @NotNull XyzResponse executeFeatureById() {
    // Parse parameters
    final String spaceId = pathParam(routingContext, SPACE_ID);
    final String featureId = pathParam(routingContext, FEATURE_ID);

    // Validate parameters
    if (spaceId == null || spaceId.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing spaceId parameter");
    }
    if (featureId == null || featureId.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing id parameter");
    }
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(spaceId, featureId);

    // Forward request to NH Space Storage writer instance
    final Result result = executeReadRequestFromSpaceStorage(rdRequest);
    // transform Result to Http XyzFeature response
    return transformReadResultToXyzFeatureResponse(result, XyzFeature.class);
  }
}
