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
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
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
      switch (this.reqType) {
        case GET_BY_ID:
          return executeFeatureById();
        case GET_BY_IDS:
          return executeFeaturesById();
        default:
          return executeUnsupported();
      }
    } catch (Exception ex) {
      // unexpected exception
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeFeaturesById() throws Exception {
    // Parse parameters
    final String spaceId = pathParam(routingContext, SPACE_ID);
    final List<String> featureIds = queryParamList(routingContext, FEATURE_IDS);

    // Validate parameters
    if (spaceId == null || spaceId.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing spaceId parameter");
    }
    if (featureIds == null || featureIds.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing id parameter");
    }

    // TODO : extract featureIds if they are comma separated (possibly using QueryParameterList.java)
    final List<String> extractedFeatureIds = new ArrayList<>();
    for (final String featureId : featureIds) {
      if (featureId.contains(",")) {
        for (Iterator<Object> it = new StringTokenizer(featureId, ",", false).asIterator(); it.hasNext(); ) {
          extractedFeatureIds.add((String) it.next());
        }
      }
    }

    // Forward request to NH Space Storage writer instance
    try (final IReadSession reader = naksha().getSpaceStorage().newReadSession(context(), true)) {
      final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdsRequest(spaceId, featureIds);
      final Result result = reader.execute(rdRequest);
      // transform Result to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(result, XyzFeature.class);
    }
  }

  private @NotNull XyzResponse executeFeatureById() throws Exception {
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

    // Forward request to NH Space Storage writer instance
    try (final IReadSession reader = naksha().getSpaceStorage().newReadSession(context(), true)) {
      final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdRequest(spaceId, featureId);
      final Result result = reader.execute(rdRequest);
      // transform Result to Http XyzFeature response
      return transformReadResultToXyzFeatureResponse(result, XyzFeature.class);
    }
  }
}
