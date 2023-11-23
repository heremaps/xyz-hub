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

import static com.here.naksha.app.service.http.apis.ApiParams.ADD_TAGS;
import static com.here.naksha.app.service.http.apis.ApiParams.PREFIX_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.REMOVE_TAGS;
import static com.here.naksha.app.service.http.apis.ApiParams.SPACE_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.pathParam;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteFeatureApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final Logger logger = LoggerFactory.getLogger(WriteFeatureApiTask.class);
  private final @NotNull WriteFeatureApiReqType reqType;

  public enum WriteFeatureApiReqType {
    CREATE_FEATURES,
    MODIFY_FEATURES
  }

  public WriteFeatureApiTask(
      final @NotNull WriteFeatureApiReqType reqType,
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
    logger.info("Received Http request {}", this.reqType);
    // Custom execute logic to process input API request based on reqType
    try {
      switch (this.reqType) {
        case CREATE_FEATURES:
          return executeCreateFeatures();
        default:
          return executeUnsupported();
      }
    } catch (Exception ex) {
      // unexpected exception
      logger.error("Exception processing Http request. ", ex);
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeCreateFeatures() throws Exception {
    // Deserialize input request
    final FeatureCollectionRequest collectionRequest;
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      collectionRequest = json.reader(ViewDeserialize.User.class)
          .forType(FeatureCollectionRequest.class)
          .readValue(bodyJson);
    }
    final List<XyzFeature> features = (List<XyzFeature>) collectionRequest.getFeatures();
    if (features.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Can't create empty features");
    }

    // Parse API parameters
    final String spaceId = pathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    final String prefixId = (queryParams != null) ? queryParams.getValueOf(PREFIX_ID, String.class) : null;
    final List<String> addTags = (queryParams != null) ? queryParams.collectAllOf(ADD_TAGS, String.class) : null;
    final List<String> removeTags =
        (queryParams != null) ? queryParams.collectAllOf(REMOVE_TAGS, String.class) : null;

    // Validate parameters
    if (spaceId == null || spaceId.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing spaceId parameter");
    }

    // as applicable, modify features based on parameters supplied
    if (prefixId != null || addTags != null || removeTags != null) {
      for (final XyzFeature feature : features) {
        feature.setIdPrefix(prefixId);
        feature.getProperties().getXyzNamespace().addTags(addTags, true).removeTags(removeTags, true);
      }
    }
    final WriteFeatures<XyzFeature> wrRequest = RequestHelper.createFeaturesRequest(spaceId, features);

    // Forward request to NH Space Storage writer instance
    final Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest);
    // transform WriteResult to Http FeatureCollection response
    return transformWriteResultToXyzCollectionResponse(wrResult, Storage.class);
  }
}
