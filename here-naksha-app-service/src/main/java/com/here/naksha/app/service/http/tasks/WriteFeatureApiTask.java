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
import static com.here.naksha.app.service.http.apis.ApiParams.FEATURE_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.PREFIX_ID;
import static com.here.naksha.app.service.http.apis.ApiParams.REMOVE_TAGS;
import static com.here.naksha.app.service.http.apis.ApiParams.SPACE_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewDeserialize.User;
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
    UPSERT_FEATURES,
    UPDATE_BY_ID,
    DELETE_FEATURES
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
      return switch (this.reqType) {
        case CREATE_FEATURES -> executeCreateFeatures();
        case UPSERT_FEATURES -> executeUpsertFeatures();
        case UPDATE_BY_ID -> executeUpdateFeature();
        default -> executeUnsupported();
      };
    } catch (XyzErrorException ex) {
      return verticle.sendErrorResponse(routingContext, ex.xyzError, ex.getMessage());
    } catch (Exception ex) {
      // unexpected exception
      logger.error("Exception processing Http request. ", ex);
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeCreateFeatures() throws Exception {
    // Deserialize input request
    final FeatureCollectionRequest collectionRequest = featuresFromRequestBody();
    final List<XyzFeature> features = (List<XyzFeature>) collectionRequest.getFeatures();
    if (features.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Can't create empty features");
    }

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    final String prefixId = (queryParams != null) ? queryParams.getValueAsString(PREFIX_ID) : null;
    final List<String> addTags = (queryParams != null) ? queryParams.collectAllOfAsString(ADD_TAGS) : null;
    final List<String> removeTags = (queryParams != null) ? queryParams.collectAllOfAsString(REMOVE_TAGS) : null;

    // as applicable, modify features based on parameters supplied
    for (final XyzFeature feature : features) {
      feature.setIdPrefix(prefixId);
      addTagsToFeature(feature, addTags);
      removeTagsFromFeature(feature, removeTags);
    }

    final WriteXyzFeatures wrRequest = RequestHelper.createFeaturesRequest(spaceId, features);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class);
    }
  }

  private @NotNull XyzResponse executeUpsertFeatures() throws Exception {
    // Deserialize input request
    final FeatureCollectionRequest collectionRequest = featuresFromRequestBody();
    final List<XyzFeature> features = (List<XyzFeature>) collectionRequest.getFeatures();
    if (features.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Can't update empty features");
    }

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    final List<String> addTags = (queryParams != null) ? queryParams.collectAllOfAsString(ADD_TAGS) : null;
    final List<String> removeTags = (queryParams != null) ? queryParams.collectAllOfAsString(REMOVE_TAGS) : null;

    // as applicable, modify features based on parameters supplied
    for (final XyzFeature feature : features) {
      addTagsToFeature(feature, addTags);
      removeTagsFromFeature(feature, removeTags);
    }
    final WriteXyzFeatures wrRequest = RequestHelper.upsertFeaturesRequest(spaceId, features);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class);
    }
  }

  private @NotNull XyzResponse executeUpdateFeature() throws Exception {
    // Deserialize input request
    XyzFeature feature;
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      feature = json.reader(User.class).forType(XyzFeature.class).readValue(bodyJson);
    }

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final String featureId = ApiParams.extractMandatoryPathParam(routingContext, FEATURE_ID);

    final QueryParameterList queryParams = (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
    final List<String> addTags = (queryParams != null) ? queryParams.collectAllOfAsString(ADD_TAGS) : null;
    final List<String> removeTags = (queryParams != null) ? queryParams.collectAllOfAsString(REMOVE_TAGS) : null;

    if (!featureId.equals(feature.getId())) {
      return verticle.sendErrorResponse(
          routingContext,
          XyzError.ILLEGAL_ARGUMENT,
          "URI path parameter featureId is not the same as id in feature request body.");
    }

    // as applicable, modify features based on parameters supplied
    addTagsToFeature(feature, addTags);
    removeTagsFromFeature(feature, removeTags);

    final WriteXyzFeatures wrRequest = RequestHelper.updateFeatureRequest(spaceId, feature);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformWriteResultToXyzFeatureResponse(wrResult, XyzFeature.class);
    }
  }

  private @NotNull FeatureCollectionRequest featuresFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class)
          .forType(FeatureCollectionRequest.class)
          .readValue(bodyJson);
    }
  }

  private void addTagsToFeature(XyzFeature feature, List<String> addTags) {
    if (addTags != null) {
      feature.getProperties().getXyzNamespace().addTags(addTags, true);
    }
  }

  private void removeTagsFromFeature(XyzFeature feature, List<String> removeTags) {
    if (removeTags != null) {
      feature.getProperties().getXyzNamespace().removeTags(removeTags, true);
    }
  }
}
