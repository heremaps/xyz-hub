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
import static com.here.naksha.lib.core.util.diff.PatcherUtils.removeAllRemoveOp;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.diff.Difference;
import com.here.naksha.lib.core.util.diff.Patcher;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteFeatureApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final Logger logger = LoggerFactory.getLogger(WriteFeatureApiTask.class);
  private final @NotNull WriteFeatureApiReqType reqType;

  public enum WriteFeatureApiReqType {
    CREATE_FEATURES,
    UPSERT_FEATURES,
    UPDATE_BY_ID,
    DELETE_FEATURES,
    DELETE_BY_ID,
    PATCH_BY_ID
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
          // TODO : POST API needs to act as UPSERT for UI wiring due to backward compatibility.
          //  It may need to be readjusted, once we better understand difference
          //  (if there is anything other than PATCH, which is already known)
        case CREATE_FEATURES -> executeUpsertFeatures();
        case UPSERT_FEATURES -> executeUpsertFeatures();
        case UPDATE_BY_ID -> executeUpdateFeature();
        case DELETE_FEATURES -> executeDeleteFeatures();
        case DELETE_BY_ID -> executeDeleteFeature();
        case PATCH_BY_ID -> executePatchFeatureById();
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

  private @NotNull XyzResponse executeCreateFeatures() throws Exception {
    // Deserialize input request
    final FeatureCollectionRequest collectionRequest = featuresFromRequestBody();
    final List<XyzFeature> features = (List<XyzFeature>) collectionRequest.getFeatures();
    if (features.isEmpty()) {
      return verticle.sendErrorResponse(routingContext, XyzError.ILLEGAL_ARGUMENT, "Can't create empty features");
    }

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    final String prefixId = extractParamAsString(queryParams, PREFIX_ID);
    final List<String> addTags = extractParamAsStringList(queryParams, ADD_TAGS);
    final List<String> removeTags = extractParamAsStringList(queryParams, REMOVE_TAGS);

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
      return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class, false);
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
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    final List<String> addTags = extractParamAsStringList(queryParams, ADD_TAGS);
    final List<String> removeTags = extractParamAsStringList(queryParams, REMOVE_TAGS);

    // as applicable, modify features based on parameters supplied
    for (final XyzFeature feature : features) {
      addTagsToFeature(feature, addTags);
      removeTagsFromFeature(feature, removeTags);
    }
    final WriteXyzFeatures wrRequest = RequestHelper.upsertFeaturesRequest(spaceId, features);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class, false);
    }
  }

  private @NotNull XyzResponse executeUpdateFeature() throws Exception {
    // Deserialize input request
    final XyzFeature feature = singleFeatureFromRequestBody();

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    final List<String> addTags = extractParamAsStringList(queryParams, ADD_TAGS);
    final List<String> removeTags = extractParamAsStringList(queryParams, REMOVE_TAGS);

    // Validate parameters
    validateFeatureId(routingContext, feature.getId());

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

  private @NotNull XyzResponse executeDeleteFeatures() {
    // Deserialize input request
    final QueryParameterList queryParameters = queryParamsFromRequest(routingContext);
    final List<String> features = extractParamAsStringList(queryParameters, FEATURE_IDS);
    if (features == null || features.isEmpty()) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, "Missing feature id parameter");
    }

    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);

    final WriteXyzFeatures wrRequest = RequestHelper.deleteFeaturesByIdsRequest(spaceId, features);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class, true);
    }
  }

  private @NotNull XyzResponse executeDeleteFeature() {
    // Parse API parameters
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final String featureId = ApiParams.extractMandatoryPathParam(routingContext, FEATURE_ID);

    final WriteXyzFeatures wrRequest = RequestHelper.deleteFeatureByIdRequest(spaceId, featureId);

    // Forward request to NH Space Storage writer instance
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      // transform WriteResult to Http FeatureCollection response
      return transformDeleteResultToXyzFeatureResponse(wrResult, XyzFeature.class);
    }
  }

  private static final int MAX_RETRY_ATTEMPT = 5;

  private @NotNull XyzResponse executePatchFeatureById() throws JsonProcessingException {

    final XyzFeature featureFromRequest = singleFeatureFromRequestBody();

    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final QueryParameterList queryParams = queryParamsFromRequest(routingContext);
    final List<String> addTags = extractParamAsStringList(queryParams, ADD_TAGS);
    final List<String> removeTags = extractParamAsStringList(queryParams, REMOVE_TAGS);

    // Validate parameters
    validateFeatureId(routingContext, featureFromRequest.getId());

    final List<XyzFeature> featuresFromRequest = new ArrayList<>();
    featuresFromRequest.add(featureFromRequest);
    return attemptFeaturesPatching(spaceId, featuresFromRequest, HttpResponseType.FEATURE, addTags, removeTags, 0);
  }

  private XyzResponse attemptFeaturesPatching(
      @NotNull String spaceId,
      @NotNull List<XyzFeature> featuresFromRequest,
      @NotNull HttpResponseType responseType,
      @Nullable List<String> addTags,
      @Nullable List<String> removeTags,
      int retry) {
    // Patched feature list is to ensure the order of input features is retained
    final List<XyzFeature> patchedFeatures;
    List<XyzFeature> featuresToPatchFromStorage = new ArrayList<>();
    final List<String> featureIds = new ArrayList<>();
    for (XyzFeature feature : featuresFromRequest) {
      if (feature.getId() != null) {
        featureIds.add(feature.getId());
      }
    }
    // Extract the version of features in storage
    final ReadFeatures rdRequest = RequestHelper.readFeaturesByIdsRequest(spaceId, featureIds);
    try (Result result = executeReadRequestFromSpaceStorage(rdRequest)) {
      if (result == null) {
        return returnError(
            XyzError.EXCEPTION,
            "Unexpected null result while reading features from storage",
            "Unexpected null result while reading features from storage: {}",
            featureIds);
      } else if (result instanceof ErrorResult er) {
        // In case of error, convert result to ErrorResponse
        return returnError(
            er.reason, er.message, "Received error result while reading features in storage: {}", er);
      }
      try {
        featuresToPatchFromStorage = readFeaturesFromResult(result, XyzFeature.class, 0, DEF_FEATURE_LIMIT);
      } catch (NoCursor | NoSuchElementException emptyException) {
        if (responseType.equals(HttpResponseType.FEATURE)) {
          // If this is patching only 1 feature (PATCH by ID), return not found
          return returnError(
              XyzError.NOT_FOUND,
              "Feature does not exist.",
              "Unexpected null result while reading current versions in storage of targeted features for PATCH. The feature does not exist.");
        } else if (!responseType.equals(HttpResponseType.FEATURE_COLLECTION)) {
          // This function was then misused somewhere. FIND AND FIX IT!!
          return returnError(
              XyzError.EXCEPTION,
              "Internal server error.",
              "Unsupported HttpResponseType was called: {}",
              responseType);
        }
        // Else none of the features exists in storage, will create them later
      }
      // Attempt patching, keeping the order of the features from the request
      patchedFeatures =
          performInMemoryPatching(featuresFromRequest, featuresToPatchFromStorage, addTags, removeTags);
    }
    final WriteXyzFeatures wrRequest = RequestHelper.upsertFeaturesRequest(spaceId, patchedFeatures);
    // Forward request to NH Space Storage writer instance
    try (final IWriteSession writer = naksha().getSpaceStorage().newWriteSession(context(), true)) {
      final Result wrResult = writer.execute(wrRequest);
      if (wrResult == null) {
        // unexpected null response
        writer.rollback(true);
        writer.close();
        return returnError(
            XyzError.EXCEPTION,
            "Unexpected null result.",
            "Received null result after writing patched features, rolled back.");
      } else if (wrResult instanceof ErrorResult er) {
        writer.rollback(true);
        writer.close();
        try (ForwardCursor<XyzFeature, XyzFeatureCodec> resultCursor = er.getXyzFeatureCursor()) {
          if (!resultCursor.hasNext()) {
            throw new NoSuchElementException("Error Result Cursor is empty");
          }
          while (resultCursor.hasNext()) {
            if (!resultCursor.next()) {
              throw new RuntimeException("Unexpected invalid error result");
            }
            // Check if there is an error that is not about mismatching UUID
            if (EExecutedOp.ERROR.equals(resultCursor.getOp())) {
              if (!XyzError.CONFLICT.equals(Objects.requireNonNull(resultCursor.getError()).err)) {
                // Other types of error, will not retry
                return returnError(
                    resultCursor.getError().err,
                    resultCursor.getError().msg,
                    "Received error result {}",
                    resultCursor.getError());
              }
              // Else it was because of UUID mismatched
              final String featureIdFromErr = resultCursor.getId();
              // Find the requested change for the corresponding feature with that ID
              for (XyzFeature requestedChange : featuresFromRequest) {
                if (featureIdFromErr.equals(requestedChange.getId())) {
                  // If UUID input by user, will not retry, return conflict
                  if (requestedChange
                          .getProperties()
                          .getXyzNamespace()
                          .getUuid()
                      != null) {
                    return verticle.sendErrorResponse(
                        routingContext,
                        XyzError.CONFLICT,
                        "Error updating feature '" + featureIdFromErr + "', wrong UUID.");
                  }
                }
              }
            }
          }
          // Else the feature was modified concurrently within Naksha
          if (retry >= MAX_RETRY_ATTEMPT) {
            return returnError(
                XyzError.EXCEPTION,
                "Max retry attempt for PATCH REST API reached, too many concurrent modification, error: "
                    + er.message,
                "Max retry attempt for PATCH REST API reached, too many concurrent modification, error: {}",
                er.message);
          }
          // Attempt retry
          resultCursor.close();
          return attemptFeaturesPatching(
              spaceId, featuresFromRequest, responseType, addTags, removeTags, retry + 1);
        } catch (NoCursor e) {
          return returnError(
              XyzError.EXCEPTION,
              "Unexpected response when trying to persist patched features.",
              "No cursor when analyzing error result, while attempting to write patched features into storage.");
        }
      } else {
        if (responseType.equals(HttpResponseType.FEATURE)) {
          return transformWriteResultToXyzFeatureResponse(wrResult, XyzFeature.class);
        }
        return transformWriteResultToXyzCollectionResponse(wrResult, XyzFeature.class, false);
      }
    }
  }

  /**
   * Return a list of patched XyzFeature, including the ones not yet existing, ready for upsert
   */
  private List<XyzFeature> performInMemoryPatching(
      @NotNull List<XyzFeature> featuresFromRequest,
      List<XyzFeature> featuresToPatchFromStorage,
      @Nullable List<String> addTags,
      @Nullable List<String> removeTags) {
    final List<XyzFeature> patchedFeatureList = new ArrayList<>();
    for (final XyzFeature inputFeature : featuresFromRequest) {
      // we take input feature as default
      XyzFeature featureToPatch = inputFeature;
      // check if input feature matches with any of the existing features in storage
      if (inputFeature.getId() != null) {
        for (XyzFeature storageFeature : featuresToPatchFromStorage) {
          if (inputFeature.getId().equals(storageFeature.getId())) {
            // we found matching feature in storage, so we take patched version of the feature
            final Difference difference = Patcher.getDifference(storageFeature, inputFeature);
            final Difference diffNoRemoveOp = removeAllRemoveOp(difference);
            featureToPatch = Patcher.patch(storageFeature, diffNoRemoveOp);
            break;
          }
        }
      }
      // We now have featureToPatch which needs to be modified (if needed) and to be added to the list
      addTagsToFeature(featureToPatch, addTags);
      removeTagsFromFeature(featureToPatch, removeTags);
      patchedFeatureList.add(featureToPatch);
    }
    return patchedFeatureList;
  }

  private XyzResponse returnError(
      XyzError xyzError, String httpResponseMsg, String internalLogMsg, Object... logArgs) {
    logger.error(internalLogMsg, logArgs);
    return verticle.sendErrorResponse(routingContext, xyzError, httpResponseMsg);
  }

  private @NotNull FeatureCollectionRequest featuresFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class)
          .forType(FeatureCollectionRequest.class)
          .readValue(bodyJson);
    }
  }

  private @NotNull XyzFeature singleFeatureFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class)
          .forType(XyzFeature.class)
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
