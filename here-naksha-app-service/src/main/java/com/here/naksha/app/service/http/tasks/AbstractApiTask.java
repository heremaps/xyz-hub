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

import static com.here.naksha.app.service.http.apis.ApiParams.DEF_ADMIN_FEATURE_LIMIT;
import static com.here.naksha.app.service.http.tasks.NoElementsStrategy.FAIL_ON_NO_ELEMENTS;
import static com.here.naksha.app.service.http.tasks.NoElementsStrategy.NOT_FOUND_ON_NO_ELEMENTS;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeatureFromResult;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesGroupedByOp;
import static java.util.Collections.emptyList;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.models.IterateHandle;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import io.vertx.ext.web.RoutingContext;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class that can be used for all Http API specific custom Task implementations.
 */
public abstract class AbstractApiTask<T extends XyzResponse>
    extends AbstractTask<XyzResponse, AbstractApiTask<XyzResponse>> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractApiTask.class);
  protected final @NotNull RoutingContext routingContext;
  protected final @NotNull NakshaHttpVerticle verticle;

  private static final String JSON_KEY_PASSWORD = "password";

  /**
   * Creates a new task.
   *
   * @param nakshaHub     The reference to the NakshaHub.
   * @param nakshaContext The reference to the NakshContext
   */
  protected AbstractApiTask(
      final @NotNull NakshaHttpVerticle verticle,
      final @NotNull INaksha nakshaHub,
      final @NotNull RoutingContext routingContext,
      final @NotNull NakshaContext nakshaContext) {
    super(nakshaHub, nakshaContext);
    this.verticle = verticle;
    this.routingContext = routingContext;
  }

  protected @NotNull XyzResponse errorResponse(@NotNull Throwable throwable) {
    logger.warn("The task failed with an exception. ", throwable);
    return verticle.sendErrorResponse(
        routingContext, XyzError.EXCEPTION, "Task failed processing! " + throwable.getMessage());
  }

  public @NotNull XyzResponse executeUnsupported() {
    return verticle.sendErrorResponse(routingContext, XyzError.NOT_IMPLEMENTED, "Unsupported operation!");
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzFeatureResponse(
      final @NotNull Result rdResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(rdResult, type, NOT_FOUND_ON_NO_ELEMENTS);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(wrResult, type, FAIL_ON_NO_ELEMENTS);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformDeleteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(wrResult, type, NOT_FOUND_ON_NO_ELEMENTS);
  }

  private XyzResponse handleNoElements(NoElementsStrategy noElementsStrategy) {
    return verticle.sendErrorResponse(routingContext, noElementsStrategy.xyzError, noElementsStrategy.message);
  }

  private <R extends XyzFeature> @NotNull XyzResponse transformResultToXyzFeatureResponse(
      final @Nullable Result result,
      final @NotNull Class<R> type,
      final @NotNull NoElementsStrategy noElementsStrategy) {
    if (result == null) {
      logger.error("Unexpected null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (result instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else {
      try {
        R feature = readFeatureFromResult(result, type);
        if (feature == null) {
          return verticle.sendErrorResponse(
              routingContext,
              XyzError.NOT_FOUND,
              "No feature found for id "
                  + result.getXyzFeatureCursor().getId());
        }
        if (Objects.equals(type, Storage.class)) {
          removePasswordFromProps(feature.getProperties());
        }
        final List<R> featureList = new ArrayList<>();
        featureList.add(feature);
        final XyzFeatureCollection featureResponse = new XyzFeatureCollection().withFeatures(featureList);
        return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, featureResponse);
      } catch (NoCursor | NoSuchElementException emptyException) {
        return handleNoElements(noElementsStrategy);
      }
    }
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult, final @NotNull Class<R> type) {
    return transformReadResultToXyzCollectionResponse(rdResult, type, DEF_ADMIN_FEATURE_LIMIT);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult, final @NotNull Class<R> type, final long maxLimit) {
    return transformReadResultToXyzCollectionResponse(rdResult, type, 0, maxLimit, null);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult,
      final @NotNull Class<R> type,
      final long offset,
      final long maxLimit,
      final @Nullable IterateHandle handle) {
    if (rdResult == null) {
      // return empty collection
      logger.warn("Unexpected null result, returning empty collection.");
      return verticle.sendXyzResponse(
          routingContext, HttpResponseType.FEATURE_COLLECTION, new XyzFeatureCollection());
    } else if (rdResult instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else {
      try {
        final List<R> features = readFeaturesFromResult(rdResult, type, offset, maxLimit);
        if (Objects.equals(type, Storage.class)) {
          for (R feature : features) {
            removePasswordFromProps(feature.getProperties());
          }
        }
        // Populate handle (if provided), with the values ready for next iteration
        final String handleStr = getIterateHandleAsString(features.size(), offset, maxLimit, handle);
        return verticle.sendXyzResponse(
            routingContext,
            HttpResponseType.FEATURE_COLLECTION,
            new XyzFeatureCollection().withFeatures(features).withNextPageToken(handleStr));
      } catch (NoCursor | NoSuchElementException emptyException) {
        logger.info("No data found in ResultCursor, returning empty collection");
        return verticle.sendXyzResponse(
            routingContext, HttpResponseType.FEATURE_COLLECTION, emptyFeatureCollection());
      }
    }
  }

  private static String getIterateHandleAsString(
      long featuresFound, long crtOffset, long maxLimit, final @Nullable IterateHandle handle) {
    // nothing to populate if handle is not provided OR if we don't have more features to iterate
    if (handle == null || featuresFound < maxLimit) return null;
    handle.setOffset(crtOffset + featuresFound); // set offset for next iteration
    handle.setLimit(maxLimit);
    return handle.base64EncodedSerializedJson();
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzCollectionResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type, final boolean isDeleteOperation) {
    if (wrResult == null) {
      // unexpected null response
      logger.error("Received null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (wrResult instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else {
      try {
        final Map<EExecutedOp, List<R>> featureMap = readFeaturesGroupedByOp(wrResult, type);
        final List<R> insertedFeatures = featureMap.get(EExecutedOp.CREATED);
        final List<R> updatedFeatures = featureMap.get(EExecutedOp.UPDATED);
        final List<R> deletedFeatures = featureMap.get(EExecutedOp.DELETED);
        // extract violations if available
        List<XyzFeature> violations = null;
        if (wrResult instanceof ContextXyzFeatureResult cr) {
          violations = cr.getViolations();
        }
        if (Objects.equals(type, Storage.class)) {
          for (R feature : insertedFeatures) {
            removePasswordFromProps(feature.getProperties());
          }
          for (R feature : updatedFeatures) {
            removePasswordFromProps(feature.getProperties());
          }
          for (R feature : deletedFeatures) {
            removePasswordFromProps(feature.getProperties());
          }
        }
        return verticle.sendXyzResponse(
            routingContext,
            HttpResponseType.FEATURE_COLLECTION,
            new XyzFeatureCollection()
                .withInsertedFeatures(insertedFeatures)
                .withUpdatedFeatures(updatedFeatures)
                .withDeletedFeatures(deletedFeatures)
                .withViolations(violations));
      } catch (NoCursor | NoSuchElementException emptyException) {
        if (isDeleteOperation) {
          logger.info("No data found in ResultCursor, returning empty collection");
          return verticle.sendXyzResponse(
              routingContext, HttpResponseType.FEATURE_COLLECTION, emptyFeatureCollection());
        }
        return verticle.sendErrorResponse(
            routingContext, XyzError.EXCEPTION, "Unexpected empty result from ResultCursor");
      }
    }
  }

  protected Result executeReadRequestFromSpaceStorage(ReadFeatures readRequest) {
    try (final IReadSession reader = naksha().getSpaceStorage().newReadSession(context(), false)) {
      return reader.execute(readRequest);
    }
  }

  protected Result executeWriteRequestFromSpaceStorage(WriteFeatures writeRequest) {
    try (final IWriteSession writer = naksha().getSpaceStorage().newWriteSession(context(), true)) {
      return writer.execute(writeRequest);
    }
  }

  private XyzFeatureCollection emptyFeatureCollection() {
    return new XyzFeatureCollection().withFeatures(emptyList());
  }

  private void removePasswordFromProps(Map<String, Object> propertiesAsMap) {
    for (Iterator<Map.Entry<String, Object>> it = propertiesAsMap.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, Object> entry = it.next();
      if (Objects.equals(entry.getKey(), JSON_KEY_PASSWORD)) {
        it.remove();
      } else if (entry.getValue() instanceof Map) {
        // recursive call to the nested json property
        removePasswordFromProps((Map<String, Object>) entry.getValue());
      } else if (entry.getValue() instanceof ArrayList array) {
        // recursive call to the nested array json
        for (Object arrayEntry : array) {
          removePasswordFromProps((Map<String, Object>) arrayEntry);
        }
      }
    }
  }
}
