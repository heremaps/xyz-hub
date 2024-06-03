/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.naksha.app.service.http.tasks.NoElementsStrategy.FAIL_ON_NO_ELEMENTS;
import static com.here.naksha.app.service.http.tasks.NoElementsStrategy.NOT_FOUND_ON_NO_ELEMENTS;
import static com.here.naksha.common.http.apis.ApiParamsConst.DEF_ADMIN_FEATURE_LIMIT;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeatureFromResult;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesGroupedByOp;
import static java.util.Collections.emptyList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.models.IterateHandle;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.PropertyPathUtil;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.RoutingContext;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryFixer;
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
    return transformResultToXyzFeatureResponse(rdResult, type, NOT_FOUND_ON_NO_ELEMENTS, null);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzFeatureResponse(
      final @NotNull Result rdResult, final @NotNull Class<R> type, @Nullable F1<R, R> preResponseProcessing) {
    return transformResultToXyzFeatureResponse(rdResult, type, NOT_FOUND_ON_NO_ELEMENTS, preResponseProcessing);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(wrResult, type, FAIL_ON_NO_ELEMENTS, null);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type, @Nullable F1<R, R> preResponseProcessing) {
    return transformResultToXyzFeatureResponse(wrResult, type, FAIL_ON_NO_ELEMENTS, preResponseProcessing);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformDeleteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(wrResult, type, NOT_FOUND_ON_NO_ELEMENTS, null);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformDeleteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type, @Nullable F1<R, R> postProcessing) {
    return transformResultToXyzFeatureResponse(wrResult, type, NOT_FOUND_ON_NO_ELEMENTS, postProcessing);
  }

  protected XyzResponse handleNoElements(NoElementsStrategy noElementsStrategy) {
    return verticle.sendErrorResponse(routingContext, noElementsStrategy.xyzError, noElementsStrategy.message);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformResultToXyzFeatureResponse(
      final @Nullable Result result,
      final @NotNull Class<R> type,
      final @NotNull NoElementsStrategy noElementsStrategy,
      final @Nullable F1<R, R> preResponseProcessing) {
    final XyzResponse validatedErrorResponse = validateErrorResult(result);
    if (validatedErrorResponse != null) {
      return validatedErrorResponse;
    } else {
      try {
        final R feature = readFeatureFromResult(result, type);
        R processedFeature = feature;
        if (feature != null && preResponseProcessing != null) {
          processedFeature = preResponseProcessing.call(feature);
        }
        if (processedFeature == null) {
          return verticle.sendErrorResponse(
              routingContext,
              XyzError.NOT_FOUND,
              "No feature found for id "
                  + result.getXyzFeatureCursor().getId());
        }
        final List<R> featureList = new ArrayList<>();
        featureList.add(processedFeature);
        final XyzFeatureCollection featureResponse = new XyzFeatureCollection().withFeatures(featureList);
        return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, featureResponse);
      } catch (NoCursor | NoSuchElementException emptyException) {
        return handleNoElements(noElementsStrategy);
      }
    }
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult,
      final @NotNull Class<R> type,
      final @Nullable F1<R, R> preResponseProcessing) {
    return transformReadResultToXyzCollectionResponse(
        rdResult, type, 0, DEF_ADMIN_FEATURE_LIMIT, null, preResponseProcessing);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult, final @NotNull Class<R> type) {
    return transformReadResultToXyzCollectionResponse(rdResult, type, DEF_ADMIN_FEATURE_LIMIT);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult, final @NotNull Class<R> type, final long maxLimit) {
    return transformReadResultToXyzCollectionResponse(rdResult, type, 0, maxLimit, null, null);
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult,
      final @NotNull Class<R> type,
      final long offset,
      final long maxLimit,
      final @Nullable IterateHandle handle,
      final @Nullable F1<R, R> preResponseProcessing) {
    final XyzResponse validatedErrorResponse = validateErrorResultEmptyCollection(rdResult);
    if (validatedErrorResponse != null) {
      return validatedErrorResponse;
    } else {
      try {
        final List<R> features = readFeaturesFromResult(rdResult, type, offset, maxLimit);
        List<R> processedFeatures = features;
        if (preResponseProcessing != null) {
          processedFeatures = new ArrayList<>();
          for (R feature : features) {
            final R processedFeature = preResponseProcessing.call(feature);
            if (processedFeature != null) {
              processedFeatures.add(processedFeature);
            }
          }
        }
        // Populate handle (if provided), with the values ready for next iteration
        final String handleStr = getIterateHandleAsString(processedFeatures.size(), offset, maxLimit, handle);
        return verticle.sendXyzResponse(
            routingContext,
            HttpResponseType.FEATURE_COLLECTION,
            new XyzFeatureCollection()
                .withFeatures(processedFeatures)
                .withNextPageToken(handleStr));
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
    final XyzResponse validatedErrorResponse = validateErrorResult(wrResult);
    if (validatedErrorResponse != null) {
      return validatedErrorResponse;
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

  XyzFeatureCollection emptyFeatureCollection() {
    return new XyzFeatureCollection().withFeatures(emptyList());
  }

  protected @Nullable XyzResponse validateErrorResultEmptyCollection(final @Nullable Result result) {
    if (result == null) {
      // return empty collection
      logger.warn("Unexpected null result, returning empty collection.");
      return verticle.sendXyzResponse(
          routingContext, HttpResponseType.FEATURE_COLLECTION, new XyzFeatureCollection());
    } else if (result instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    }
    return null;
  }

  protected @Nullable XyzResponse validateErrorResult(final @Nullable Result result) {
    if (result == null) {
      // return empty collection
      logger.error("Unexpected null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (result instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    }
    return null;
  }

  protected <F> @NotNull F parseRequestBodyAs(final Class<F> type) throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class).forType(type).readValue(bodyJson);
    }
  }

  protected <F extends XyzFeature> @Nullable F1<F, F> standardReadFeaturesPreResponseProcessing(
      final @Nullable Set<String> propPaths, final boolean clip, final Geometry clipGeo) {
    if (propPaths == null && !clip) return null;
    return f -> {
      F newF = f;
      // Apply prop selection if enabled
      if (propPaths != null) newF = applyPropertySelection(newF, propPaths);
      // Apply geometry clipping if enabled
      if (clip) applyGeometryClipping(newF, clipGeo);
      return newF;
    };
  }

  @SuppressWarnings("unchecked")
  private <F extends XyzFeature> @NotNull F applyPropertySelection(
      final @NotNull F f, final @NotNull Set<String> propPaths) {
    final Map<String, Object> tgtMap = PropertyPathUtil.extractPropertyMapFromFeature(f, propPaths);
    return (F) JsonSerializable.fromMap(tgtMap, f.getClass());
  }

  private <F extends XyzFeature> void applyGeometryClipping(final @NotNull F f, final Geometry clipGeo) {
    // clip Feature geometry (if present) to a given clipGeo geometry
    final XyzGeometry xyzGeo = f.getGeometry();
    if (xyzGeo != null) {
      // NOTE - in JTS when we say:
      //    GeometryFixer.fix(geom).intersection(bbox)
      // it is the best available way of clipping geometry, equivalent to PostGIS approach of:
      //    ST_Intersection(ST_MakeValid(geo, 'method=structure'), bbox)
      final Geometry clippedGeo =
          GeometryFixer.fix(xyzGeo.getJTSGeometry()).intersection(clipGeo);
      f.setGeometry(XyzGeometry.convertJTSGeometry(clippedGeo));
    }
  }
}
