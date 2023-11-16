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

import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static java.util.Collections.emptyList;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.NoSuchElementException;
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

  public @NotNull XyzResponse executeUnsupported() {
    return verticle.sendErrorResponse(routingContext, XyzError.NOT_IMPLEMENTED, "Unsupported operation!");
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzFeatureResponse(
      final @NotNull Result rdResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(
        rdResult,
        type,
        () -> verticle.sendErrorResponse(
            routingContext, XyzError.NOT_FOUND, "The desired feature does not exist."));
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    return transformResultToXyzFeatureResponse(
        wrResult,
        type,
        () -> verticle.sendErrorResponse(
            routingContext,
            XyzError.EXCEPTION,
            "Unexpected error while saving feature, the result cursor is empty / does not exist"));
  }

  private <R extends XyzFeature> @NotNull XyzResponse transformResultToXyzFeatureResponse(
      final @Nullable Result result,
      final @NotNull Class<R> type,
      final @NotNull F0<XyzResponse> onNoElementsReturned) {
    if (result == null) {
      logger.error("Unexpected null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (result instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else {
      try {
        List<R> features = readFeaturesFromResult(result, type);
        final XyzFeatureCollection featureResponse = new XyzFeatureCollection().withFeatures(features);
        return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, featureResponse);
      } catch (NoCursor | NoSuchElementException emptyException) {
        return onNoElementsReturned.call();
      }
    }
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzCollectionResponse(
      final @Nullable Result rdResult, final @NotNull Class<R> type) {
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
        List<R> features = readFeaturesFromResult(rdResult, type, 1000);
        return verticle.sendXyzResponse(
            routingContext,
            HttpResponseType.FEATURE_COLLECTION,
            new XyzFeatureCollection().withInsertedFeatures(features));
      } catch (NoCursor | NoSuchElementException emptyException) {
        logger.info("No data found in ResultCursor, returning empty collection");
        return verticle.sendXyzResponse(
            routingContext, HttpResponseType.FEATURE_COLLECTION, emptyFeatureCollection());
      }
    }
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzCollectionResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
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
        List<R> features = readFeaturesFromResult(wrResult, type);
        return verticle.sendXyzResponse(
            routingContext,
            HttpResponseType.FEATURE_COLLECTION,
            new XyzFeatureCollection().withInsertedFeatures(features));
      } catch (NoCursor | NoSuchElementException emptyException) {
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

  protected Result executeWriteRequestFromSpaceStorage(WriteFeatures<?> writeRequest) {
    try (final IWriteSession writer = naksha().getSpaceStorage().newWriteSession(context(), true)) {
      return writer.execute(writeRequest);
    }
  }

  private XyzFeatureCollection emptyFeatureCollection() {
    return new XyzFeatureCollection().withFeatures(emptyList());
  }
}
