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

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ReadResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteResult;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class that can be used for all Http API specific custom Task implementations.
 *
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
    } else if (rdResult instanceof ReadResult<?> rr) {
      // In case of success, convert result to success XyzResponse
      final List<R> features = new ArrayList<>();
      int cnt = 0;
      for (final R feature : rr.withFeatureType(type)) {
        features.add(feature);
        if (++cnt >= 1000) {
          break; // TODO : can be improved later (perhaps by accepting limit as an input)
        }
      }
      rr.close();
      final XyzFeatureCollection response = new XyzFeatureCollection().withFeatures(features);
      return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
    }
    // unexpected result type
    logger.error("Unexpected result type {}", rdResult.getClass().getSimpleName());
    return verticle.sendErrorResponse(
        routingContext,
        XyzError.EXCEPTION,
        "Unsupported result type : " + rdResult.getClass().getSimpleName());
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformReadResultToXyzFeatureResponse(
      final @NotNull Result rdResult, final @NotNull Class<R> type) {
    if (rdResult == null) {
      logger.error("Unexpected null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (rdResult instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else if (rdResult instanceof ReadResult<?> rr) {
      // In case of success, convert result to success XyzResponse
      final Iterator<R> iterator = rr.withFeatureType(type).iterator();
      if (!iterator.hasNext())
        return verticle.sendErrorResponse(
            routingContext, XyzError.NOT_FOUND, "The desired feature does not exist.");
      final List<R> features = new ArrayList<>();
      features.add(iterator.next());
      final XyzFeatureCollection featureResponse = new XyzFeatureCollection().withFeatures(features);
      return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, featureResponse);
    }
    // unexpected result type
    logger.error("Unexpected result type {}", rdResult.getClass().getSimpleName());
    return verticle.sendErrorResponse(
        routingContext,
        XyzError.EXCEPTION,
        "Unsupported result type : " + rdResult.getClass().getSimpleName());
  }

  protected <R extends XyzFeature> @NotNull XyzResponse transformWriteResultToXyzFeatureResponse(
      final @Nullable Result wrResult, final @NotNull Class<R> type) {
    if (wrResult == null) {
      // unexpected null response
      logger.error("Unexpected null result!");
      return verticle.sendErrorResponse(routingContext, XyzError.EXCEPTION, "Unexpected null result!");
    } else if (wrResult instanceof ErrorResult er) {
      // In case of error, convert result to ErrorResponse
      logger.error("Received error result {}", er);
      return verticle.sendErrorResponse(routingContext, er.reason, er.message);
    } else if (wrResult instanceof WriteResult<?> wr) {
      // In case of success, convert result to success XyzResponse
      //noinspection unchecked
      final WriteResult<R> featureWR = (WriteResult<R>) wr;
      final List<R> features =
          featureWR.results.stream().map(op -> op.object).toList();
      final XyzFeatureCollection featureResponse = new XyzFeatureCollection().withFeatures(features);
      return verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, featureResponse);
    }
    // unexpected result type
    logger.error("Unexpected result type {}", wrResult.getClass().getSimpleName());
    return verticle.sendErrorResponse(
        routingContext,
        XyzError.EXCEPTION,
        "Unsupported result type : " + wrResult.getClass().getSimpleName());
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
    } else if (wrResult instanceof WriteResult<?> wr) {
      // In case of success, convert result to success XyzResponse
      //noinspection unchecked
      final WriteResult<R> featureWR = (WriteResult<R>) wr;
      final List<R> features =
          featureWR.results.stream().map(op -> op.object).toList();
      return verticle.sendXyzResponse(
          routingContext,
          HttpResponseType.FEATURE_COLLECTION,
          new XyzFeatureCollection().withInsertedFeatures(features));
    }
    // unexpected result type
    logger.error("Unexpected result type {}", wrResult.getClass().getSimpleName());
    return verticle.sendErrorResponse(
        routingContext,
        XyzError.EXCEPTION,
        "Unsupported result type : " + wrResult.getClass().getSimpleName());
  }
}
