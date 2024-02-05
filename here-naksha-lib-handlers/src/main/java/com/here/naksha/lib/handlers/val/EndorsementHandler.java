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
package com.here.naksha.lib.handlers.val;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SEND_UPSTREAM_WITHOUT_PROCESSING;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.geojson.implementation.XyzReference;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.EReviewState;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.storage.ContextWriteFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.handlers.AbstractEventHandler;
import com.here.naksha.lib.handlers.util.HandlerUtil;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndorsementHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(EndorsementHandler.class);
  protected @NotNull EventHandler eventHandler;
  protected @NotNull EventTarget<?> eventTarget;
  protected @NotNull XyzProperties properties;

  public EndorsementHandler(
      final @NotNull EventHandler eventHandler,
      final @NotNull INaksha hub,
      final @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.eventHandler = eventHandler;
    this.eventTarget = eventTarget;
    this.properties = JsonSerializable.convert(eventHandler.getProperties(), XyzProperties.class);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ContextWriteFeatures<?, ?, ?, ?, ?>) {
      return PROCESS;
    }
    return SEND_UPSTREAM_WITHOUT_PROCESSING;
  }

  /**
   * The method invoked by the event-pipeline to process custom Storage specific read/write operations
   *
   * @param event the event to process.
   * @return the result.
   */
  @Override
  public @NotNull Result process(@NotNull IEvent event) {
    final Request<?> request = event.getRequest();

    logger.info("Handler received request {}", request.getClass().getSimpleName());

    final ContextWriteFeatures<?, ?, ?, ?, ?> cwf = HandlerUtil.checkInstanceOf(
        request, ContextWriteFeatures.class, "Unsupported request type during endorsement");

    // Extract violations from request
    final List<XyzFeature> violations = HandlerUtil.getXyzViolationsFromGenericList(cwf.getViolations());

    // Mark each feature as AUTO_REVIEW_DEFERRED or UNPUBLISHED
    // (depending on whether there is associated violation or not)
    final List<XyzFeature> updatedFeatures = HandlerUtil.getXyzFeaturesFromCodecList(cwf.features);
    for (final XyzFeature feature : updatedFeatures) {
      updateFeatureDeltaStateIfMatchesViolations(feature, violations);
    }

    // TODO : Extract context (list of features) and make the violated ones part of the updatedFeatures
    // list, so they also get updated in storage

    // create and forward request for next handler in the pipeline
    final ContextWriteFeatures<?, ?, ?, ?, ?> upstreamRequest =
        HandlerUtil.createContextWriteRequestFromFeatureList(
            cwf.getCollectionId(), updatedFeatures, cwf.getContext(), violations);
    return event.sendUpstream(upstreamRequest);
  }

  protected void updateFeatureDeltaStateIfMatchesViolations(
      final @NotNull XyzFeature feature, final @Nullable List<XyzFeature> violations) {
    HandlerUtil.setDeltaReviewState(feature, EReviewState.UNPUBLISHED);
    if (violations == null) {
      return;
    }
    for (final XyzFeature violation : violations) {
      final List<XyzReference> references = violation.getProperties().getReferences();
      if (references == null) {
        continue;
      }
      for (final XyzReference reference : references) {
        if (feature.getId().equals(reference.getId())) {
          HandlerUtil.setDeltaReviewState(feature, EReviewState.AUTO_REVIEW_DEFERRED);
          return;
        }
      }
    }
  }
}
