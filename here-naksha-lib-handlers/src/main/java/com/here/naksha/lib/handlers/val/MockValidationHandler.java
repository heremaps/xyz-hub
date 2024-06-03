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
package com.here.naksha.lib.handlers.val;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SEND_UPSTREAM_WITHOUT_PROCESSING;
import static com.here.naksha.lib.handlers.util.MockUtil.parseJson;
import static com.here.naksha.lib.handlers.util.MockUtil.parseJsonFile;
import static com.here.naksha.lib.handlers.util.MockUtil.toJson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.geojson.implementation.XyzReference;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.storage.ContextWriteFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.handlers.AbstractEventHandler;
import com.here.naksha.lib.handlers.util.HandlerUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockValidationHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(MockValidationHandler.class);
  protected @NotNull EventHandler eventHandler;
  protected @NotNull EventTarget<?> eventTarget;
  protected @NotNull XyzProperties properties;

  private static final String MOCK_VIOLATIONS_FILE = "mock_data/dry_run_violations.json";
  private static final TypeReference<List<XyzFeature>> LIST_FEATURE_TYPE_REF = new TypeReference<>() {};
  private static final List<XyzFeature> mockViolations = parseJsonFile(MOCK_VIOLATIONS_FILE, LIST_FEATURE_TYPE_REF);
  private static final int totalViolations = mockViolations.size();

  public MockValidationHandler(
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
        request, ContextWriteFeatures.class, "Unsupported request type for validation");

    final @Nullable List<XyzFeature> violations = validateFeatures(cwf, cwf.getContext());

    // create and forward request for next handler in the pipeline
    final ContextWriteFeatures<?, ?, ?, ?, ?> upstreamRequest = HandlerUtil.createContextWriteRequestFromCodecList(
        cwf.getCollectionId(), cwf.features, cwf.getContext(), violations);
    return event.sendUpstream(upstreamRequest);
  }

  protected @Nullable List<XyzFeature> validateFeatures(
      final @NotNull ContextWriteFeatures<?, ?, ?, ?, ?> cwf, final @Nullable List<?> context) {
    // For random features from the input list, create 0-to-N random violations
    final List<XyzFeature> violations;

    // Decide randomly whether to generate violations or not
    // Generation violations if odd number of features supplied, otherwise not
    final boolean generateViolation = (cwf.features.size() % 2) > 0;

    if (!generateViolation) {
      return null;
    }

    // TODO : Write validation logic.

    // Generate random violations and attach feature references
    violations = new ArrayList<>();
    int featureCnt = 0;
    final List<XyzFeature> features = HandlerUtil.getXyzFeaturesFromCodecList(cwf.features);
    for (final XyzFeature feature : features) {
      featureCnt++;
      // Distribution of "count" of violations, depends on feature "number",
      // using min condition (i.e. min (feature, violation count))
      // For example, if we have 4 features and 3 mock violations, then:
      //    feature #1, will have 1 violation i.e. min(1,3)
      //    feature #2, will have 2 violations i.e. min(2,3)
      //    feature #3, will have 3 violations i.e. min(3,3)
      //    feature #4, will have 3 violations i.e. min(4,3)
      int violationsCount = Math.min(featureCnt, totalViolations);
      final Object momType = feature.get("momType");
      violations.addAll(getNViolationsWithFeatureReference(
          violationsCount, feature, cwf.getCollectionId(), (momType == null) ? "" : momType.toString()));
    }
    return violations;
  }

  private @NotNull List<XyzFeature> getNViolationsWithFeatureReference(
      final int count,
      final @NotNull XyzFeature feature,
      final @NotNull String spaceId,
      final @Nullable String featureType) {
    final List<XyzFeature> violations = new ArrayList<>();
    for (int i = 0; i < count && i < totalViolations; i++) {
      final XyzFeature violation = parseJson(toJson(mockViolations.get(i)), XyzFeature.class);
      // randomize violation id
      violation.setId("urn:here::here:Topology:violation_" + RandomStringUtils.randomAlphabetic(12));
      // add reference to feature
      final XyzReference reference = new XyzReference(feature.getId(), spaceId, featureType);
      violation.getProperties().setReferences(List.of(reference));
      violation.put("violatedObject", reference);
      violation.setGeometry(feature.getGeometry());
      // add violation to the list
      violations.add(violation);
    }
    return violations;
  }
}
