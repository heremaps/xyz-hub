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
package com.here.naksha.handler.activitylog;

import static com.here.naksha.handler.activitylog.ReversePatchUtil.reversePatch;
import static com.here.naksha.handler.activitylog.ReversePatchUtil.toJsonNode;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.NOT_IMPLEMENTED;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SUCCEED_WITHOUT_PROCESSING;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.Original;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.handlers.AbstractEventHandler;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActivityLogHandler extends AbstractEventHandler {

  private static final Comparator<XyzFeature> FEATURE_COMPARATOR = new ActivityLogComparator();

  private final @NotNull Logger logger = LoggerFactory.getLogger(ActivityLogHandler.class);
  private final @NotNull ActivityLogHandlerProperties properties;

  // TODO: remove unused 'eventTarget' property as part of MCPODS-7103
  public ActivityLogHandler(
      @NotNull EventHandler handlerConfig, @NotNull INaksha hub, @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.properties = JsonSerializable.convert(handlerConfig.getProperties(), ActivityLogHandlerProperties.class);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ReadFeatures) {
      return PROCESS;
    }
    if (request instanceof WriteCollections<?, ?, ?>) {
      return SUCCEED_WITHOUT_PROCESSING;
    }
    return NOT_IMPLEMENTED;
  }

  @Override
  protected @NotNull Result process(@NotNull IEvent event) {
    final ErrorResult validationError = propertiesValidationError();
    if (validationError != null) {
      return validationError;
    }
    final NakshaContext ctx = NakshaContext.currentContext();
    final ReadFeatures request = transformRequest(event.getRequest());
    List<XyzFeature> activityLogFeatures = activityLogFeatures(request, ctx);
    return ActivityLogSuccessResult.forFeatures(activityLogFeatures);
  }

  private @NotNull ReadFeatures transformRequest(Request<?> request) {
    final ReadFeatures readFeatures = (ReadFeatures) request;
    readFeatures.withReturnAllVersions(true);
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);
    readFeatures.setCollections(List.of(properties.getSpaceId()));
    return readFeatures;
  }

  private @Nullable ErrorResult propertiesValidationError() {
    if (nullOrEmpty(properties.getSpaceId())) {
      return new ErrorResult(
          XyzError.ILLEGAL_ARGUMENT, "Missing required property: " + ActivityLogHandlerProperties.SPACE_ID);
    }
    return null;
  }

  private List<XyzFeature> activityLogFeatures(ReadFeatures readFeatures, NakshaContext context) {
    List<XyzFeature> historyFeatures = fetchHistoryFeatures(readFeatures, context);
    return featuresEnhancedWithActivity(historyFeatures, context);
  }

  private List<XyzFeature> fetchHistoryFeatures(ReadFeatures readFeatures, NakshaContext context) {
    try (IReadSession readSession = nakshaHub().getSpaceStorage().newReadSession(context, true)) {
      try (Result result = readSession.execute(readFeatures)) {
        return readFeaturesFromResult(result, XyzFeature.class);
      }
    } catch (NoCursor | NoSuchElementException e) {
      return Collections.emptyList();
    }
  }

  private List<XyzFeature> featuresEnhancedWithActivity(List<XyzFeature> historyFeatures, NakshaContext context) {
    List<FeatureWithPredecessor> featuresWithPredecessors = featuresWithPredecessors(historyFeatures, context);
    return featuresWithPredecessors.stream()
        .map(this::featureEnhancedWithActivity)
        .sorted(FEATURE_COMPARATOR)
        .toList();
  }

  private List<FeatureWithPredecessor> featuresWithPredecessors(
      List<XyzFeature> historyFeatures, NakshaContext context) {
    List<XyzFeature> allNecessaryFeatures = collectAllNecessaryFeatures(historyFeatures, context);
    Map<String, XyzFeature> allFeaturesByUuid = featuresByUuid(allNecessaryFeatures);
    return historyFeatures.stream()
        .map(feature -> new FeatureWithPredecessor(feature, allFeaturesByUuid.get(puuid(feature))))
        .toList();
  }

  private List<XyzFeature> collectAllNecessaryFeatures(List<XyzFeature> historyFeatures, NakshaContext context) {
    List<XyzFeature> missingPredecessors = fetchMissingPredecessors(missingPuuids(historyFeatures), context);
    return combine(historyFeatures, missingPredecessors);
  }

  private List<XyzFeature> combine(List<XyzFeature> historyFeatures, List<XyzFeature> missingPredecessors) {
    if (missingPredecessors.isEmpty()) {
      return historyFeatures;
    }
    return Stream.concat(historyFeatures.stream(), missingPredecessors.stream())
        .toList();
  }

  private Set<String> missingPuuids(List<XyzFeature> historyFeatures) {
    Set<String> requiredPredecessorsUuids = new HashSet<>();
    Set<String> fetchedUuids = new HashSet<>();
    historyFeatures.forEach(historyFeature -> {
      fetchedUuids.add(uuid(historyFeature));
      String puuid = puuid(historyFeature);
      if (puuid != null) {
        requiredPredecessorsUuids.add(puuid);
      }
    });
    requiredPredecessorsUuids.removeAll(fetchedUuids);
    return requiredPredecessorsUuids;
  }

  private List<XyzFeature> fetchMissingPredecessors(Set<String> missingUuids, NakshaContext context) {
    if (missingUuids.isEmpty()) {
      return Collections.emptyList();
    }
    return fetchHistoryFeatures(missingPredecessorsRequest(missingUuids), context);
  }

  private ReadFeatures missingPredecessorsRequest(Set<String> missingUuids) {
    POp[] matchUuids = missingUuids.stream()
        .map(missingUuid -> POp.eq(PRef.uuid(), missingUuid))
        .toArray(POp[]::new);
    return new ReadFeatures(properties.getSpaceId())
        .withReturnAllVersions(true)
        .withPropertyOp(POp.or(matchUuids));
  }

  @NotNull
  private static Map<String, XyzFeature> featuresByUuid(List<XyzFeature> historyFeatures) {
    return historyFeatures.stream().collect(toMap(ActivityLogHandler::uuid, identity()));
  }

  private XyzFeature featureEnhancedWithActivity(@NotNull FeatureWithPredecessor featureWithPredecessor) {
    XyzActivityLog activityLog = activityLog(featureWithPredecessor);
    XyzFeature feature = featureWithPredecessor.feature;
    feature.getProperties().setXyzActivityLog(activityLog);
    feature.setId(uuid(feature));
    return feature;
  }

  private XyzActivityLog activityLog(@NotNull FeatureWithPredecessor featureWithPredecessor) {
    final XyzNamespace xyzNamespace =
        featureWithPredecessor.feature.getProperties().getXyzNamespace();
    final XyzActivityLog xyzActivityLog = new XyzActivityLog();
    xyzActivityLog.setId(featureWithPredecessor.feature.getId());
    xyzActivityLog.setOriginal(original(xyzNamespace, properties.getSpaceId()));
    EXyzAction action = xyzNamespace.getAction();
    if (action != null) {
      xyzActivityLog.setAction(action);
    }
    xyzActivityLog.setDiff(calculateDiff(action, featureWithPredecessor));
    return xyzActivityLog;
  }

  private @Nullable JsonNode calculateDiff(
      @Nullable EXyzAction action, @NotNull FeatureWithPredecessor featureWithPredecessor) {
    if (action == null || EXyzAction.CREATE.equals(action) || EXyzAction.DELETE.equals(action)) {
      return null;
    } else if (EXyzAction.UPDATE.equals(action)) {
      if (featureWithPredecessor.oldFeature == null) {
        logger.warn(
            "Unable to calculate reversePatch for, missing predecessor for feature with uuid: {}, returning null",
            uuid(featureWithPredecessor.feature));
        return null;
      }
      ReversePatch reversePatch = reversePatch(featureWithPredecessor.oldFeature, featureWithPredecessor.feature);
      return toJsonNode(reversePatch);
    } else {
      throw new IllegalStateException("Unable to process unknown action type: " + action);
    }
  }

  private Original original(@Nullable XyzNamespace xyzNamespace, @Nullable String spaceId) {
    Original original = new Original();
    if (xyzNamespace != null) {
      original.setPuuid(xyzNamespace.getPuuid());
      original.setUpdatedAt(xyzNamespace.getUpdatedAt());
      original.setCreatedAt(xyzNamespace.getCreatedAt());
    }
    if (spaceId != null) {
      original.setSpace(spaceId);
    }
    return original;
  }

  private static boolean nullOrEmpty(String value) {
    return value == null || value.isBlank();
  }

  private static String uuid(XyzFeature feature) {
    return xyzNamespace(feature).getUuid();
  }

  private static String puuid(XyzFeature feature) {
    return xyzNamespace(feature).getPuuid();
  }

  private static XyzNamespace xyzNamespace(XyzFeature feature) {
    return feature.getProperties().getXyzNamespace();
  }

  private record FeatureWithPredecessor(@NotNull XyzFeature feature, @Nullable XyzFeature oldFeature) {}
}
