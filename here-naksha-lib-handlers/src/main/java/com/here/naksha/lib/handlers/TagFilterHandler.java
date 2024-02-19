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
package com.here.naksha.lib.handlers;

import static com.here.naksha.lib.core.util.CollectionUtils.isNotNullOrEmpty;
import static com.here.naksha.lib.core.util.CollectionUtils.isNullOrEmpty;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SEND_UPSTREAM_WITHOUT_PROCESSING;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagFilterHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(TagFilterHandler.class);

  private final @NotNull TagFilterHandlerProperties properties;

  public TagFilterHandler(
      final @NotNull EventHandler eventHandler,
      final @NotNull INaksha hub,
      final @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.properties = JsonSerializable.convert(eventHandler.getProperties(), TagFilterHandlerProperties.class);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ReadFeatures || request instanceof WriteFeatures) {
      return PROCESS;
    }
    return SEND_UPSTREAM_WITHOUT_PROCESSING;
  }

  @Override
  public @NotNull Result process(@NotNull IEvent event) {
    final Request<?> request = event.getRequest();
    logger.info("Handler received request {}", request.getClass().getSimpleName());

    // Forward the request without changing it, if no tag configuration specified
    if (isNullOrEmpty(properties.getAdd())
        && isNullOrEmpty(properties.getRemoveWithPrefixes())
        && isNullOrEmpty(properties.getContains())) {
      logger.warn("No tag filtering applicable, forwarding request as is.");
      return event.sendUpstream();
    }

    if (request instanceof ReadFeatures readRequest) {
      applyFilterConditionOnRequest(readRequest, properties.getContains());
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      applyTagChangesOnRequest(wf, properties.getAdd(), properties.getRemoveWithPrefixes());
    }

    return event.sendUpstream(request);
  }

  public static void applyFilterConditionOnRequest(
      final @NotNull ReadFeatures readRequest, final @Nullable List<String> tagValues) {
    if (isNullOrEmpty(tagValues)) {
      return;
    }
    final POp origPOp = readRequest.getPropertyOp();
    final POp tagFilterOp = buildFilterOperationForTags(tagValues);
    final POp newOp = (origPOp == null) ? tagFilterOp : POp.and(origPOp, tagFilterOp);
    readRequest.setPropertyOp(newOp);
  }

  private static POp buildFilterOperationForTags(final @NotNull List<String> tagValues) {
    // Do we have only one tag? Then use EXISTS operation
    if (tagValues.size() == 1) {
      return POp.exists(PRef.tag(tagValues.get(0)));
    }
    // we have multiple tags, so use AND operation
    final POp[] ops = new POp[tagValues.size()];
    int idx = 0;
    for (final @NotNull String value : tagValues) {
      ops[idx++] = POp.exists(PRef.tag(value));
    }
    return POp.and(ops);
  }

  public static void applyTagChangesOnRequest(
      final @NotNull WriteFeatures<?, ?, ?> wf,
      final @Nullable List<String> addTags,
      final @Nullable List<String> removeTags) {
    if (isNullOrEmpty(addTags) && isNullOrEmpty(removeTags)) return;
    List<XyzFeatureCodec> codecList = null;
    if (wf instanceof WriteXyzFeatures wxf) {
      codecList = wxf.features;
    } else if (wf instanceof ContextWriteXyzFeatures cwxf) {
      codecList = cwxf.features;
    }
    if (isNotNullOrEmpty(codecList)) {
      for (final @NotNull XyzFeatureCodec codec : codecList) {
        applyTagChangesOnFeature(codec.getFeature(), addTags, removeTags);
      }
    }
  }

  private static void applyTagChangesOnFeature(
      final @Nullable XyzFeature feature,
      final @Nullable List<String> addTags,
      final @Nullable List<String> removeTags) {
    if (feature == null) return;
    final XyzNamespace xyzNS = feature.getProperties().getXyzNamespace();
    // NOTE - we need to remove existing tags first, before adding new ones
    if (isNotNullOrEmpty(removeTags)) xyzNS.removeTagsWithPrefixes(removeTags);
    if (isNotNullOrEmpty(addTags)) xyzNS.addTags(addTags, true);
  }
}
