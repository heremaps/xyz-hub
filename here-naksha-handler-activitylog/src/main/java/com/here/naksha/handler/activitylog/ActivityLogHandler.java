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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.JsonDiff;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.Original;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The activity log compatibility handler. Can be used as pre- and post-processor. */
public class ActivityLogHandler implements IEventHandler {

  /**
   * Creates a new activity log handler.
   *
   * @param eventHandler the event-handler configuration.
   * @throws XyzErrorException If any error occurred.
   */
  public ActivityLogHandler(@NotNull Connector eventHandler) throws XyzErrorException {
    try {
      this.params = new ActivityLogHandlerParams(eventHandler.getProperties());
    } catch (Exception e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  final @NotNull ActivityLogHandlerParams params;

  protected static void toActivityLogFormat(@NotNull XyzFeature feature, @Nullable XyzFeature oldFeature) {
    final XyzActivityLog xyzActivityLog = new XyzActivityLog();
    final Original original = new Original();
    final ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNodeFeature = mapper.createObjectNode();
    JsonNode jsonNodeOldFeature = mapper.createObjectNode();
    JsonNode jsonDiff = mapper.createObjectNode();
    if (feature.getProperties() != null && feature.getProperties().getXyzNamespace() != null) {
      original.setPuuid(feature.getProperties().getXyzNamespace().getPuuid());
      original.setMuuid(feature.getProperties().getXyzNamespace().getMuuid());
      original.setUpdatedAt(feature.getProperties().getXyzNamespace().getUpdatedAt());
      original.setCreatedAt(feature.getProperties().getXyzNamespace().getCreatedAt());
      original.setSpace(feature.getProperties().getXyzNamespace().getSpace());
      xyzActivityLog.setAction(feature.getProperties().getXyzNamespace().getAction());
      feature.setId(feature.getProperties().getXyzNamespace().getUuid());
    }
    xyzActivityLog.setOriginal(original);
    xyzActivityLog.setId(feature.getId());
    if (feature.getProperties() != null) {
      feature.getProperties().setXyzActivityLog(xyzActivityLog);
    }
    if (feature.getProperties() != null && feature.getProperties().getXyzActivityLog() != null) {
      try {
        jsonNodeFeature = mapper.readTree(JsonSerializable.serialize(feature));
        jsonNodeOldFeature = mapper.readTree(JsonSerializable.serialize(oldFeature));
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      if (jsonNodeFeature != null && jsonNodeOldFeature != null) {
        jsonDiff = JsonDiff.asJson(jsonNodeFeature, jsonNodeOldFeature);
      }
      feature.getProperties().getXyzActivityLog().setDiff(jsonDiff);
    }
    // InvalidatedAt can be ignored for now.
  }

  protected static void fromActivityLogFormat(@NotNull XyzFeature activityLogFeature) {
    final XyzActivityLog xyzActivityLog = activityLogFeature.getProperties().getXyzActivityLog();
    final XyzNamespace xyzNamespace = activityLogFeature.getProperties().getXyzNamespace();
    if (xyzNamespace != null) {
      xyzNamespace.setUuid(activityLogFeature.getId());
    }
    if (xyzActivityLog != null) {
      if (xyzActivityLog.getOriginal() != null) {
        if (xyzActivityLog.getOriginal().getPuuid() == null) {
          xyzNamespace.setAction("CREATE");
        }
        if (xyzActivityLog.getOriginal().getPuuid() != null) {
          xyzNamespace.setAction("UPDATE");
        }
      }
      if (xyzActivityLog.getAction() == "DELETE" && xyzNamespace != null) {
        xyzNamespace.setAction("DELETE");
      }
      activityLogFeature.setId(xyzActivityLog.getId());
      if (xyzNamespace != null && xyzActivityLog.getOriginal() != null) {
        xyzNamespace.setMuuid(xyzActivityLog.getOriginal().getMuuid());
        xyzNamespace.setPuuid(xyzActivityLog.getOriginal().getPuuid());
        xyzNamespace.setSpace(xyzActivityLog.getOriginal().getSpace());
        xyzNamespace.setUpdatedAt(xyzActivityLog.getOriginal().getUpdatedAt());
        xyzNamespace.setCreatedAt(xyzActivityLog.getOriginal().getCreatedAt());
        // InvalidatedAt can be ignored for now.
      }
      activityLogFeature.getProperties().removeActivityLog();
    }
  }

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.getEvent();
    final Map<@NotNull String, Object> spaceParams = event.getParams();
    // TODO: Maybe allow overriding of some parameters per space?
    // TODO: If necessary, perform pre-processing.
    // event.addOldFeatures() <-- we need to see
    XyzResponse response = eventContext.sendUpstream(event);
    if (response instanceof XyzFeatureCollection collection) {
      // TODO: Post-process the features so that they match the new stuff.
      final List<XyzFeature> oldFeatures = collection.getOldFeatures();
      final List<@NotNull XyzFeature> readFeatures = collection.getFeatures();
      for (int i = 0; i < readFeatures.size(); i++) {
        // TODO: Improve
        final XyzFeature feature = readFeatures.get(i);
        final XyzFeature oldFeature = oldFeatures.get(i);
        toActivityLogFormat(feature, oldFeature);
      }
    }
    return response;
  }
}
