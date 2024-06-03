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
package com.here.naksha.handler.activitylog;

import static com.here.naksha.handler.activitylog.ReversePatchUtil.reversePatch;
import static com.here.naksha.handler.activitylog.ReversePatchUtil.toJsonNode;

import com.fasterxml.jackson.databind.JsonNode;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.Original;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActivityLogEnhancer {

  private static final Logger logger = LoggerFactory.getLogger(ActivityLogEnhancer.class);

  private ActivityLogEnhancer() {}

  static XyzFeature enhanceWithActivityLog(
      @NotNull XyzFeature newFeature, @Nullable XyzFeature oldFeature, @NotNull String spaceId) {
    XyzActivityLog activityLog = activityLog(newFeature, oldFeature, spaceId);
    newFeature.getProperties().setXyzActivityLog(activityLog);
    newFeature.setId(uuid(newFeature));
    return newFeature;
  }

  private static XyzActivityLog activityLog(
      @NotNull XyzFeature newFeature, @Nullable XyzFeature oldFeature, @NotNull String spaceId) {
    final XyzNamespace xyzNamespace = xyzNamespace(newFeature);
    final XyzActivityLog xyzActivityLog = new XyzActivityLog();
    xyzActivityLog.setId(newFeature.getId());
    xyzActivityLog.setOriginal(original(xyzNamespace, spaceId));
    EXyzAction action = xyzNamespace.getAction();
    if (action != null) {
      xyzActivityLog.setAction(action);
    }
    xyzActivityLog.setDiff(calculateDiff(action, newFeature, oldFeature));
    return xyzActivityLog;
  }

  private static Original original(@Nullable XyzNamespace xyzNamespace, @Nullable String spaceId) {
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

  private static @Nullable JsonNode calculateDiff(
      @Nullable EXyzAction action, @NotNull XyzFeature newFeature, @Nullable XyzFeature oldFeature) {
    if (action == null || EXyzAction.CREATE.equals(action) || EXyzAction.DELETE.equals(action)) {
      return null;
    } else if (EXyzAction.UPDATE.equals(action)) {
      if (oldFeature == null) {
        logger.warn(
            "Unable to calculate reversePatch for, missing predecessor for feature with uuid: {}, returning null",
            uuid(newFeature));
        return null;
      }
      ReversePatch reversePatch = reversePatch(oldFeature, newFeature);
      return toJsonNode(reversePatch);
    } else {
      throw new IllegalStateException("Unable to process unknown action type: " + action);
    }
  }

  private static String uuid(XyzFeature feature) {
    return xyzNamespace(feature).getUuid();
  }

  private static XyzNamespace xyzNamespace(XyzFeature feature) {
    return feature.getProperties().getXyzNamespace();
  }
}
