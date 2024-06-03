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

import static java.lang.System.arraycopy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.util.diff.Difference;
import com.here.naksha.lib.core.util.diff.InsertOp;
import com.here.naksha.lib.core.util.diff.ListDiff;
import com.here.naksha.lib.core.util.diff.MapDiff;
import com.here.naksha.lib.core.util.diff.Patcher;
import com.here.naksha.lib.core.util.diff.PrimitiveDiff;
import com.here.naksha.lib.core.util.diff.RemoveOp;
import com.here.naksha.lib.core.util.diff.UpdateOp;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

class ReversePatchUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String ROOT_PATH = "";

  private static final String PATCH_PATH_DELIMITER = "/";

  private static final String ID_PATH = patchPath(ROOT_PATH, XyzFeature.ID);

  private static final String XYZ_NAMESPACE_PATH =
      patchPath(ROOT_PATH, XyzFeature.PROPERTIES, XyzProperties.XYZ_NAMESPACE);
  private static final String XYZ_NAMESPACE_TAGS_PATH = patchPath(prependRoot(PRef.TAGS_PROP_PATH));

  private ReversePatchUtil() {}

  static JsonNode toJsonNode(ReversePatch activityLogReversePatch) {
    return MAPPER.valueToTree(activityLogReversePatch);
  }

  static @Nullable ReversePatch reversePatch(XyzFeature older, XyzFeature younger) {
    Difference difference = Patcher.getDifference(older, younger);
    if (difference == null) {
      return null;
    } else {
      return fromDifference(difference);
    }
  }

  private static @Nullable ReversePatch fromDifference(Difference difference) {
    if (!(difference instanceof MapDiff rootDiff)) {
      throw new IllegalArgumentException("Expected root Difference to be MapDiff, got "
          + difference.getClass().getName() + " instead");
    }
    ReversePatch.Builder diffBuilder = ReversePatch.builder();
    handleMap(rootDiff, diffBuilder, ROOT_PATH);
    ReversePatch result = diffBuilder.build();
    if (result.equals(ReversePatch.EMPTY)) {
      return null;
    }
    return result;
  }

  private static void handle(Difference difference, ReversePatch.Builder builder, String currentPath) {
    if (difference instanceof PrimitiveDiff pd) {
      handlePrimitive(pd, builder, currentPath);
    } else if (difference instanceof MapDiff md) {
      handleMap(md, builder, currentPath);
    } else if (difference instanceof ListDiff ld) {
      handleList(ld, builder, currentPath);
    } else {
      throw new UnsupportedOperationException("Unable to  process unknown Difference type: "
          + difference.getClass().getName());
    }
  }

  private static void handleMap(MapDiff mapDiff, ReversePatch.Builder builder, String currentPath) {
    for (Map.Entry<Object, Difference> diffEntry : mapDiff.entrySet()) {
      handle(diffEntry.getValue(), builder, diffPatchPath(currentPath, diffEntry.getKey()));
    }
  }

  private static void handleList(ListDiff listDiff, ReversePatch.Builder builder, String currentPath) {
    for (int i = 0; i < listDiff.size(); i++) {
      Difference diff = listDiff.get(i);
      if (diff != null) {
        handle(diff, builder, diffPatchPath(currentPath, i));
      }
    }
  }

  private static void handlePrimitive(PrimitiveDiff primitiveDiff, ReversePatch.Builder builder, String currentPath) {
    if (shouldFilter(currentPath)) {
      return;
    }
    if (primitiveDiff instanceof InsertOp) {
      builder.reverseInsert(currentPath);
    } else if (primitiveDiff instanceof RemoveOp removeOp) {
      builder.reverseRemove(removeOp, currentPath);
    } else if (primitiveDiff instanceof UpdateOp updateOp) {
      builder.reverseUpdate(updateOp, currentPath);
    } else {
      throw new UnsupportedOperationException("Unable to process unknown PrimitiveDifference type: "
          + primitiveDiff.getClass().getName());
    }
  }

  private static boolean shouldFilter(String currentPath) {
    return isId(currentPath) || isXyzNamespaceButNotTag(currentPath);
  }

  private static boolean isId(String currentPath) {
    return ID_PATH.equals(currentPath);
  }

  private static boolean isXyzNamespaceButNotTag(String currentPath) {
    return !currentPath.startsWith(XYZ_NAMESPACE_TAGS_PATH) && currentPath.startsWith(XYZ_NAMESPACE_PATH);
  }

  private static String diffPatchPath(String currentPath, Object diffPlacement) {
    return patchPath(currentPath, diffPlacement.toString());
  }

  private static String patchPath(String... components) {
    return ROOT_PATH + String.join(PATCH_PATH_DELIMITER, components);
  }

  private static String[] prependRoot(String... components) {
    String[] res = new String[components.length + 1];
    res[0] = ROOT_PATH;
    arraycopy(components, 0, res, 1, components.length);
    return res;
  }
}
