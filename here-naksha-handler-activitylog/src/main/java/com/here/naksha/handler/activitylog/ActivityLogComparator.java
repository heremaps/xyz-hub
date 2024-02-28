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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.Comparator;

public class ActivityLogComparator implements Comparator<XyzFeature> {

  @Override
  public int compare(XyzFeature featureA, XyzFeature featureB) {
    int updatedAtComparison = Long.compare(updatedAt(featureA), updatedAt(featureB));
    if (updatedAtComparison == 0) {
      return uuid(featureA).compareTo(uuid(featureB)) * -1;
    }
    return updatedAtComparison * -1;
  }

  private static String uuid(XyzFeature feature) {
    return xyzNamespace(feature).getUuid();
  }

  private static long updatedAt(XyzFeature feature) {
    return xyzNamespace(feature).getUpdatedAt();
  }

  private static XyzNamespace xyzNamespace(XyzFeature feature) {
    return feature.getProperties().getXyzNamespace();
  }
}
