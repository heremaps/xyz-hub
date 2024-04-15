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
package com.here.naksha.app.common;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class FeatureUtil {

  private FeatureUtil() {}

  public static void generateBigFeature(final @NotNull XyzFeature feature,
                                        final long targetBodySize) {
    long crtSize = 0;
    do {
      final String randomFieldName = UUID.randomUUID().toString();
      final String randomFieldValue = UUID.randomUUID().toString();
      feature.getProperties().put(randomFieldName, randomFieldValue);
      crtSize += randomFieldName.length() + randomFieldValue.length();
    } while (crtSize < targetBodySize);
  }

}
