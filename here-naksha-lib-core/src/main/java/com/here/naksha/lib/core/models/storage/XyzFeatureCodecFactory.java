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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.NotNull;

/**
 * The default codec factory for {@link XyzFeature}'s.
 */
public class XyzFeatureCodecFactory extends XyzCodecFactory<XyzFeature, XyzFeatureCodec> {

  XyzFeatureCodecFactory() {}

  /**
   * Returns the standard XYZ feature codec factory.
   *
   * @return the standard XYZ feature codec factory.
   */
  public static @NotNull XyzFeatureCodecFactory get() {
    return XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class);
  }

  @Override
  public @NotNull XyzFeatureCodec newInstance() {
    return new XyzFeatureCodec();
  }
}
