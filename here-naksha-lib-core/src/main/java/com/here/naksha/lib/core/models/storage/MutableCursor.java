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

import org.jetbrains.annotations.NotNull;

/**
 * A seekable cursor that allows the modification of the features.
 *
 * @param <FEATURE> The feature type that the cursor returns.
 * @param <CODEC> The codec type.
 */
public abstract class MutableCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    extends SeekableCursor<FEATURE, CODEC> {

  /**
   * Creates a new seekable cursor.
   *
   * @param codecFactory The codec factory to use.
   */
  protected MutableCursor(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {
    super(codecFactory);
  }

  /**
   * Replace the feature with the given one.
   * @param feature The new feature.
   * @return the old feature.
   */
  public abstract @NotNull FEATURE setFeature(@NotNull FEATURE feature);

  /**
   * Removes the feature at the current cursor position and returns it.
   * @return The removed feature.
   */
  public abstract @NotNull FEATURE removeFeature();

  // TODO: spliceFeatures, addFeature, insertFeature, pushFeature, popFeature, shiftFeature, ...?
}