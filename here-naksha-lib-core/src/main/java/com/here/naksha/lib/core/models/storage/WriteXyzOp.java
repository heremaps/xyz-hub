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

import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.NotNull;

/**
 * Helper that automatically split an {@link XyzFeature} into the parts of the write-operation, like feature and geometry. Beware, that the
 * geometry will be removed from the given feature.
 *
 * @param <T> The concrete type to write.
 */
public class WriteXyzOp<T extends XyzFeature> extends WriteOp<T> {

  /**
   * Automatically split an {@link XyzFeature} into its parts and bind the operation. Beware that the feature will be modified.
   *
   * @param op      The operation to perform.
   * @param feature The feature to use.
   */
  public WriteXyzOp(@NotNull EWriteOp op, @NotNull T feature) {
    this(op, feature, false);
  }

  /**
   * Automatically split an {@link XyzFeature} into its parts and bind the operation. Beware that the feature will be modified.
   *
   * @param op        The operation to perform.
   * @param feature   The feature to use.
   * @param minResult {@code true} if a minimal result should be returned, that means the feature and geometry is not returned.
   */
  public WriteXyzOp(@NotNull EWriteOp op, @NotNull T feature, boolean minResult) {
    super(
        op,
        feature.getId(),
        feature.xyz().getUuid(),
        feature,
        JTSHelper.toGeometry(feature.removeGeometry()),
        minResult);
  }
}
