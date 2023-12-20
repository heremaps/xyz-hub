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

import static com.here.naksha.lib.core.models.storage.XyzCodecFactory.getFactory;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.List;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Helper to simplify creation of ContextWriteFeatures request, using standard {@link XyzFeature}
 * type for features, context and violations.
 */
@ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
public class ContextWriteXyzFeatures
    extends ContextWriteFeatures<XyzFeature, XyzFeature, XyzFeature, XyzFeatureCodec, ContextWriteXyzFeatures> {

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public ContextWriteXyzFeatures(@NotNull String collectionId) {
    super(getFactory(XyzFeatureCodecFactory.class), collectionId);
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public ContextWriteXyzFeatures(
      @NotNull String collectionId, final @NotNull List<@NotNull XyzFeatureCodec> features) {
    super(getFactory(XyzFeatureCodecFactory.class), collectionId, features);
  }
}
