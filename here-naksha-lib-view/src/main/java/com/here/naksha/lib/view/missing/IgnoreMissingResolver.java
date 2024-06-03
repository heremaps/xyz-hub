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
package com.here.naksha.lib.view.missing;

import com.here.naksha.lib.core.models.storage.FeatureCodec;
import com.here.naksha.lib.view.MissingIdResolver;
import com.here.naksha.lib.view.ViewLayer;
import com.here.naksha.lib.view.ViewLayerRow;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IgnoreMissingResolver<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    implements MissingIdResolver<FEATURE, CODEC> {

  @Override
  public boolean skip() {
    return true;
  }

  @Override
  public @Nullable List<Pair<ViewLayer, String>> layersToSearch(
      @NotNull List<ViewLayerRow<FEATURE, CODEC>> multipleResults) {
    return null;
  }
}
