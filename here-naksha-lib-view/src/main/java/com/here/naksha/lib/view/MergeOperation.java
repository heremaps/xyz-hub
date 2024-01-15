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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.models.storage.FeatureCodec;
import java.util.List;

public interface MergeOperation<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> {

  /**
   * This operation is used to combine results from multiple layers into one.
   * The input is one single feature (correlated by ID) returned by multiple layers:
   * {@code
   * [Layer_0:Feature1, Layer_1:Feature1, Layer_2:Feature1, ...]
   * }
   * What's important - the input have only layers where feature was found. <br>
   *
   * It's up to implementation how feature will be reduced to 1. It might be a feature from one specific layer
   * or merged feature from few.
   *
   * @param multipleResults - has to have at least one element
   * @return
   */
  // TODO should we know from which storage result comes from? If yes then we should return SingleStorageRow instead.
  CODEC apply(List<ViewLayerRow<FEATURE, CODEC>> multipleResults);
}
