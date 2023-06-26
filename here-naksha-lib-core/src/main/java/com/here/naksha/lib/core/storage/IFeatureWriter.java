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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import org.jetbrains.annotations.NotNull;

/**
 * Interface to grant write-access to features in a collection.
 *
 * @param <FEATURE> the feature-type to modify.
 */
public interface IFeatureWriter<FEATURE extends Feature> extends IFeatureReader<FEATURE> {

  /**
   * Perform the given operations as bulk operation and return the results.
   *
   * @param req the modification request.
   * @return the modification result with the features that have been inserted, update and deleted.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull
  ModifyFeaturesResp<FEATURE> modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req) throws Exception;
}
