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

package com.here.xyz.jobs.datasets;

import java.util.List;

/**
 * A {@link DatasetDescription} which is a logical representation of multiple Sub-DatasetDescriptions being combined.
 * @param <T> The type of the Sub-DatasetDescriptions
 */
public interface CombinedDatasetDescription<T> {

  /**
   * Creates the Sub-DatasetDescriptions this CombinedDatasetDescription is representing.
   * @return The list of Sub-DatasetDescription instances
   */
  List<T> createChildEntities();
}
