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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.here.naksha.lib.core.util.json.JsonEnum;

/**
 * The change-state enumeration.
 */
public class EChangeState extends JsonEnum {

  /**
   * The feature was created (did not exist in base layer).
   */
  public static final EChangeState CREATED = defIgnoreCase(EChangeState.class, "CREATED");

  /**
   * The feature was updated (did exist in base layer).
   */
  public static final EChangeState UPDATED = defIgnoreCase(EChangeState.class, "UPDATED");

  /**
   * The feature was removed from the map.
   */
  public static final EChangeState REMOVED = defIgnoreCase(EChangeState.class, "REMOVED");

  /**
   * The feature was a road or topology and split, which means, it was deleted, but replaced with new child nodes that should be in
   * {@code CREATED} state.
   */
  public static final EChangeState SPLIT = defIgnoreCase(EChangeState.class, "SPLIT");

  @Override
  protected void init() {
    register(EChangeState.class);
  }
}
