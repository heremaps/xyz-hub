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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.JsonEnum;

/** The actions that are supported by Naksha. */
public class EXyzAction extends JsonEnum {

  /**
   * The feature has just been created, the {@link XyzNamespace#getVersion() version} will be {@code
   * 1}.
   */
  public static final EXyzAction CREATE = def(EXyzAction.class, "CREATE");

  /**
   * The feature has been updated, the {@link XyzNamespace#getVersion() version} will be greater
   * than {@code 1}.
   */
  public static final EXyzAction UPDATE = def(EXyzAction.class, "UPDATE");

  /**
   * The feature has been deleted, the {@link XyzNamespace#getVersion() version} will be greater
   * than {@code 1}. No other state with a higher version should be possible.
   */
  public static final EXyzAction DELETE = def(EXyzAction.class, "DELETE");

  @Override
  protected void init() {
    register(EXyzAction.class);
  }
}
