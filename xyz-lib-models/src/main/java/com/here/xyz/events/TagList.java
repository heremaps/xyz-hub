/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.events;

import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class TagList extends ArrayList<String> {

  @SuppressWarnings("unused")
  public TagList() {
    super();
  }

  @SuppressWarnings("WeakerAccess")
  public TagList(String[] tags) throws NullPointerException {
    super(tags.length);
    for (String tag : tags) {
      add(XyzNamespace.normalizeTag(tag));
    }
  }

  /**
   * Same as {@link #add(Object)}, just for better code readability.
   *
   * @param value The value to add.
   */
  public void addOr(@NotNull String value) {
    add(value);
  }
}