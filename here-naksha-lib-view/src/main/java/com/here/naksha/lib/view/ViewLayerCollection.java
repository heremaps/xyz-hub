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
package com.here.naksha.lib.view;

import java.util.Collections;
import java.util.List;

public class ViewLayerCollection {

  private final String name;
  private final List<ViewLayer> layers;

  public ViewLayerCollection(String name, List<ViewLayer> layers) {
    this.name = name;
    this.layers = Collections.unmodifiableList(layers);
  }

  public ViewLayerCollection(String name, ViewLayer... orderedLowerLevelStorages) {
    this.name = name;
    this.layers = List.of(orderedLowerLevelStorages);
  }

  public String getName() {
    return name;
  }

  public List<ViewLayer> getLayers() {
    return layers;
  }

  public int priorityOf(ViewLayer layer) {
    return layers.indexOf(layer);
  }

  public ViewLayer getTopPriorityLayer() {
    return layers.get(0);
  }
}
