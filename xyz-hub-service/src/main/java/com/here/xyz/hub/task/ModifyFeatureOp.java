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

package com.here.xyz.hub.task;

import com.google.common.base.Objects;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.models.geojson.implementation.Feature;
import io.vertx.core.json.Json;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ModifyFeatureOp extends ModifyOp<Feature, Feature, Feature> {

  public ModifyFeatureOp(List<Feature> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    super(inputStates, ifNotExists, ifExists, isTransactional);
  }

  @Override
  public Feature patch(Feature headState, Feature editedState, Feature inputState) throws ModifyOpError {
    final Map<String, Object> editedStateMap = editedState.asMap();

    final Difference diff = Patcher.calculateDifferenceOfPartialUpdate(editedStateMap, inputState.asMap(), null, true);
    Patcher.patch(editedStateMap, diff);
    return merge(headState, editedState, XyzSerializable.fromMap(editedStateMap, Feature.class));
  }

  @Override
  public Feature merge(Feature headState, Feature editedState, Feature inputState) throws ModifyOpError {
    if (equalStates(editedState, headState)) {
      return replace(headState, inputState);
    }

    final Map<String, Object> editedStateMap = editedState.asMap();
    final Difference diffInput = Patcher.getDifference(editedStateMap, inputState.asMap());
    final Difference diffHead = Patcher.getDifference(editedStateMap, headState.asMap());
    try {
      final Difference mergedDiff = Patcher.mergeDifferences(diffInput, diffHead);
      Patcher.patch(editedStateMap, mergedDiff);
      return XyzSerializable.fromMap(editedStateMap, Feature.class);
    } catch (Exception e) {
      throw new ModifyOpError(e.getMessage());
    }
  }

  @Override
  public Feature replace(Feature headState, Feature inputState) throws ModifyOpError {
    if (getUuid(inputState) != null && !Objects.equal(getUuid(inputState), getUuid(headState) != null)) {
      throw new ModifyOpError(
          "The feature with id " + headState.getId() + " cannot be replaced. The provided UUID doesn't match the UUID of the head state: "+ getUuid(headState));
    }
    return inputState.copy();
  }

  @Override
  public Feature create(Feature inputState) {
    return inputState.copy();
  }

  @Override
  public Feature transform(Feature sourceState) {
    return sourceState;
  }

  private String getUuid(Feature feature) {
    if (feature == null || feature.getProperties() == null || feature.getProperties().getXyzNamespace() == null) {
      return null;
    }
    return feature.getProperties().getXyzNamespace().getUuid();
  }

  @Override
  public boolean equalStates(Feature state1, Feature state2) {
    if( Objects.equal(state1, state2) ) {
      return true;
    }

    // TODO: Move to Feature#equals()
    Difference diff = Patcher.getDifference(Json.mapper.convertValue(state1, Map.class), Json.mapper.convertValue(state2, Map.class));
    return diff == null;
  }
}
