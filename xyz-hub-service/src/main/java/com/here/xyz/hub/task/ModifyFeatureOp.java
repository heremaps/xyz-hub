/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.models.geojson.implementation.Feature;
import io.vertx.core.json.Json;
import java.util.List;
import java.util.Map;

public class ModifyFeatureOp extends ModifyOp<Feature, Feature, Feature> {

  public ModifyFeatureOp(List<Feature> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    super(inputStates, ifNotExists, ifExists, isTransactional);
  }

  @Override
  public Feature patch(Feature headState, Feature baseState, Feature inputState) throws ModifyOpError {
    String inputUUID = getUuid(inputState);
    final Map<String, Object> base = baseState.asMap(metadataFilter);
    final Map<String, Object> input = inputState.asMap(metadataFilter);
    final Difference diff = Patcher.calculateDifferenceOfPartialUpdate(base, input, null, true);
    if (diff == null) {
      return headState;
    }

    Patcher.patch(base, diff);
    Feature mergeInput = XyzSerializable.fromMap(base, Feature.class);
    if (inputUUID != null) {
      mergeInput.getProperties().getXyzNamespace().setUuid(inputUUID);
    }
    return merge(headState, baseState, mergeInput);
  }

  @Override
  public Feature merge(Feature headState, Feature baseState, Feature inputState) throws ModifyOpError {
    // If the latest state is the state, which was updated, execute a replace
    if (baseState.equals(headState)) {
      return replace(headState, inputState);
    }

    final Map<String, Object> base = baseState.asMap(metadataFilter);
    final Map<String, Object> head = headState.asMap(metadataFilter);
    final Map<String, Object> input = inputState.asMap(metadataFilter);

    final Difference diffInput = Patcher.getDifference(base, input);
    if (diffInput == null) {
      return headState;
    }
    final Difference diffHead = Patcher.getDifference(base, head);
    try {
      final Difference mergedDiff = Patcher.mergeDifferences(diffInput, diffHead);
      Patcher.patch(base, mergedDiff);
      return XyzSerializable.fromMap(base, Feature.class);
    } catch (Exception e) {
      throw new ModifyOpError(e.getMessage());
    }
  }

  @Override
  public Feature replace(Feature headState, Feature inputState) throws ModifyOpError {
    if (getUuid(inputState) != null && !Objects.equal(getUuid(inputState), getUuid(headState))) {
      throw new ModifyOpError(
          "The feature with id " + headState.getId() + " cannot be replaced. The provided UUID doesn't match the UUID of the head state: "
              + getUuid(headState));
    }
    return XyzSerializable.fromMap(inputState.asMap(metadataFilter), Feature.class);
  }

  @Override
  public Feature create(Feature inputState) {
    return inputState;
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
  public boolean dataEquals(Feature feature1, Feature feature2) {
    if (Objects.equal(feature1, feature2)) {
      return true;
    }

    Map<String,Object> map1 = XyzSerializable.filter(Json.mapper.convertValue(feature1, Map.class), metadataFilter);
    Map<String,Object> map2 = XyzSerializable.filter(Json.mapper.convertValue(feature2, Map.class), metadataFilter);

    return Patcher.getDifference(map1, map2) == null;
  }

  public static Map<String, Object> metadataFilter;

  static {
    try {
      //noinspection unchecked
      metadataFilter = Json.mapper.readValue(
          "{\"properties\": {\"@ns:com:here:xyz\": {\"space\": true,\"createdAt\": true,\"updatedAt\": true,\"uuid\": true,\"puuid\": true,\"muuid\": true}}}",
          Map.class);
    } catch (JsonProcessingException ignored) {
    }
  }
}
