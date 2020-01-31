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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.hub.util.diff.Patcher.MergeConflictException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ModifySpaceOp extends ModifyOp<Space> {

  public ModifySpaceOp(List<Map<String,Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists,
      boolean isTransactional) {
    super(inputStates, ifNotExists, ifExists, isTransactional);
    for (Entry<Space> entry : entries) {
      entry.input = XyzSerializable.filter(entry.input, metadataFilter);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Space patch(Entry<Space> entry, Space headState, Space baseState, Map<String,Object> inputState) throws ModifyOpError, HttpException {
    Map baseClone = baseState.asMap(metadataFilter);
    Map input = XyzSerializable.filter(inputState, metadataFilter);

    final Difference difference = Patcher.calculateDifferenceOfPartialUpdate(baseClone, input, null, true);

    // Nothing was changed, return the head state
    if (difference == null) {
      return headState;
    }

    Patcher.patch(baseClone, difference);
    return merge(entry, headState, baseState, baseClone);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Space merge(Entry<Space> entry, Space headState, Space baseState, Map<String,Object> inputState) throws ModifyOpError, HttpException {
    if (headState.equals(baseState)) {
      return replace(entry, headState, inputState);
    }
    Map headClone = headState.asMap(metadataFilter);
    Map baseClone = baseState.asMap(metadataFilter);
    Map input = XyzSerializable.filter(inputState, metadataFilter);

    final Difference diffInput = Patcher.getDifference(baseClone, input);
    // Nothing was changed, return the head state
    if (diffInput == null) {
      return headState;
    }

    final Difference diffHead = Patcher.getDifference(baseClone, headClone);
    try {
      final Difference mergedDiff = Patcher.mergeDifferences(diffInput, diffHead);
      Patcher.patch(headClone, mergedDiff);
      return Json.mapper.readValue(Json.encode(headClone), Space.class);
    } catch (MergeConflictException e) {
      throw new ModifyOpError(e.getMessage());
    } catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space replace(Entry<Space> entry, Space headState, Map<String,Object> inputState) throws HttpException {
    try {
      return Json.mapper.readValue(Json.encode(inputState), Space.class);
    } catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space delete(Entry<Space> entry, Space headState, Map<String,Object> inputState) throws HttpException {
    return null;
  }

  @Override
  public Space create(Entry<Space> entry, Map<String,Object> input) throws HttpException {
    try {
      return Json.mapper.readValue(Json.encode(input), Space.class);
    } catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space transform(Entry<Space> entry, Space sourceState) {
    return sourceState;
  }

  @Override
  public boolean dataEquals(Space space1, Space space2) {
    if (Objects.equals(space1, space2)) {
      return true;
    }

    final ObjectMapper mapper = XyzSerializable.STATIC_MAPPER.get();
    Map map1 = XyzSerializable.filter(mapper.convertValue(space1, Map.class), metadataFilter);
    Map map2 = XyzSerializable.filter(mapper.convertValue(space2, Map.class), metadataFilter);
    return Patcher.getDifference(map1, map2) == null;
  }

  public static Map metadataFilter;

  static {
    try {
      metadataFilter = Json.mapper.readValue("{\"createdAt\":true, \"updatedAt\":true}", Map.class);
    } catch (JsonProcessingException ignored) {
    }
  }
}
