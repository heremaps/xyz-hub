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

public class ModifySpaceOp extends ModifyOp<JsonObject, Space, Space> {

  public ModifySpaceOp(List<JsonObject> inputStates, IfNotExists ifNotExists, IfExists ifExists,
      boolean isTransactional) {
    super(inputStates, ifNotExists, ifExists, isTransactional);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Space patch(Space headState, Space baseState, JsonObject inputState) throws ModifyOpError, HttpException {
    Map baseClone = Json.mapper.convertValue(baseState, Map.class);
    Map input = inputState.getMap();
    final Difference difference = Patcher.calculateDifferenceOfPartialUpdate(baseClone, input, null, true);
    Patcher.patch(baseClone, difference);
    return merge(headState, baseState, new JsonObject(baseClone));
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Space merge(Space headState, Space baseState, JsonObject inputState) throws ModifyOpError, HttpException {
    Map headClone = Json.mapper.convertValue(headState, Map.class);
    Map baseClone = Json.mapper.convertValue(baseState, Map.class);
    Map input = inputState.getMap();

    final Difference diffInput = Patcher.getDifference(baseClone, input);
    final Difference diffHead = Patcher.getDifference(baseClone, headClone);
    try {
      final Difference mergedDiff = Patcher.mergeDifferences(diffInput, diffHead);
      Patcher.patch(headClone, mergedDiff);
      return Json.mapper.readValue(Json.encode(headClone), Space.class);
    } catch (MergeConflictException e) {
      throw new ModifyOpError(e.getMessage());
    } catch( JsonProcessingException e ){
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space replace(Space headState, JsonObject inputState) throws ModifyOpError, HttpException {
    try {
      return Json.mapper.readValue(Json.encode(inputState), Space.class);
    }
    catch( JsonProcessingException e ){
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space create(JsonObject input) throws ModifyOpError, HttpException {
    try {
      return Json.mapper.readValue(Json.encode(input), Space.class);
    }
    catch( JsonProcessingException e ){
      throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
    }
  }

  @Override
  public Space transform(Space sourceState) throws ModifyOpError {
    return Json.decodeValue(Json.encode(sourceState), Space.class);
  }

  @Override
  public boolean equalStates(Space state1, Space state2) {
    if (Objects.equals(state1, state2)) {
      return true;
    }

    final ObjectMapper mapper = XyzSerializable.STATIC_MAPPER.get();
    Difference diff = Patcher.getDifference(mapper.convertValue(state1, Map.class), mapper.convertValue(state2, Map.class));
    return diff == null;
  }
}
