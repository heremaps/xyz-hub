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

package com.here.xyz.hub.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.task.ModifySpaceOp.SpaceEntry;
import com.here.xyz.hub.task.SpaceTask.ConnectorMapping;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.FeatureModificationList.ConflictResolution;
import com.here.xyz.models.hub.FeatureModificationList.IfExists;
import com.here.xyz.models.hub.FeatureModificationList.IfNotExists;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModifySpaceOp extends ModifyOp<Space, SpaceEntry> {

  ConnectorMapping connectorMapping;
  boolean dryRun;
  boolean forceStorage;

  public ModifySpaceOp(List<Map<String, Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional, boolean dryRun) {
    this(inputStates, ifNotExists, ifExists, isTransactional, null, dryRun);
  }

  public ModifySpaceOp(List<Map<String, Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional, boolean dryRun, boolean forceStorage) {
    this(inputStates, ifNotExists, ifExists, isTransactional, null, dryRun, forceStorage);
  }

  public ModifySpaceOp(List<Map<String, Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional, ConnectorMapping connectorMapping, boolean dryRun) {
    this(inputStates, ifNotExists, ifExists, isTransactional, connectorMapping, dryRun, false);
  }

  public ModifySpaceOp(List<Map<String, Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional, ConnectorMapping connectorMapping, boolean dryRun, boolean forceStorage) {
    super((inputStates == null) ? Collections.emptyList() : inputStates.stream().map(input -> new SpaceEntry(input, ifNotExists, ifExists))
        .collect(Collectors.toList()), isTransactional);
    this.connectorMapping = connectorMapping;
    this.dryRun = dryRun;
    this.forceStorage = forceStorage;
  }


  public static class SpaceEntry extends ModifyOp.Entry<Space> {

    public SpaceEntry(Map<String, Object> input, IfNotExists ifNotExists, IfExists ifExists) {
      super(input, ifNotExists, ifExists, ConflictResolution.ERROR);
    }

    @Override
    protected String getId(Space record) {
      return record == null ? null : record.getId();
    }

    @Override
    protected long getVersion(Map<String, Object> record) {
      return -1;
    }

    @Override
    protected long getVersion(Space record) {
      return -1;
    }

    @Override
    public Map<String, Object> filterMetadata(Map<String, Object> map) {
      return filter(map, metadataFilter);
    }

    @Override
    public Space fromMap(Map<String, Object> map) throws ModifyOpError, HttpException {
      try {
        return DatabindCodec.mapper().readValue(Json.encode(map), Space.class);
      } catch (JsonProcessingException e) {
        throw new HttpException(BAD_REQUEST, "Invalid space definition: " + e.getMessage(), e);
      }
    }

    @Override
    public Map<String, Object> toMap(Space record) throws ModifyOpError, HttpException {
      return filter(record.asMap(), metadataFilter);
    }

    public String getId(Feature record) {
      return record == null ? null : record.getId();
    }
  }


  public static Map<String, Object> metadataFilter = new JsonObject()
      .put("createdAt", true)
      .put("updatedAt", true).getMap();
}
