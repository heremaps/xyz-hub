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

import static com.here.xyz.hub.task.FeatureTask.FeatureKey.CREATED_AT;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.MUUID;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.PROPERTIES;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.PUUID;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.UPDATED_AT;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.UUID;

import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ModifyFeatureOp.FeatureEntry;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModifyFeatureOp extends ModifyOp<Feature, FeatureEntry> {

  public ModifyFeatureOp(List<Map<String, Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    super((inputStates == null) ? Collections.emptyList() : inputStates.stream().map(FeatureEntry::new).collect(Collectors.toList()),
        ifNotExists, ifExists, isTransactional);
  }

  public static class FeatureEntry extends ModifyOp.Entry<Feature> {

    public FeatureEntry(Map<String, Object> input) {
      super(input);
    }

    @Override
    public Feature fromMap(Map<String, Object> map) throws ModifyOpError, HttpException {
      return XyzSerializable.fromMap(map, Feature.class);
    }

    @Override
    public Map<String, Object> toMap(Feature record) throws ModifyOpError, HttpException {
      return filterMetadata(record.asMap());
    }

    @Override
    protected String getUuid(Map<String, Object> feature) {
      try {
        return new JsonObject(feature).getJsonObject(PROPERTIES).getJsonObject(XyzNamespace.XYZ_NAMESPACE).getString(UUID);
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    protected String getUuid(Feature input) {
      try {
        return input.getProperties().getXyzNamespace().getUuid();
      } catch (Exception e) {
        return null;
      }
    }

    public String getId(Feature record) {
      return record == null ? null : record.getId();
    }

    @Override
    public Map<String, Object> filterMetadata(Map<String, Object> map) {
      return filter(map,metadataFilter);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> metadataFilter = new JsonObject()
        .put(PROPERTIES, new JsonObject()
            .put(XyzNamespace.XYZ_NAMESPACE, new JsonObject()
                .put(CREATED_AT, true)
                .put(UPDATED_AT, true)
                .put(UUID, true)
                .put(PUUID, true)
                .put(MUUID, true))).mapTo(Map.class);
  }
}
