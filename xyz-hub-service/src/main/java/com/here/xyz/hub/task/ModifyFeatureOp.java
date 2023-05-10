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

import static com.here.xyz.NakshaLogger.currentLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.task.ModifyFeatureOp.FeatureEntry;
import com.here.xyz.hub.util.diff.Patcher.ConflictResolution;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModifyFeatureOp extends ModifyOp<Feature, FeatureEntry> {

  private final static String ON_FEATURE_NOT_EXISTS = "onFeatureNotExists";
  private final static String ON_FEATURE_EXISTS = "onFeatureExists";
  private final static String ON_MERGE_CONFLICT = "onMergeConflict";

  public boolean allowFeatureCreationWithUUID;

  /**
   * @param featureModifications A list of FeatureModifications of which each may have different settings for existence-handling and/or
   *                             conflict-handling. If these settings are not specified at the FeatureModification the according other
   *                             parameters (ifNotExists, ifExists, conflictResolution) of this constructor will be applied for that
   *                             purpose.
   */
  public ModifyFeatureOp(List<Map<String, Object>> featureModifications, IfNotExists ifNotExists, IfExists ifExists,
      boolean isTransactional, ConflictResolution conflictResolution, boolean allowFeatureCreationWithUUID) {
    this(featureModifications, ifNotExists, ifExists, isTransactional, conflictResolution);
    this.allowFeatureCreationWithUUID = allowFeatureCreationWithUUID;
  }

  public ModifyFeatureOp(List<Map<String, Object>> featureModifications, IfNotExists ifNotExists, IfExists ifExists,
      boolean isTransactional, ConflictResolution conflictResolution) {
    super((featureModifications == null) ? Collections.emptyList() : featureModifications.stream().flatMap(fm -> {
      IfNotExists ne =
          fm.get(ON_FEATURE_NOT_EXISTS) instanceof String ? IfNotExists.of((String) fm.get(ON_FEATURE_NOT_EXISTS)) : ifNotExists;
      IfExists e = fm.get(ON_FEATURE_EXISTS) instanceof String ? IfExists.of((String) fm.get(ON_FEATURE_EXISTS)) : ifExists;
      ConflictResolution cr = fm.get(ON_MERGE_CONFLICT) instanceof String ?
          ConflictResolution.of((String) fm.get(ON_MERGE_CONFLICT)) : conflictResolution;

      List<String> featureIds = (List<String>) fm.get("featureIds");
      Map<String, Object> featureCollection = (Map<String, Object>) fm.get("featureData");
      List<Map<String, Object>> features = null;
      if (featureCollection != null) {
        features = (List<Map<String, Object>>) featureCollection.get("features");
      }

      if (featureIds == null && features == null) {
        return Stream.empty();
      }

      if (featureIds != null) {
        if (features == null) {
          features = idsToFeatures(featureIds);
        } else {
          features.addAll(idsToFeatures(featureIds));
        }
      }

      return features.stream().map(feature -> new FeatureEntry(feature, ne, e, cr));
    }).collect(Collectors.toList()), isTransactional);
  }

  private static List<Map<String, Object>> idsToFeatures(List<String> featureIds) {
    return featureIds.stream().map(fId -> new JsonObject().put("id", fId).getMap()).collect(Collectors.toList());
  }

  public static class FeatureEntry extends ModifyOp.Entry<Feature> {

    public int inputRevision;

    public FeatureEntry(Map<String, Object> input, IfNotExists ifNotExists, IfExists ifExists, ConflictResolution cr) {
      super(input, ifNotExists, ifExists, cr);
      this.inputRevision = getRevision(input);
    }

    @Override
    public Feature fromMap(Map<String, Object> map) throws ModifyOpError {
      try {
        return XyzSerializable.fromMap(map, Feature.class);
      } catch (Exception e) {
        currentLogger().error("Failed to deserialize feature from map", e);
        try {
          throw new ModifyOpError("Unable to deserialize feature from map: " + XyzSerializable.DEFAULT_MAPPER.get().writeValueAsString(map));
        } catch (JsonProcessingException jsonProcessingException) {
          throw new ModifyOpError("Unable to deserialize feature from map. id: " + map.get("id") + ",type: " + map.get("type"));
        }
      }
    }

    @Override
    public Map<String, Object> toMap(Feature record) throws ModifyOpError {
      return filterMetadata(record.asMap());
    }

    @Override
    protected String getUuid(Map<String, Object> feature) {
      try {
        return new JsonObject(feature).getJsonObject(Feature.PROPERTIES).getJsonObject(Properties.XYZ_NAMESPACE)
            .getString(XyzNamespace.UUID);
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
      return filter(map, metadataFilter);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> metadataFilter = new JsonObject()
        .put(Feature.PROPERTIES, new JsonObject()
            .put(Properties.XYZ_NAMESPACE, new JsonObject()
                .put(XyzNamespace.SPACE, true)
                .put(XyzNamespace.CREATED_AT, true)
                .put(XyzNamespace.UPDATED_AT, true)
                .put(XyzNamespace.UUID, true)
                .put(XyzNamespace.PUUID, true)
                .put(XyzNamespace.MUUID, true))).mapTo(Map.class);

    private int getRevision(Map<String, Object> input) {
      try {
        return new JsonObject(input).getJsonObject(Feature.PROPERTIES).getJsonObject(Properties.XYZ_NAMESPACE)
            .getInteger(XyzNamespace.REVISION, 0);
      } catch (Exception e) {
        return 0;
      }
    }
  }

  /**
   * Validates whether the feature can be created based on the space's flag allowFeatureCreationWithUUID. Creation of features using UUID in
   * the payload should always return an error, however at the moment, due to a bug, the creation succeeds.
   *
   * @param entry
   * @throws ModifyOpError
   */
  @Override
  public void validateCreate(Entry<Feature> entry) throws ModifyOpError {
    if (!allowFeatureCreationWithUUID && entry.inputUUID != null) {
      throw new ModifyOpError(
          "The feature with id " + entry.input.get("id") + " cannot be created. Property UUID should not be provided as input.");
    }
  }
}
