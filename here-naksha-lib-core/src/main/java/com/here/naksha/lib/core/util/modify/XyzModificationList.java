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

package com.here.naksha.lib.core.util.modify;

public class XyzModificationList // extends FeatureModificationList<Feature, FeatureModificationEntry>
{
  //
  //  private final static String ON_FEATURE_NOT_EXISTS = "onFeatureNotExists";
  //  private final static String ON_FEATURE_EXISTS = "onFeatureExists";
  //  private final static String ON_MERGE_CONFLICT = "onMergeConflict";
  //
  //  public boolean allowFeatureCreationWithUUID;
  //
  //  /**
  //   * @param featureModifications A list of FeatureModifications of which each may have different
  // settings for existence-handling and/or
  //   *                             conflict-handling. If these settings are not specified at the
  // FeatureModification the according other
  //   *                             parameters (ifNotExists, ifExists, conflictResolution) of this
  // constructor will be applied for that
  //   *                             purpose.
  //   */
  //  public XyzModificationList(List<Map<String, Object>> featureModifications, IfNotExists
  // ifNotExists, IfExists ifExists,
  //      boolean isTransactional, ConflictResolution conflictResolution, boolean
  // allowFeatureCreationWithUUID) {
  //    this(featureModifications, ifNotExists, ifExists, isTransactional, conflictResolution);
  //    this.allowFeatureCreationWithUUID = allowFeatureCreationWithUUID;
  //  }
  //
  //  public XyzModificationList(List<Map<String, Object>> featureModifications, IfNotExists
  // ifNotExists, IfExists ifExists,
  //      boolean isTransactional, ConflictResolution conflictResolution) {
  //    super((featureModifications == null) ? Collections.emptyList() :
  // featureModifications.stream().flatMap(fm -> {
  //      IfNotExists ne =
  //          fm.get(ON_FEATURE_NOT_EXISTS) instanceof String ? IfNotExists.of((String)
  // fm.get(ON_FEATURE_NOT_EXISTS)) : ifNotExists;
  //      IfExists e = fm.get(ON_FEATURE_EXISTS) instanceof String ? IfExists.of((String)
  // fm.get(ON_FEATURE_EXISTS)) : ifExists;
  //      ConflictResolution cr = fm.get(ON_MERGE_CONFLICT) instanceof String ?
  //          ConflictResolution.of((String) fm.get(ON_MERGE_CONFLICT)) : conflictResolution;
  //
  //      List<String> featureIds = (List<String>) fm.get("featureIds");
  //      Map<String, Object> featureCollection = (Map<String, Object>) fm.get("featureData");
  //      List<Map<String, Object>> features = null;
  //      if (featureCollection != null) {
  //        features = (List<Map<String, Object>>) featureCollection.get("features");
  //      }
  //
  //      if (featureIds == null && features == null) {
  //        return Stream.empty();
  //      }
  //
  //      if (featureIds != null) {
  //        if (features == null) {
  //          features = idsToFeatures(featureIds);
  //        } else {
  //          features.addAll(idsToFeatures(featureIds));
  //        }
  //      }
  //
  //      return features.stream().map(feature -> new FeatureModificationEntry(feature, ne, e, cr));
  //    }).collect(Collectors.toList()), isTransactional);
  //  }
  //
  //  private static List<Map<String, Object>> idsToFeatures(List<String> featureIds) {
  //    return featureIds.stream().map(fId -> new JsonObject().put("id",
  // fId).getMap()).collect(Collectors.toList());
  //  }
  //
  //  /**
  //   * Validates whether the feature can be created based on the space's flag
  // allowFeatureCreationWithUUID. Creation of features using UUID in
  //   * the payload should always return an error, however at the moment, due to a bug, the
  // creation succeeds.
  //   *
  //   * @param entry
  //   * @throws ModificationException
  //   */
  //  @Override
  //  public void validateCreate(FeatureModificationEntry<Feature> entry) throws
  // ModificationException {
  //    if (!allowFeatureCreationWithUUID && entry.inputUUID != null) {
  //      throw new ModificationException(
  //          "The feature with id " + entry.input.get("id") + " cannot be created. Property UUID
  // should not be provided as input.");
  //    }
  //  }
}
