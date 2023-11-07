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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import java.util.List;
import java.util.Map;

/**
 * Ask the xyz storage connector to modify the state of features. For those features that do not have an ID, the storage must generated
 * unique identifiers.
 *
 * The response to this event will be a {@link FeatureCollection} where the {@link FeatureCollection#getFeatures()} list contains the new
 * HEAD state of all successfully modified features.
 *
 * - For successfully inserted features their IDs are returned in the {@link FeatureCollection#getInserted()} list. - For successfully
 * updated features their IDs are returned in the {@link FeatureCollection#getUpdated()} list. - For successfully deleted features their IDs
 * are returned in the {@link FeatureCollection#getDeleted()} list.
 *
 * When the operation for a feature failed, then the reason is returned in the {@link FeatureCollection#getFailed()} map, of which the key
 * is the {@link Feature#getId()} and the value is the error message (reason).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ModifyFeaturesEvent")
public final class ModifyFeaturesEvent extends ContextAwareEvent<ModifyFeaturesEvent> {

  private List<Feature> insertFeatures;
  private List<Feature> updateFeatures;
  private List<Feature> upsertFeatures;
  @JsonInclude(Include.ALWAYS)
  private Map<String, String> deleteFeatures;
  private boolean transaction;
  private boolean conflictDetectionEnabled;
  private List<ModificationFailure> failed;

  /**
   * Returns the list of all features to be inserted.
   *
   * @return the list of all features to be inserted.
   */
  @SuppressWarnings("unused")
  public List<Feature> getInsertFeatures() {
    return this.insertFeatures;
  }

  @SuppressWarnings("WeakerAccess")
  public void setInsertFeatures(List<Feature> insertFeatures) {
    this.insertFeatures = insertFeatures;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withInsertFeatures(List<Feature> insertFeatures) {
    setInsertFeatures(insertFeatures);
    return this;
  }

  /**
   * Returns the list of all features to be updated.
   *
   * @return the list of all features to be updated.
   */
  @SuppressWarnings("unused")
  public List<Feature> getUpdateFeatures() {
    return this.updateFeatures;
  }

  @SuppressWarnings("WeakerAccess")
  public void setUpdateFeatures(List<Feature> updateFeatures) {
    this.updateFeatures = updateFeatures;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withUpdateFeatures(List<Feature> updateFeatures) {
    setUpdateFeatures(updateFeatures);
    return this;
  }

  /**
   * Returns the list of all features to be updated.
   *
   * @return the list of all features to be updated.
   */
  @SuppressWarnings("unused")
  public List<Feature> getUpsertFeatures() {
    return this.upsertFeatures;
  }

  @SuppressWarnings("WeakerAccess")
  public void setUpsertFeatures(List<Feature> upsertFeatures) {
    this.upsertFeatures = upsertFeatures;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withUpsertFeatures(List<Feature> upsertFeatures) {
    setUpsertFeatures(upsertFeatures);
    return this;
  }

  /**
   * Returns the IDs map of the features to be deleted. That is a map where the key contains the unique ID of the feature to be deleted. The
   * value is the state hash or null, if the HEAD state should be deleted.
   *
   * @return the IDs map.
   */
  @SuppressWarnings("unused")
  public Map<String, String> getDeleteFeatures() {
    return this.deleteFeatures;
  }

  @SuppressWarnings("WeakerAccess")
  public void setDeleteFeatures(Map<String, String> deleteFeatures) {
    this.deleteFeatures = deleteFeatures;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withDeleteFeatures(Map<String, String> deleteFeatures) {
    setDeleteFeatures(deleteFeatures);
    return this;
  }

  /**
   * Returns true if the store event should be transactional, therefore either fully succeed or fully fail.
   *
   * @return true if the store event should be transactional, therefore either fully succeed or fully fail.
   */
  @SuppressWarnings("unused")
  public boolean getTransaction() {
    return this.transaction;
  }

  /**
   * Sets the transactional state of the store event.
   *
   * @param transaction if true, then the store event should be transactional, therefore either fully succeed or fully fail.
   */
  @SuppressWarnings("WeakerAccess")
  public void setTransaction(boolean transaction) {
    this.transaction = transaction;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withTransaction(boolean transaction) {
    setTransaction(transaction);
    return this;
  }

  /**
   * @return Whether conflict detection should be enabled for this modification.
   */
  public boolean isConflictDetectionEnabled() {
    return conflictDetectionEnabled;
  }

  /**
   * @param conflictDetectionEnabled whether conflict detection should be enabled for this modification.
   */
  public void setConflictDetectionEnabled(boolean conflictDetectionEnabled) {
    this.conflictDetectionEnabled = conflictDetectionEnabled;
  }

  /**
   * @param conflictDetectionEnabled whether conflict detection should be enabled for this modification.
   * @return this event object for chaining.
   */
  public ModifyFeaturesEvent withConflictDetectionEnabled(boolean conflictDetectionEnabled) {
    setConflictDetectionEnabled(conflictDetectionEnabled);
    return this;
  }

  /**
   * @return A list of modification failures
   */
  @SuppressWarnings("unused")
  public List<ModificationFailure> getFailed() {
    return this.failed;
  }

  @SuppressWarnings("WeakerAccess")
  public void setFailed(List<ModificationFailure> failed) {
    this.failed = failed;
  }

  @SuppressWarnings("unused")
  public ModifyFeaturesEvent withFailed(List<ModificationFailure> failed) {
    setFailed(failed);
    return this;
  }
}
