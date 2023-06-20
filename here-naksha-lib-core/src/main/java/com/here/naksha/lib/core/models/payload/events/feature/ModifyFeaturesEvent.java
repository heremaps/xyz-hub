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

package com.here.naksha.lib.core.models.payload.events.feature;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.naksha.lib.core.models.payload.events.FeatureEvent;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Ask the xyz storage connector to modify the state of features. For those features that do not
 * have an ID, the storage must generate unique identifiers.
 *
 * <p>The response to this event will be a {@link FeatureCollection} where the {@link
 * FeatureCollection#getFeatures()} list contains the new HEAD state of all successfully modified
 * features.
 *
 * <p>For successfully inserted features their IDs are returned in the {@link
 * FeatureCollection#getInserted()} list. - For successfully updated features their IDs are returned
 * in the {@link FeatureCollection#getUpdated()} list. - For successfully deleted features their IDs
 * are returned in the {@link FeatureCollection#getDeleted()} list.
 *
 * <p>When the operation for a feature failed, then the reason is returned in the {@link
 * FeatureCollection#getFailed()} map, of which the key is the {@link Feature#getId()} and the value
 * is the error message (reason).
 *
 * @since 0.1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ModifyFeaturesEvent")
public final class ModifyFeaturesEvent extends FeatureEvent {

    /**
     * The features to insert; if any.
     *
     * @since 0.1.0
     */
    private @Nullable List<@NotNull Feature> insertFeatures;

    /**
     * The features to update; if any.
     *
     * @since 0.1.0
     */
    private @Nullable List<@NotNull Feature> updateFeatures;

    /**
     * The features for which to perform an upsert, this means no matter what the current state is, it
     * should be created or updated.
     *
     * @since 0.6.0
     */
    private @Nullable List<@NotNull Feature> upsertFeatures;

    /**
     * A map where the key is the identifier of the feature to delete, and the value optionally the
     * uuid of the state to delete. If the state is not {@code null} and the current head uuid is not
     * the expected one, deleting must fail.
     */
    private @Nullable Map<@NotNull String, @Nullable String> deleteFeatures;

    private Boolean transaction;

    @JsonInclude(Include.NON_DEFAULT)
    private boolean enableHistory;

    @JsonInclude(Include.NON_DEFAULT)
    private boolean enableUUID;

    @JsonInclude(Include.NON_DEFAULT)
    private boolean enableGlobalVersioning;

    private List<ModificationFailure> failed;
    private Integer maxVersionCount;

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
     * Returns the IDs map of the features to be deleted. That is a map where the key contains the
     * unique ID of the feature to be deleted. The value is the state hash or null, if the HEAD state
     * should be deleted.
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
     * Returns true if the store event should be transactional, therefore either fully succeed or
     * fully fail.
     *
     * @return true if the store event should be transactional, therefore either fully succeed or
     *     fully fail.
     */
    @SuppressWarnings("unused")
    public Boolean getTransaction() {
        return this.transaction;
    }

    /**
     * Sets the transactional state of the store event.
     *
     * @param transaction if true, then the store event should be transactional, therefore either
     *     fully succeed or fully fail.
     */
    @SuppressWarnings("WeakerAccess")
    public void setTransaction(Boolean transaction) {
        this.transaction = transaction;
    }

    @SuppressWarnings("unused")
    public ModifyFeaturesEvent withTransaction(Boolean transaction) {
        setTransaction(transaction);
        return this;
    }

    /**
     * Returns true if the history should be maintained.
     *
     * @return true if the history should be maintained, false otherwise.
     */
    @SuppressWarnings("unused")
    public boolean isEnableHistory() {
        return this.enableHistory;
    }

    /**
     * Sets the history.
     *
     * @param enableHistory if true, then the store history.
     */
    @SuppressWarnings("WeakerAccess")
    public void setEnableHistory(boolean enableHistory) {
        this.enableHistory = enableHistory;
    }

    @SuppressWarnings("unused")
    public ModifyFeaturesEvent withEnableHistory(boolean enableHistory) {
        setEnableHistory(enableHistory);
        return this;
    }

    /**
     * Returns true if the hash should be maintained.
     *
     * @return true if the hash should be maintained, false otherwise.
     */
    @SuppressWarnings("unused")
    public boolean getEnableUUID() {
        return this.enableUUID;
    }

    /**
     * Sets the enabler for uuid.
     *
     * @param enableUUID if true, then set an uuid for each feature state
     */
    @SuppressWarnings("WeakerAccess")
    public void setEnableUUID(boolean enableUUID) {
        this.enableUUID = enableUUID;
    }

    @SuppressWarnings("unused")
    public ModifyFeaturesEvent withEnableUUID(boolean enableUUID) {
        setEnableUUID(enableUUID);
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

    public Integer getMaxVersionCount() {
        return maxVersionCount;
    }

    public void setMaxVersionCount(Integer maxVersionCount) {
        this.maxVersionCount = maxVersionCount;
    }

    public ModifyFeaturesEvent withMaxVersionCount(Integer maxVersionCount) {
        setMaxVersionCount(maxVersionCount);
        return this;
    }

    public boolean isEnableGlobalVersioning() {
        return enableGlobalVersioning;
    }

    public void setEnableGlobalVersioning(final boolean enableGlobalVersioning) {
        this.enableGlobalVersioning = enableGlobalVersioning;
    }

    public ModifyFeaturesEvent withEnableGlobalVersioning(final boolean enableGlobalVersioning) {
        this.enableGlobalVersioning = enableGlobalVersioning;
        return this;
    }
}
