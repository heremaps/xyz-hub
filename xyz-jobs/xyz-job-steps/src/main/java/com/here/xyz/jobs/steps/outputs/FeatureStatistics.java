/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.outputs;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.models.hub.Ref;

@JsonInclude(NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureStatistics extends ModelBasedOutput {

    /**
     * For an import: The rows of the input files. An import into an empty layer drops the deletions before counting.
     */
    @JsonInclude(ALWAYS)
    private long featureCount;
    @JsonInclude(ALWAYS)
    private long byteSize;
    @JsonInclude(NON_DEFAULT)
    private Ref versionRef;
    @JsonInclude(NON_DEFAULT)
    private long fileCount;
    /**
     * For an import: The features which were not visible before and are now. With the updated, deleted and unchanged counts it adds
     * up to the featureCount. All four are null if not counted (see TaskedImportFilesToSpace#withWrittenFeatures()).
     * NOTE: Not serialized, only logged for now.
     * TODO: Decide how to publish them, e.g., with the output (remove @JsonIgnore, document in openapi.yaml) or as a separate output
     *  with SYSTEM visibility for a reporter.
     */
    @JsonIgnore
    private Long insertedCount;
    /**
     * For an import: The features which were visible before and were written again. With history, also the ones which did not
     * differ (they still get a new version). A feature in several batches is counted by each of them.
     * NOTE: A new feature in several files which are imported at the same time can be counted as inserted by each of them.
     */
    @JsonIgnore
    private Long updatedCount;
    /**
     * For an import: The features which were visible before and are deleted now.
     */
    @JsonIgnore
    private Long deletedCount;
    /**
     * For an import: The rest, e.g., duplicates within a batch, deletions of features which were not visible and, without history,
     * features which did not differ.
     */
    @JsonIgnore
    private Long unchangedCount;
    @JsonInclude(NON_DEFAULT)
    private String tag;

    public long getFeatureCount() {
        return featureCount;
    }

    public void setFeatureCount(long featureCount) {
        this.featureCount = featureCount;
    }

    public FeatureStatistics withFeatureCount(long featureCount) {
        setFeatureCount(featureCount);
        return this;
    }

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public FeatureStatistics withByteSize(long byteSize) {
        setByteSize(byteSize);
        return this;
    }

    public Ref getVersionRef() {
        return versionRef;
    }

    public void setVersionRef(Ref versionRef) {
        this.versionRef = versionRef;
    }

    public FeatureStatistics withVersionRef(Ref versionRef) {
        setVersionRef(versionRef);
        return this;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public FeatureStatistics withFileCount(long fileCount) {
        setFileCount(fileCount);
        return this;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public FeatureStatistics withTag(String tag) {
        setTag(tag);
        return this;
    }

    public Long getInsertedCount() {
        return insertedCount;
    }

    public void setInsertedCount(Long insertedCount) {
        this.insertedCount = insertedCount;
    }

    public FeatureStatistics withInsertedCount(Long insertedCount) {
        setInsertedCount(insertedCount);
        return this;
    }

    public Long getUpdatedCount() {
        return updatedCount;
    }

    public void setUpdatedCount(Long updatedCount) {
        this.updatedCount = updatedCount;
    }

    public FeatureStatistics withUpdatedCount(Long updatedCount) {
        setUpdatedCount(updatedCount);
        return this;
    }

    public Long getDeletedCount() {
        return deletedCount;
    }

    public void setDeletedCount(Long deletedCount) {
        this.deletedCount = deletedCount;
    }

    public FeatureStatistics withDeletedCount(Long deletedCount) {
        setDeletedCount(deletedCount);
        return this;
    }

    public Long getUnchangedCount() {
        return unchangedCount;
    }

    public void setUnchangedCount(Long unchangedCount) {
        this.unchangedCount = unchangedCount;
    }

    public FeatureStatistics withUnchangedCount(Long unchangedCount) {
        setUnchangedCount(unchangedCount);
        return this;
    }
}
