/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.responses.changesets;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzResponse;

/**
 * A Changeset represents a set of Feature modifications having been performed as one transaction, single authored,
 * which can contain multiple operations like insertions, deletions and updates.
 */
@JsonInclude(Include.NON_DEFAULT)
public class Changeset extends XyzResponse<Changeset> {

    long version = -1;
    String author;
    long createdAt;
    private FeatureCollection inserted;
    private FeatureCollection updated;
    private FeatureCollection deleted;

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Changeset withVersion(long version) {
        setVersion(version);
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Changeset withAuthor(String author) {
        setAuthor(author);
        return this;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public Changeset withCreatedAt(long createdAt) {
        setCreatedAt(createdAt);
        return this;
    }

    public FeatureCollection getInserted() {
        return inserted;
    }

    public void setInserted(FeatureCollection inserted) {
        this.inserted = inserted;
    }

    public Changeset withInserted(final FeatureCollection inserted) {
        setInserted(inserted);
        return this;
    }

    public FeatureCollection getUpdated() {
        return updated;
    }

    public void setUpdated(FeatureCollection updated) {
        this.updated = updated;
    }

    public Changeset withUpdated(final FeatureCollection updated) {
        setUpdated(updated);
        return this;
    }

    public FeatureCollection getDeleted() {
        return deleted;
    }

    public void setDeleted(FeatureCollection deleted) {
        this.deleted = deleted;
    }

    public Changeset withDeleted(final FeatureCollection deleted) {
        setDeleted(deleted);
        return this;
    }
}
