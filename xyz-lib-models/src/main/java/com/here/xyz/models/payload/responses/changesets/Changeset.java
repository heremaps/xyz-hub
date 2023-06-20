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

package com.here.xyz.models.payload.responses.changesets;

import com.fasterxml.jackson.annotation.*;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.payload.XyzResponse;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Changeset")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Changeset extends XyzResponse {

    @JsonTypeInfo( use = JsonTypeInfo.Id.NONE )
    private FeatureCollection inserted;
    @JsonTypeInfo( use = JsonTypeInfo.Id.NONE )
    private FeatureCollection updated;
    @JsonTypeInfo( use = JsonTypeInfo.Id.NONE )
    private FeatureCollection deleted;

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
