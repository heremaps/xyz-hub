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

package com.here.xyz.models.payload.events.feature;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.payload.events.clustering.Clustering;
import com.here.xyz.models.payload.events.tweaks.Tweaks;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByBBoxEvent")
public class GetFeaturesByBBoxEvent extends SpatialQueryEvent {

    @JsonProperty
    public BBox bbox;

    @JsonProperty
    public Clustering clustering;

    @JsonProperty
    public Tweaks tweaks;

    public BBox getBbox() {
        return this.bbox;
    }

    public void setBbox(BBox bbox) {
        this.bbox = bbox;
    }

    public @Nullable Clustering getClustering() {
        return this.clustering;
    }

    public void setClustering(@Nullable Clustering clustering) {
        this.clustering = clustering;
    }

    public @Nullable Tweaks getTweaks() {
        return this.tweaks;
    }

    public void setTweaks(@Nullable Tweaks tweaks) {
        this.tweaks = tweaks;
    }
}
