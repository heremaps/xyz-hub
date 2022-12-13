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

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.responses.XyzResponse;

/**
 * A Changeset represents a set of Feature modifications having been performed as one transaction, single authored,
 * which can contain multiple operations like insertions, deletions and updates.
 */
@JsonInclude(Include.NON_DEFAULT)
public class FeatureChange extends Payload {

    private Operation operation;
    private Feature feature;

    public Operation getOperation() { return operation; }

    public void setOperation(Operation operation) { this.operation = operation; }

    public FeatureChange withOperation(Operation operation) {
        setOperation(operation);
        return this;
    }

    public Feature getFeature() {
        return feature;
    }

    public void setFeature(Feature feature) {
        this.feature = feature;
    }

    public FeatureChange withFeature(final Feature feature) {
        setFeature(feature);
        return this;
    }

    public enum Operation {
        INSERT, UPDATE, DELETE
    }
}
