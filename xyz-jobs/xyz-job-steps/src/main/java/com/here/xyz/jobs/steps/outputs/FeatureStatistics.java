/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.models.hub.Ref;

@JsonInclude(NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureStatistics extends ModelBasedOutput {

    @JsonInclude(ALWAYS)
    private long featureCount;
    @JsonInclude(ALWAYS)
    private long byteSize;
    @JsonInclude(NON_DEFAULT)
    private Ref versionRef;

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
}
