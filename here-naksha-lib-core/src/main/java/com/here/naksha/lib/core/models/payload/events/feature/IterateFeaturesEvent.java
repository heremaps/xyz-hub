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
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "IterateFeaturesEvent")
public final class IterateFeaturesEvent extends SearchForFeaturesEvent {

    private String handle;

    @Deprecated
    private Integer v;

    private List<String> sort;
    private Integer[] part;
    private long offset;

    @Deprecated
    public Integer getV() {
        return v;
    }

    @Deprecated
    public void setV(Integer v) {
        this.v = v;
    }

    @Deprecated
    public IterateFeaturesEvent withV(Integer v) {
        setV(v);
        return this;
    }

    @SuppressWarnings("unused")
    public String getHandle() {
        return handle;
    }

    @SuppressWarnings("WeakerAccess")
    public void setHandle(String handle) {
        this.handle = handle;
    }

    @SuppressWarnings("unused")
    public IterateFeaturesEvent withHandle(String handle) {
        setHandle(handle);
        return this;
    }

    @SuppressWarnings("unused")
    public List<String> getSort() {
        return this.sort;
    }

    @SuppressWarnings("WeakerAccess")
    public void setSort(List<String> sort) {
        this.sort = sort;
    }

    @SuppressWarnings("unused")
    public IterateFeaturesEvent withSort(List<String> sort) {
        setSort(sort);
        return this;
    }

    @SuppressWarnings("unused")
    public Integer[] getPart() {
        return this.part;
    }

    @SuppressWarnings("WeakerAccess")
    public void setPart(Integer[] part) {
        this.part = part;
    }

    @SuppressWarnings("unused")
    public IterateFeaturesEvent withPart(Integer[] part) {
        setPart(part);
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
