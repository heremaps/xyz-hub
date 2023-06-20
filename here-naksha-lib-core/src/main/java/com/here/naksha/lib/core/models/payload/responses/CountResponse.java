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

package com.here.naksha.lib.core.models.payload.responses;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.XyzResponse;

/** The response providing the count of features in a space. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "CountResponse")
@Deprecated
public class CountResponse extends XyzResponse {

    private Long count;
    private Boolean estimated;

    /**
     * Returns the proprietary count property that is used by Space count requests to return the
     * number of features found.
     *
     * @return the amount of features that are matching the query.
     */
    @SuppressWarnings("unused")
    public Long getCount() {
        return this.count;
    }

    /**
     * Sets the amount of features that where matching a query, without returning the features (so
     * features will be null or an empty array).
     *
     * @param count the amount of features that where matching a query, if null, then the property is
     *     removed.
     */
    @SuppressWarnings("WeakerAccess")
    public void setCount(final Long count) {
        this.count = count;
    }

    @SuppressWarnings("unused")
    public CountResponse withCount(final Long count) {
        setCount(count);
        return this;
    }

    /**
     * Returns the estimated flag that defines, if the value of the count property is an estimation.
     *
     * @return true, if the value of the count property is an estimation.
     */
    @SuppressWarnings("unused")
    public Boolean getEstimated() {
        return this.estimated;
    }

    /**
     * Sets the estimated flag that defines, if the count property is an estimation.
     *
     * @param estimated the estimated flag that defines, if the count property is an estimation.
     */
    @SuppressWarnings("WeakerAccess")
    public void setEstimated(Boolean estimated) {
        this.estimated = estimated;
    }

    @SuppressWarnings("unused")
    public CountResponse withEstimated(Boolean estimated) {
        setEstimated(estimated);
        return this;
    }
}
