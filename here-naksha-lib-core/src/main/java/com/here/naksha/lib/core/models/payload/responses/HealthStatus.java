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

package com.here.naksha.lib.core.models.payload.responses;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;

/**
 * The response being sent in response to an {@link HealthCheckEvent} when the service is healthy.
 * If the service is not healthy it should send back an {@link ErrorResponse}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "HealthStatus")
public class HealthStatus extends XyzResponse {

    private String status;

    public HealthStatus() {
        super();
        this.status = "OK";
    }

    public String getStatus() {
        return this.status;
    }

    @SuppressWarnings("WeakerAccess")
    public void setStatus(String status) {
        this.status = status;
    }

    @SuppressWarnings("unused")
    public HealthStatus withStatus(String status) {
        setStatus(status);
        return this;
    }
}
