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

package com.here.naksha.lib.core.models.payload.events.info;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.Event;
import org.jetbrains.annotations.NotNull;

/** Check the status of the storage connector and request maintenance work. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "HealthCheckEvent")
public final class HealthCheckEvent extends Event {

    /** The database identifier. This is needed when installing the Naksha SQL functions. */
    @JsonProperty
    public Long dbId;

    @Deprecated
    @JsonProperty
    private long minResponseTime = 0L;

    @Deprecated
    @JsonProperty
    private int warmupCount = 0;

    /**
     * Returns the minimal response time in milliseconds. The storage connector should not send a
     * response until at least the given amount of milliseconds passed.
     *
     * @return the minimal amount of milliseconds to wait.
     */
    @Deprecated
    public long getMinResponseTime() {
        return minResponseTime;
    }

    @Deprecated
    public void setMinResponseTime(long minResponseTime) {
        this.minResponseTime = minResponseTime;
    }

    @Deprecated
    public @NotNull HealthCheckEvent withMinResponseTime(long timeInMilliseconds) {
        setMinResponseTime(timeInMilliseconds);
        return this;
    }

    /**
     * Returns the amount of runtime-environments which should be kept "warmed-up" in order to react
     * quickly to incoming traffic. The implementing connector should ensure to keep that amount of
     * runtime-environments ready.
     *
     * @return the amount of runtime-environments to keep ready.
     */
    @Deprecated
    public int getWarmupCount() {
        return warmupCount;
    }

    @Deprecated
    public void setWarmupCount(int warmupCount) {
        this.warmupCount = warmupCount;
    }

    @Deprecated
    public @NotNull HealthCheckEvent withWarmupCount(int warmupCount) {
        setWarmupCount(warmupCount);
        return this;
    }
}
