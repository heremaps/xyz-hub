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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** An error response. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ErrorResponse")
public class ErrorResponse extends XyzResponse {

    @JsonProperty
    private XyzError error;
    @JsonProperty
    private String errorMessage;
    @JsonProperty
    private String streamId;
    @JsonProperty
    private @Nullable Map<@NotNull String, @Nullable Object> errorDetails;

    /**
     * Returns the errorDetails which can contains additional detail information.
     *
     * @return the errorDetails map.
     */
    public Map<String, Object> getErrorDetails() {
        return this.errorDetails;
    }

    /**
     * Set the errorDetails map to the provided value.
     *
     * @param errorDetails the map with detailed information to be set.
     */
    public void setErrorDetails(final Map<String, Object> errorDetails) {
        this.errorDetails = errorDetails;
    }

    public ErrorResponse withErrorDetails(final Map<String, Object> errorDetails) {
        setErrorDetails(errorDetails);
        return this;
    }

    /**
     * Returns the error code as enumeration value.
     *
     * @return the error code as enumeration value.
     */
    public XyzError getError() {
        return this.error;
    }

    /**
     * Set the error code to the provided value.
     *
     * @param error the error code to be set.
     */
    public void setError(final XyzError error) {
        this.error = error;
    }

    @SuppressWarnings("unused")
    public ErrorResponse withError(final XyzError error) {
        setError(error);
        return this;
    }

    /**
     * Returns a human readable English error message that describes the error details.
     *
     * @return a human readable English error message that describes the error details.
     */
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Set the error message to the provided value.
     *
     * @param errorMessage the error message to be set.
     */
    @SuppressWarnings("WeakerAccess")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @SuppressWarnings("unused")
    public ErrorResponse withErrorMessage(String errorMessage) {
        setErrorMessage(errorMessage);
        return this;
    }

    /**
     * The unique stream-identifier of this request used to search in log files across the XYZ
     * platform what happened while processing the request.
     *
     * @return the unique stream-identifier of this request
     */
    public String getStreamId() {
        return this.streamId;
    }

    /**
     * Set the unique stream-identifier of this request used to search in log files across the XYZ
     * platform what happened while processing the request.
     *
     * @param streamId the unique stream-identifier to be set.
     */
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    @SuppressWarnings({"unused"})
    public ErrorResponse withStreamId(String streamId) {
        setStreamId(streamId);
        return this;
    }
}
