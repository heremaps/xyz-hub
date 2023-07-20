/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static com.here.naksha.lib.core.AbstractTask.currentTask;
import static com.here.naksha.lib.core.NakshaContext.currentContext;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.ParameterError;
import com.here.naksha.lib.core.exceptions.TooManyTasks;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An error response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ErrorResponse")
public class ErrorResponse extends XyzResponse {

  /**
   * Create a new empty error-response, the stream-id is set calling {@link NakshaContext#streamId()} of the current context.
   */
  public ErrorResponse() {
    setError(XyzError.EXCEPTION);
    final AbstractTask<?> task = currentTask();
    if (task != null) {
      setErrorMessage("Error in task " + task.getClass().getName());
    } else {
      setErrorMessage("Internal Naksha-Hub error");
    }
    setStreamId(currentContext().streamId());
  }

  /**
   * Create a new error-response with the given values.
   *
   * @param error    The XYZ error to return.
   * @param message  The message to return.
   * @param streamId The stream-id; if {@code null} is given, then calling {@link NakshaContext#streamId()} of the current context.
   */
  public ErrorResponse(@NotNull XyzError error, @NotNull String message, @Nullable String streamId) {
    if (streamId == null) {
      streamId = currentContext().streamId();
    }
    setError(error);
    setErrorMessage(message);
    setStreamId(streamId);
  }

  /**
   * Create an error-response for the given exception.
   *
   * @param t        The exception for which to create an error response.
   * @param streamId The stream-id; if {@code null} is given, then calling {@link NakshaContext#streamId()} of the current context.
   */
  public ErrorResponse(@NotNull Throwable t, @Nullable String streamId) {
    if (streamId == null) {
      streamId = currentContext().streamId();
    }
    if (t instanceof XyzErrorException e) {
      setStreamId(streamId);
      setError(e.xyzError);
      setErrorMessage(t.getMessage());
    } else if (t instanceof ParameterError) {
      setStreamId(streamId);
      setError(XyzError.ILLEGAL_ARGUMENT);
      setErrorMessage(t.getMessage());
    } else if (t instanceof TooManyTasks) {
      setStreamId(streamId);
      setError(XyzError.TOO_MANY_REQUESTS);
      setErrorMessage(t.getMessage());
    } else {
      setStreamId(streamId);
      setError(XyzError.EXCEPTION);
      final AbstractTask<?> task = currentTask();
      if (task != null) {
        setErrorMessage("Exception in task " + task.getClass().getName() + ": " + t.getMessage());
      } else {
        setErrorMessage("Exception in service: " + t.getMessage());
      }
    }
  }

  @JsonProperty
  private XyzError error;

  @JsonProperty
  private String errorMessage;

  @JsonProperty
  private String streamId;

  @JsonProperty
  private @Nullable Map<@NotNull String, @Nullable Object> errorDetails;

  /**
   * Returns the {@link #errorDetails error-details}, which can contains additional information.
   *
   * @return the error-details map.
   */
  public @Nullable Map<@NotNull String, @Nullable Object> getErrorDetails() {
    return this.errorDetails;
  }

  /**
   * Set the errorDetails map to the provided value.
   *
   * @param errorDetails the map with detailed information to be set.
   */
  public void setErrorDetails(final @Nullable Map<@NotNull String, @Nullable Object> errorDetails) {
    this.errorDetails = errorDetails;
  }

  /**
   * Set the errorDetails map to the provided value.
   *
   * @param errorDetails the map with detailed information to be set.
   * @return this.
   */
  public ErrorResponse withErrorDetails(final @Nullable Map<@NotNull String, @Nullable Object> errorDetails) {
    setErrorDetails(errorDetails);
    return this;
  }

  /**
   * Sets some arbitrary error-detail information.
   *
   * @param key   The key of the detail.
   * @param value The value of detail.
   * @return this.
   */
  public ErrorResponse with(@NotNull String key, @Nullable Object value) {
    if (errorDetails == null) {
      errorDetails = new HashMap<>();
    }
    errorDetails.put(key, value);
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
   * The unique stream-identifier of this request used to search in log files across the XYZ platform what happened while processing the
   * request.
   *
   * @return the unique stream-identifier of this request
   */
  public String getStreamId() {
    return this.streamId;
  }

  /**
   * Set the unique stream-identifier of this request used to search in log files across the XYZ platform what happened while processing the
   * request.
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
