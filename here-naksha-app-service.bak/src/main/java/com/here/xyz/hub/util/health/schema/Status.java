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
package com.here.xyz.hub.util.health.schema;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonView;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Status
 * <p>
 * Status of a check
 *
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({"result", "checkDuration", "timestamp"})
public class Status implements HTTPResultMapper {

  /**
   * Result of a check
   * (Required)
   *
   */
  @JsonProperty("result")
  @JsonView(Public.class)
  @JsonPropertyDescription("Result of a check")
  private Status.Result result = Status.Result.fromValue("UNKNOWN");
  /**
   * The time in milliseconds which was needed to gatther this status
   *
   */
  @JsonProperty("checkDuration")
  @JsonView(Public.class)
  @JsonPropertyDescription("The time in milliseconds which was needed to gatther this status")
  private Long checkDuration;
  /**
   * Timestamp when the healthcheckresult was created in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("timestamp")
  @JsonView(Public.class)
  @JsonPropertyDescription(
      "Timestamp when the healthcheckresult was created in milliseconds elapsed since 01/01/1970")
  private Long timestamp;

  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /**
   * Result of a check
   * (Required)
   *
   */
  @JsonProperty("result")
  public Status.Result getResult() {
    return result;
  }

  /**
   * Result of a check
   * (Required)
   *
   */
  @JsonProperty("result")
  public void setResult(Status.Result result) {
    this.result = result;
  }

  public Status withResult(Status.Result result) {
    this.result = result;
    return this;
  }

  /**
   * The time in milliseconds which was needed to gatther this status
   *
   */
  @JsonProperty("checkDuration")
  public Long getCheckDuration() {
    return checkDuration;
  }

  /**
   * The time in milliseconds which was needed to gatther this status
   *
   */
  @JsonProperty("checkDuration")
  public void setCheckDuration(Long checkDuration) {
    this.checkDuration = checkDuration;
  }

  public Status withCheckDuration(Long checkDuration) {
    this.checkDuration = checkDuration;
    return this;
  }

  /**
   * Timestamp when the healthcheckresult was created in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("timestamp")
  public Long getTimestamp() {
    return timestamp;
  }

  /**
   * Timestamp when the healthcheckresult was created in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("timestamp")
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public Status withTimestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
  }

  public Status withAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("result", result)
        .append("checkDuration", checkDuration)
        .append("timestamp", timestamp)
        .append("additionalProperties", additionalProperties)
        .toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(result)
        .append(additionalProperties)
        .append(checkDuration)
        .append(timestamp)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Status) == false) {
      return false;
    }
    Status rhs = ((Status) other);
    return new EqualsBuilder()
        .append(result, rhs.result)
        .append(additionalProperties, rhs.additionalProperties)
        .append(checkDuration, rhs.checkDuration)
        .append(timestamp, rhs.timestamp)
        .isEquals();
  }

  public enum Result {
    OK("OK"),
    WARNING("WARNING"),
    UNKNOWN("UNKNOWN"),
    TIMEOUT("TIMEOUT"),
    UNAVAILABLE("UNAVAILABLE"),
    ERROR("ERROR"),
    CRITICAL("CRITICAL"),
    MAINTENANCE("MAINTENANCE");
    private final String value;
    private static final Map<String, Status.Result> CONSTANTS = new HashMap<String, Status.Result>();

    static {
      for (Status.Result c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    private Result(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @JsonValue
    public String value() {
      return this.value;
    }

    @JsonCreator
    public static Status.Result fromValue(String value) {
      Status.Result constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }
  }
}
