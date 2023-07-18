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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonView;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Reporter
 * <p>
 * Reporter of a healthcheck
 *
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({"name", "version", "upSince", "buildDate", "endpoint"})
public class Reporter {

  /**
   * Application name or service name
   * (Required)
   *
   */
  @JsonProperty("name")
  @JsonView(Public.class)
  @JsonPropertyDescription("Application name or service name")
  private String name;
  /**
   * Application version or service version
   *
   */
  @JsonProperty("version")
  @JsonView(Public.class)
  @JsonPropertyDescription("Application version or service version")
  private String version;
  /**
   * Timestamp of when the application or service was started in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("upSince")
  @JsonView(Public.class)
  @JsonPropertyDescription(
      "Timestamp of when the application or service was started in milliseconds elapsed since 01/01/1970")
  private Long upSince;
  /**
   * Timestamp of when the application or service was build in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("buildDate")
  @JsonView(Public.class)
  @JsonPropertyDescription(
      "Timestamp of when the application or service was build in milliseconds elapsed since 01/01/1970")
  private Long buildDate;
  /**
   * The main / base endpoint of the application or service
   *
   */
  @JsonProperty("endpoint")
  @JsonView(Public.class)
  @JsonPropertyDescription("The main / base endpoint of the application or service")
  private URI endpoint;

  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /**
   * Application name or service name
   * (Required)
   *
   */
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  /**
   * Application name or service name
   * (Required)
   *
   */
  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  public Reporter withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Application version or service version
   *
   */
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  /**
   * Application version or service version
   *
   */
  @JsonProperty("version")
  public void setVersion(String version) {
    this.version = version;
  }

  public Reporter withVersion(String version) {
    this.version = version;
    return this;
  }

  /**
   * Timestamp of when the application or service was started in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("upSince")
  public Long getUpSince() {
    return upSince;
  }

  /**
   * Timestamp of when the application or service was started in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("upSince")
  public void setUpSince(Long upSince) {
    this.upSince = upSince;
  }

  public Reporter withUpSince(Long upSince) {
    this.upSince = upSince;
    return this;
  }

  /**
   * Timestamp of when the application or service was build in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("buildDate")
  public Long getBuildDate() {
    return buildDate;
  }

  /**
   * Timestamp of when the application or service was build in milliseconds elapsed since 01/01/1970
   *
   */
  @JsonProperty("buildDate")
  public void setBuildDate(Long buildDate) {
    this.buildDate = buildDate;
  }

  public Reporter withBuildDate(Long buildDate) {
    this.buildDate = buildDate;
    return this;
  }

  /**
   * The main / base endpoint of the application or service
   *
   */
  @JsonProperty("endpoint")
  public URI getEndpoint() {
    return endpoint;
  }

  /**
   * The main / base endpoint of the application or service
   *
   */
  @JsonProperty("endpoint")
  public void setEndpoint(URI endpoint) {
    this.endpoint = endpoint;
  }

  public Reporter withEndpoint(URI endpoint) {
    this.endpoint = endpoint;
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

  public Reporter withAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("name", name)
        .append("version", version)
        .append("upSince", upSince)
        .append("buildDate", buildDate)
        .append("endpoint", endpoint)
        .append("additionalProperties", additionalProperties)
        .toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(endpoint)
        .append(upSince)
        .append(name)
        .append(buildDate)
        .append(additionalProperties)
        .append(version)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Reporter) == false) {
      return false;
    }
    Reporter rhs = ((Reporter) other);
    return new EqualsBuilder()
        .append(endpoint, rhs.endpoint)
        .append(upSince, rhs.upSince)
        .append(name, rhs.name)
        .append(buildDate, rhs.buildDate)
        .append(additionalProperties, rhs.additionalProperties)
        .append(version, rhs.version)
        .isEquals();
  }
}
