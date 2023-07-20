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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Response
 * <p>
 * A response of a healthcheck request returned by an application or service
 *
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({"schemaVersion", "reporter", "node", "endpoint", "status", "message", "checks"})
public class Response implements ResponseSerializer {

  /**
   * The version of the healthcheckresult schema
   *
   */
  @JsonProperty("schemaVersion")
  @JsonView(Public.class)
  @JsonPropertyDescription("The version of the healthcheckresult schema")
  private String schemaVersion = "0.9";
  /**
   * Reporter
   * <p>
   * Reporter of a healthcheck
   *
   */
  @JsonProperty("reporter")
  @JsonView(Public.class)
  @JsonPropertyDescription("Reporter of a healthcheck")
  private Reporter reporter;
  /**
   * PRIVATE: The hostname or instancename that was called and where the application or service is running
   *
   */
  @JsonProperty("node")
  @JsonPropertyDescription(
      "PRIVATE: The hostname or instancename that was called and where the application or service is running")
  private String node;
  /**
   * The endpoint of the node that was called and which returned the healthcheckresponse
   *
   */
  @JsonProperty("endpoint")
  @JsonPropertyDescription("The endpoint of the node that was called and which returned the healthcheckresponse")
  private URI endpoint;
  /**
   * Status
   * <p>
   * Status of a check
   * (Required)
   *
   */
  @JsonProperty("status")
  @JsonView(Public.class)
  @JsonPropertyDescription("Status of a check")
  private Status status;
  /**
   * A custom message on the healthcheckresult
   *
   */
  @JsonProperty("message")
  @JsonView(Public.class)
  @JsonPropertyDescription("A custom message on the healthcheckresult")
  private String message;
  /**
   * Optional details on components or dependencies
   *
   */
  @JsonProperty("checks")
  @JsonDeserialize(as = LinkedHashSet.class)
  @JsonPropertyDescription("Optional details on components or dependencies")
  private Set<Check> checks = new LinkedHashSet<Check>();

  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /**
   * The version of the healthcheckresult schema
   *
   */
  @JsonProperty("schemaVersion")
  public String getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * The version of the healthcheckresult schema
   *
   */
  @JsonProperty("schemaVersion")
  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public Response withSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
    return this;
  }

  /**
   * Reporter
   * <p>
   * Reporter of a healthcheck
   *
   */
  @JsonProperty("reporter")
  public Reporter getReporter() {
    return reporter;
  }

  /**
   * Reporter
   * <p>
   * Reporter of a healthcheck
   *
   */
  @JsonProperty("reporter")
  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  public Response withReporter(Reporter reporter) {
    this.reporter = reporter;
    return this;
  }

  /**
   * PRIVATE: The hostname or instancename that was called and where the application or service is running
   *
   */
  @JsonProperty("node")
  public String getNode() {
    return node;
  }

  /**
   * PRIVATE: The hostname or instancename that was called and where the application or service is running
   *
   */
  @JsonProperty("node")
  public void setNode(String node) {
    this.node = node;
  }

  public Response withNode(String node) {
    this.node = node;
    return this;
  }

  /**
   * The endpoint of the node that was called and which returned the healthcheckresponse
   *
   */
  @JsonProperty("endpoint")
  public URI getEndpoint() {
    return endpoint;
  }

  /**
   * The endpoint of the node that was called and which returned the healthcheckresponse
   *
   */
  @JsonProperty("endpoint")
  public void setEndpoint(URI endpoint) {
    this.endpoint = endpoint;
  }

  public Response withEndpoint(URI endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  /**
   * Status
   * <p>
   * Status of a check
   * (Required)
   *
   */
  @JsonProperty("status")
  public Status getStatus() {
    return status;
  }

  /**
   * Status
   * <p>
   * Status of a check
   * (Required)
   *
   */
  @JsonProperty("status")
  public void setStatus(Status status) {
    this.status = status;
  }

  public Response withStatus(Status status) {
    this.status = status;
    return this;
  }

  /**
   * A custom message on the healthcheckresult
   *
   */
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  /**
   * A custom message on the healthcheckresult
   *
   */
  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }

  public Response withMessage(String message) {
    this.message = message;
    return this;
  }

  /**
   * Optional details on components or dependencies
   *
   */
  @JsonProperty("checks")
  public Set<Check> getChecks() {
    return checks;
  }

  /**
   * Optional details on components or dependencies
   *
   */
  @JsonProperty("checks")
  public void setChecks(Set<Check> checks) {
    this.checks = checks;
  }

  public Response withChecks(Set<Check> checks) {
    this.checks = checks;
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

  public Response withAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("schemaVersion", schemaVersion)
        .append("reporter", reporter)
        .append("node", node)
        .append("endpoint", endpoint)
        .append("status", status)
        .append("message", message)
        .append("checks", checks)
        .append("additionalProperties", additionalProperties)
        .toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(node)
        .append(endpoint)
        .append(schemaVersion)
        .append(checks)
        .append(reporter)
        .append(additionalProperties)
        .append(message)
        .append(status)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Response) == false) {
      return false;
    }
    Response rhs = ((Response) other);
    return new EqualsBuilder()
        .append(node, rhs.node)
        .append(endpoint, rhs.endpoint)
        .append(schemaVersion, rhs.schemaVersion)
        .append(checks, rhs.checks)
        .append(reporter, rhs.reporter)
        .append(additionalProperties, rhs.additionalProperties)
        .append(message, rhs.message)
        .append(status, rhs.status)
        .isEquals();
  }
}
