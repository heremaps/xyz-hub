
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
 * Check
 * <p>
 * Details of a what was checked and what was the result
 * 
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({
    "name",
    "essential",
    "role",
    "target",
    "status",
    "response"
})
public class Check {

    /**
     * The name of the check, component or dependency
     * (Required)
     * 
     */
    @JsonProperty("name")
    @JsonPropertyDescription("The name of the check, component or dependency")
    private String name;
    /**
     * Whether a negative response of this check should cause the health-check to fail
     * 
     */
    @JsonProperty("essential")
    @JsonPropertyDescription("Whether a negative response of this check should cause the health-check to fail")
    private Boolean essential = false;
    /**
     * The role of a check, component or dependency
     * 
     */
    @JsonProperty("role")
    @JsonPropertyDescription("The role of a check, component or dependency")
    private Check.Role role;
    /**
     * The target location which was checked
     *
     */
    @JsonProperty("target")
    @JsonPropertyDescription("The target location which was checked")
    private Check.Target target;
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
     * Response
     * <p>
     * A response of a health check request returned by an application or service
     *
     */
    @JsonProperty("response")
    @JsonPropertyDescription("A response of a health check request returned by an application or service")
    private Response response;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();

    /**
     * The name of the check, component or dependency
     * (Required)
     *
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     * The name of the check, component or dependency
     * (Required)
     *
     */
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @SuppressWarnings("unused")
    public Check withName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Whether a negative response of this check should cause the health-check to fail
     *
     */
    @JsonProperty("essential")
    public Boolean getEssential() {
        return essential;
    }

    /**
     * Whether a negative response of this check should cause the health-check to fail
     *
     */
    @SuppressWarnings("unused")
    @JsonProperty("essential")
    public void setEssential(Boolean essential) {
        this.essential = essential;
    }

    public Check withEssential(Boolean essential) {
        this.essential = essential;
        return this;
    }

    /**
     * The role of a check, component or dependency
     *
     */
    @JsonProperty("role")
    public Check.Role getRole() {
        return role;
    }

    /**
     * The role of a check, component or dependency
     *
     */
    @JsonProperty("role")
    public void setRole(Check.Role role) {
        this.role = role;
    }

    @SuppressWarnings("unused")
    public Check withRole(Check.Role role) {
        this.role = role;
        return this;
    }

    /**
     * The target location which was checked
     *
     */
    @JsonProperty("target")
    public Check.Target getTarget() {
        return target;
    }

    /**
     * The target location which was checked
     *
     */
    @JsonProperty("target")
    public void setTarget(Check.Target target) {
        this.target = target;
    }

    @SuppressWarnings("unused")
    public Check withTarget(Check.Target target) {
        this.target = target;
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

    @SuppressWarnings("unused")
    public Check withStatus(Status status) {
        this.status = status;
        return this;
    }

    /**
     * Response
     * <p>
     * A response of a health check request returned by an application or service
     *
     */
    @JsonProperty("response")
    public Response getResponse() {
        return response;
    }

    /**
     * Response
     * <p>
     * A response of a healthcheck request returned by an application or service
     *
     */
    @JsonProperty("response")
    public void setResponse(Response response) {
        this.response = response;
    }

    @SuppressWarnings("unused")
    public Check withResponse(Response response) {
        this.response = response;
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

    @SuppressWarnings("unused")
    public Check withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("name", name).append("essential", essential).append("role", role).append("target", target).append("status", status).append("response", response).append("additionalProperties", additionalProperties).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(role).append(response).append(name).append(additionalProperties).append(essential).append(target).append(status).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Check)) {
            return false;
        }
        Check rhs = ((Check) other);
        return new EqualsBuilder().append(role, rhs.role).append(response, rhs.response).append(name, rhs.name).append(additionalProperties, rhs.additionalProperties).append(essential, rhs.essential).append(target, rhs.target).append(status, rhs.status).isEquals();
    }

    public enum Role {

        DATABASE("DATABASE"),
        STORAGE("STORAGE"),
        CACHE("CACHE"),
        SERVICE("SERVICE"),
        AUTHENTICATION("AUTHENTICATION"),
        AUTHORIZATION("AUTHORIZATION"),
        CUSTOM("CUSTOM");
        private final String value;
        private final static Map<String, Check.Role> CONSTANTS = new HashMap<>();

        static {
            for (Check.Role c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Role(String value) {
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

    }

    public enum Target {

        LOCAL("LOCAL"),
        REMOTE("REMOTE");
        private final String value;
        private final static Map<String, Check.Target> CONSTANTS = new HashMap<>();

        static {
            for (Check.Target c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Target(String value) {
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

        @SuppressWarnings("unused")
        @JsonCreator
        public static Check.Target fromValue(String value) {
            Check.Target constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
