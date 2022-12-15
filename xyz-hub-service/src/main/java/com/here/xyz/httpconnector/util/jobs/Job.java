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
package com.here.xyz.httpconnector.util.jobs;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.hub.Space;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Import.class , name = "Import"),
        @JsonSubTypes.Type(value = Export.class , name = "Export")
})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public abstract class Job {
    @JsonView({Public.class})
    public enum Type {
        Import, Export;
        public static Type of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    @JsonView({Public.class})
    public enum Status {
        waiting, queued, validating, validated, preparing, prepared, executing, executed, finalizing, finalized, aborted, failed;
        public static Status of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toLowerCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    @JsonView({Public.class})
    public enum CSVFormat {
        GEOJSON,JSON_WKT,JSON_WKB;

        public static CSVFormat of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    @JsonView({Public.class})
    public enum Strategy {
        LASTWINS,SKIPEXISTING,ERROR;

        public static Strategy of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    /**
     * Beta release date: 2018-10-01T00:00Z[UTC]
     */
    @JsonIgnore
    private final long DEFAULT_TIMESTAMP = 1538352000000L;

    /**
     * The creation timestamp.
     */
    @JsonView({Public.class})
    public long createdAt = DEFAULT_TIMESTAMP;

    /**
     * The last update timestamp.
     */
    @JsonView({Public.class})
    public long updatedAt = DEFAULT_TIMESTAMP;

    /**
     * The job ID
     */
    @JsonView({Public.class})
    public String id;

    @JsonView({Public.class})
    public String description;

    @JsonView({Internal.class})
    public String targetSpaceId;

    @JsonView({Internal.class})
    public String targetTable;

    @JsonView({Public.class})
    public String errorDescription;

    @JsonView({Public.class})
    public String errorType;

    @JsonView({Public.class})
    public Status status;

    @JsonView({Public.class})
    public CSVFormat csvFormat;

    @JsonView({Public.class})
    public Strategy strategy;

    @JsonView({Internal.class})
    private String targetConnector;

    public String getId(){
        return id;
    }

    public void setId(final String id){
        this.id = id;
    }

    public Job withId(final String id) {
        setId(id);
        return this;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public Job withErrorDescription(final String errorDescription) {
        setErrorDescription(description);
        return this;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Job withDescription(final String description) {
        setDescription(description);
        return this;
    }


    public String getTargetSpaceId() {
        return targetSpaceId;
    }

    public void setTargetSpaceId(String targetSpaceId) {
        this.targetSpaceId = targetSpaceId;
    }

    public Job withTargetSpaceId(final String targetSpaceId) {
        setTargetSpaceId(targetSpaceId);
        return this;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public Job withTargetTable(final String targetTable) {
        setTargetTable(targetTable);
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Job.Status status) {
        this.status = status;
    }

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(CSVFormat csv_format) {
        this.csvFormat = csv_format;
    }

    public Job withCsvFormat(CSVFormat csv_format) {
        setCsvFormat(csv_format);
        return this;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public Job withCsvFormat(Strategy importStrategy) {
        setStrategy(importStrategy);
        return this;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final long createdAt) {
        this.createdAt = createdAt;
    }

    public Job withCreatedAt(final long createdAt) {
        setCreatedAt(createdAt);
        return this;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(final long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Job withUpdatedAt(final long updatedAt) {
        setUpdatedAt(updatedAt);
        return this;
    }

    public String getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(String targetConnector) {
        this.targetConnector = targetConnector;
    }

    public Job withTargetConnecto(String targetConnector) {
        setTargetConnector(targetConnector);
        return this;
    }

    public void setErrorType(String errorType){
        this.errorType = errorType;
    }

    public String getErrorType() {
        return errorType;
    }

    public static class Public {

    }

    public static class Internal extends Space.Internal {

    }

    public Job(){ }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
