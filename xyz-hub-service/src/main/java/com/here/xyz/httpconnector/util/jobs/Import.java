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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Import extends Job {
    public static String ERROR_TYPE_NO_DB_CONNECTION = "no_db_connection";

    public static String ERROR_DESCRIPTION_UPLOAD_MISSING = "UPLOAD_MISSING";
    public static String ERROR_DESCRIPTION_INVALID_FILE = "INVALID_FILE";
    public static String ERROR_DESCRIPTION_NO_VALID_FILES_FOUND = "NO_VALID_FILES_FOUND";
    public static String ERROR_DESCRIPTION_IDX_CREATION_FAILED = "IDX_CREATION_FAILED";
    public static String ERROR_DESCRIPTION_ALL_IMPORTS_FAILED = "ALL_IMPORTS_FAILED";
    public static String ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED = "IMPORTS_PARTIALLY_FAILED";
    public static String ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS = "TARGET_TABLE_DOES_NOT_EXISTS";
    public static String ERROR_DESCRIPTION_UNEXPECTED = "UNEXPECTED_ERROR";
    public static String ERROR_DESCRIPTION_TABLE_CLEANUP_FAILED = "TABLE_CLEANUP_FAILED";
    public static String ERROR_DESCRIPTION_READONLY_MODE_FAILED= "READONLY_MODE_FAILED";
    public static String ERROR_DESCRIPTION_SEQUENCE_NOT_0 = "SEQUENCE_NOT_0";

    @JsonInclude
    @JsonView({Public.class})
    private Map<String,ImportObject> importObjects;

    @JsonInclude
    private String type = "Import";

    @JsonView({Internal.class})
    private List<String> idxList;

    public Import(){ }

    public Import(String description, String targetSpaceId, String targetTable,CSVFormat csvFormat, Strategy strategy) {
        this.description = description;
        this.targetSpaceId = targetSpaceId;
        this.targetTable = targetTable;
        this.csvFormat = csvFormat;
        this.strategy = strategy;
    }

    public String getType() {
        return type;
    }

    public Map<String,ImportObject> getImportObjects() {
        if(importObjects == null)
            importObjects = new HashMap<>();
        return importObjects;
    }

    public void setImportObjects(Map<String, ImportObject> importObjects) {
        this.importObjects = importObjects;
    }

    public List<String> getIdxList() {
        return idxList;
    }

    public void setIdxList(List<String> idxList) {
        this.idxList = idxList;
    }

    public Import withId(final String id) {
        setId(id);
        return this;
    }

    public Import withImportObjects(Map<String, ImportObject> importObjects) {
        setImportObjects(importObjects);
        return this;
    }

    public Import withErrorDescription(final String errorDescription) {
        setErrorDescription(errorDescription);
        return this;
    }

    public Import withDescription(final String description) {
        setDescription(description);
        return this;
    }

    public Import withTargetSpaceId(final String targetSpaceId) {
        setTargetSpaceId(targetSpaceId);
        return this;
    }

    public Import withTargetTable(final String targetTable) {
        setTargetTable(targetTable);
        return this;
    }

    public Import withStatus(final Job.Status status) {
        setStatus(status);
        return this;
    }

    public Import withCsvFormat(CSVFormat csv_format) {
        setCsvFormat(csv_format);
        return this;
    }

    public Import withCsvFormat(Strategy importStrategy) {
        setStrategy(importStrategy);
        return this;
    }

    public Import withCreatedAt(final long createdAt) {
        setCreatedAt(createdAt);
        return this;
    }

    public Import withUpdatedAt(final long updatedAt) {
        setUpdatedAt(updatedAt);
        return this;
    }

    public Import withExecutedAt(final Long startedAt) {
        setExecutedAt(startedAt);
        return this;
    }

    public Import withFinalizedAt(final Long finalizedAt) {
        setFinalizedAt(finalizedAt);
        return this;
    }

    public Import withExp(final Long exp) {
        setExp(exp);
        return this;
    }

    public Import withTargetConnector(String targetConnector) {
        setTargetConnector(targetConnector);
        return this;
    }

    public Import withErrorType(String errorType) {
        setErrorType(errorType);
        return this;
    }

    public Import withSpaceVersion(final long spaceVersion) {
        setSpaceVersion(spaceVersion);
        return this;
    }

    public Import withAuthor(String author) {
        setAuthor(author);
        return this;
    }

    public Import withParams(Map params) {
        setParams(params);
        return this;
    }

    public void addImportObject(ImportObject importObject){
        if(this.importObjects == null)
            this.importObjects = new HashMap<>();
        this.importObjects.put(importObject.getFilename(), importObject);
    }

    public void addIdx(String idx){
        if(this.idxList == null)
            this.idxList = new ArrayList<>();
        this.idxList.add(idx);
    }
}
