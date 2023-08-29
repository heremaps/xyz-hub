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

import static com.here.xyz.httpconnector.util.scheduler.ImportQueue.NODE_EXECUTED_IMPORT_MEMORY;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobAborted;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobFailed;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.updateJobStatus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.hub.Core;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Import extends Job {
    private static final Logger logger = LogManager.getLogger();
    public static String ERROR_TYPE_NO_DB_CONNECTION = "no_db_connection";

    public static String ERROR_DESCRIPTION_UPLOAD_MISSING = "UPLOAD_MISSING";
    public static String ERROR_DESCRIPTION_INVALID_FILE = "INVALID_FILE";
    public static String ERROR_DESCRIPTION_NO_VALID_FILES_FOUND = "NO_VALID_FILES_FOUND";
    public static String ERROR_DESCRIPTION_IDX_CREATION_FAILED = "IDX_CREATION_FAILED";
    public static String ERROR_DESCRIPTION_ALL_IMPORTS_FAILED = "ALL_IMPORTS_FAILED";
    public static String ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED = "IMPORTS_PARTIALLY_FAILED";
    public static String ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS = "TARGET_TABLE_DOES_NOT_EXISTS";
    public static String ERROR_DESCRIPTION_UNEXPECTED = "UNEXPECTED_ERROR";
    public static String ERROR_DESCRIPTION_IDS_NOT_UNIQUE = "IDS_NOT_UNIQUE";
    public static String ERROR_DESCRIPTION_READONLY_MODE_FAILED= "READONLY_MODE_FAILED";
    public static String ERROR_DESCRIPTION_SEQUENCE_NOT_0 = "SEQUENCE_NOT_0";

    @JsonInclude
    @JsonView({Public.class})
    private Map<String,ImportObject> importObjects;

    @JsonInclude
    private Type type = Type.Import;

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

    public Type getType() {
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

    public void resetFailedImportObjects(){
        for (String id : this.importObjects.keySet()) {
            if(this.importObjects.get(id).getStatus() != null
                    && this.importObjects.get(id).getStatus().equals(ImportObject.Status.failed))
                this.importObjects.get(id).setStatus(ImportObject.Status.waiting);
        }
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

    public void resetToPreviousState() throws Exception{

        switch (getStatus()){
            case failed:
            case aborted:
                resetFailedImportObjects();
                setErrorType(null);
                setErrorDescription(null);
                if(getLastStatus() != null) {
                    /** set to last valid state */
                    resetStatus(getLastStatus());
                    setLastStatus(null);
                    resetToPreviousState();
                }else
                    throw new Exception("No last Status found!");
                break;
            case validating:
                resetStatus(Status.waiting);
                break;
            case preparing:
                resetStatus(Status.queued);
                break;
            case executing:
                resetStatus(Status.prepared);
                break;
            case finalizing:
                resetStatus(Status.executed);
                break;
        }
    }

    @Override
    public String getQueryIdentifier() {
        return "import_hint";
    }

    @Override
    public void execute() {
        setExecutedAt(Core.currentTimeMillis() / 1000L);
        String defaultSchema = JDBCImporter.getDefaultSchema(getTargetConnector());
        List<Future> importFutures = new ArrayList<>();

        Map<String, ImportObject> importObjects = getImportObjects();
        for (String key : importObjects.keySet()) {
            if(!importObjects.get(key).isValid())
                continue;

            if(importObjects.get(key).getStatus().equals(ImportObject.Status.imported)
                || importObjects.get(key).getStatus().equals(ImportObject.Status.failed))
                continue;

            /** compressed processing of 9,5GB leads into ~120 GB RDS Mem */
            long curFileSize = Long.valueOf(importObjects.get(key).isCompressed() ? (importObjects.get(key).getFilesize() * 12)  : importObjects.get(key).getFilesize());
            double maxMemInGB = new RDSStatus.Limits(CService.rdsLookupCapacity.get(getTargetConnector())).getMaxMemInGB();

            logger.info("job[{}] IMPORT_MEMORY {}/{} = {}% of max", getId(), NODE_EXECUTED_IMPORT_MEMORY, (maxMemInGB * 1024 * 1024 * 1024) , (NODE_EXECUTED_IMPORT_MEMORY/ (maxMemInGB * 1024 * 1024 * 1024)));

            //TODO: Also view RDS METRICS?
            if (NODE_EXECUTED_IMPORT_MEMORY < CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES){
                importObjects.get(key).setStatus(ImportObject.Status.processing);
                NODE_EXECUTED_IMPORT_MEMORY += curFileSize;
                logger.info("job[{}] start execution of {}! mem: {}", getId(), importObjects.get(key).getS3Key(getId(), key), NODE_EXECUTED_IMPORT_MEMORY);

                importFutures.add(
                    CService.jdbcImporter.executeImport(getId(), getTargetConnector(), defaultSchema, getTargetTable(),
                            CService.configuration.JOBS_S3_BUCKET, importObjects.get(key).getS3Key(getId(), key), CService.configuration.JOBS_REGION, curFileSize, getCsvFormat() )
                        .onSuccess(result -> {
                                NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                logger.info("job[{}] Import of '{}' succeeded!", getId(), importObjects.get(key));

                                importObjects.get(key).setStatus(ImportObject.Status.imported);
                                if(result != null && result.indexOf("imported") !=1) {
                                    //242579 rows imported int…sv.gz of 56740921 bytes
                                    importObjects.get(key).setDetails(result.substring(0,result.indexOf("imported")+8));
                                }
                            }
                        )
                        .onFailure(e -> {
                                NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                logger.warn("JOB[{}] Import of '{}' failed - mem: {}!", getId(), importObjects.get(key), NODE_EXECUTED_IMPORT_MEMORY, e);
                                importObjects.get(key).setStatus(ImportObject.Status.failed);
                            }
                        )
                );
            }
            else
                importObjects.get(key).setStatus(ImportObject.Status.waiting);
        }

        CompositeFuture.join(importFutures)
            .onComplete(
                t -> {
                    if (t.failed()){
                        logger.warn("job[{}] Import of '{}' failed! ", getId(), getTargetSpaceId(), t);

                        if (t.cause().getMessage() != null && t.cause().getMessage().equalsIgnoreCase("Fail to read any response from the server, the underlying connection might get lost unexpectedly."))
                            setJobAborted(this);
                        else if (t.cause().getMessage() != null && t.cause().getMessage().contains("duplicate key value violates unique constraint"))
                            setJobFailed(this, Import.ERROR_DESCRIPTION_IDS_NOT_UNIQUE, Job.ERROR_TYPE_EXECUTION_FAILED);
                        else
                            setJobFailed(this, Import.ERROR_DESCRIPTION_UNEXPECTED, Job.ERROR_TYPE_EXECUTION_FAILED);
                    }
                    else {
                        int cntInvalid = 0;

                        for (String key : importObjects.keySet()) {
                            if (importObjects.get(key).getStatus() == ImportObject.Status.failed)
                                cntInvalid++;
                            if (importObjects.get(key).getStatus() == ImportObject.Status.waiting)
                                /** Some Imports are still queued - execute again */
                                updateJobStatus(this, Job.Status.prepared);
                        }

                        if (cntInvalid == importObjects.size())
                            setErrorDescription(Import.ERROR_DESCRIPTION_ALL_IMPORTS_FAILED);
                        else if(cntInvalid > 0 && cntInvalid < importObjects.size())
                            setErrorDescription(Import.ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED);

                        updateJobStatus(this, Job.Status.executed);
                    }
                });
    }

    @Override
    public void finalizeJob() {
        String getDefaultSchema = JDBCImporter.getDefaultSchema(getTargetConnector());

        //@TODO: Limit parallel Creations
        CService.jdbcImporter.finalizeImport(this, getDefaultSchema)
            .onFailure(f -> {
                logger.warn("job[{}] finalization failed!", getId(), f);

                if (f.getMessage().equalsIgnoreCase(Import.ERROR_TYPE_ABORTED))
                    setJobAborted(this);
                else
                    setJobFailed(this, null, Job.ERROR_TYPE_EXECUTION_FAILED);
            })
            .compose(
                f -> {
                    setFinalizedAt(Core.currentTimeMillis() / 1000L);
                    logger.info("job[{}] finalization finished!", getId());
                    if (getErrorDescription() != null)
                        return updateJobStatus(this, Job.Status.failed);

                    return updateJobStatus(this, Job.Status.finalized);
                });
    }
}
