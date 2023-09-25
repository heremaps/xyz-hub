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

package com.here.xyz.httpconnector.util.jobs;

import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;
import static com.here.xyz.httpconnector.util.scheduler.ImportQueue.NODE_EXECUTED_IMPORT_MEMORY;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobAborted;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobFailed;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.updateJobStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Import extends Job<Import> {
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
    @JsonIgnore
    private AtomicBoolean executing = new AtomicBoolean();

    @JsonInclude
    @JsonView({Public.class})
    private Map<String,ImportObject> importObjects;

    @JsonView({Internal.class})
    private List<String> idxList;

    private Future<String> checkRunningImportJobs() {
        //Check in node memory
        String jobId = CService.importQueue.checkRunningJobsOnSpace(getTargetSpaceId());

        if (jobId != null)
            return Future.succeededFuture(jobId);
        else
            //Check if other import is running on target
            return CService.jobConfigClient.getRunningJobsOnSpace(getMarker(), getTargetSpaceId(), "Import");
    }

    private Future<Job> checkRunningJobs() {
        //States are getting set in JobQueue
        return checkRunningImportJobs()
            .compose(runningJobs -> {
                if (runningJobs == null)
                    return Future.succeededFuture(this);
                else
                    return  Future.failedFuture(new HttpException(CONFLICT, "Job '" + runningJobs + "' is already running on target!"));
            });
    }

    @Override
    public Future<Import> init() {
        return setDefaults();
    }

    @Override
    public Future<Import> validate() {
        return super.validate()
            .compose(job ->  HubWebClient.getSpaceStatistics(getTargetSpaceId())
            .compose(statistics -> {
                long value = statistics.getCount().getValue();
                if (value != 0)
                    return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Layer is not empty!"));
                return Future.succeededFuture(this);
            }))
            .compose(j -> Future.succeededFuture(j), e -> e instanceof HttpException ? Future.failedFuture(e)
                : Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage(), e)));
    }

    public Import() {
        super();
    }

    public Import(String description, String targetSpaceId, String targetTable, CSVFormat csvFormat, Strategy strategy) {
        super(description, targetSpaceId, targetTable, strategy);
        setCsvFormat(csvFormat);
    }

    public static Import validateImportObjects(Import job){
        /** Validate if provided files are existing and ok */
        if(job.getImportObjects().size() == 0){
            job.setErrorDescription(ERROR_DESCRIPTION_UPLOAD_MISSING);
            job.setErrorType(ERROR_TYPE_VALIDATION_FAILED);
        }else {
            try {
                /** scan S3 Path for existing Uploads and validate first line of CSV */
                Map<String, ImportObject> scannedObjects = CService.jobS3Client.scanImportPath(job, job.getCsvFormat());
                if (scannedObjects.size() == 0) {
                    job.setErrorDescription(ERROR_DESCRIPTION_UPLOAD_MISSING);
                    job.setErrorType(ERROR_TYPE_VALIDATION_FAILED);
                }
                else {
                    boolean foundOneValid = false;
                    for (String key : job.getImportObjects().keySet()) {
                        if (scannedObjects.get(key) != null) {
                            /** S3 Object is found - update job with file metadata */
                            ImportObject scannedFile = scannedObjects.get(key);

                            if (!scannedFile.isValid()) {
                                /** Keep Upload-URL for retry */
                                scannedFile.setUploadUrl(job.getImportObjects().get(key).getUploadUrl());
                                /** If file is invalid fail validation */
                                job.setErrorDescription(ERROR_DESCRIPTION_INVALID_FILE);
                                job.setErrorType(ERROR_TYPE_VALIDATION_FAILED);
                            } else
                                foundOneValid = true;
                            /** Add meta-data */
                            job.addImportObject(scannedFile);
                        } else {
                            job.getImportObjects().get(key).setFilesize(-1);
                        }
                    }

                    if (!foundOneValid) {
                        /** No Uploads found */
                        job.setErrorDescription(ERROR_DESCRIPTION_NO_VALID_FILES_FOUND);
                        job.setErrorType(ERROR_TYPE_VALIDATION_FAILED);
                    }
                }
            }
            catch (Exception e) {
                logger.warn("job[{}] validation has failed! ", job.getId(), e);
                job.setStatus(failed);
                job.setErrorType(ERROR_TYPE_VALIDATION_FAILED);
            }
        }

        //ToDo: Decide if we want to proceed partially
        if (job.getErrorType() != null)
            job.setStatus(failed);
        else
            job.setStatus(Status.validated);

        return job;
    }

    @JsonIgnore
    public void isValidForCreateUrl() throws HttpException {
        if (getStatus() != waiting)
            throw new HttpException(PRECONDITION_FAILED, "Invalid state: " + getStatus() + " creation is only allowed on status = waiting");
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

    public Import withImportObjects(Map<String, ImportObject> importObjects) {
        setImportObjects(importObjects);
        return this;
    }

    public void resetFailedImportObjects(){
        for (String id : this.importObjects.keySet()) {
            if(this.importObjects.get(id).getStatus() != null
                    && this.importObjects.get(id).getStatus().equals(ImportObject.Status.failed))
                this.importObjects.get(id).setStatus(ImportObject.Status.waiting);
        }
    }

    public void addImportObject(ImportObject importObject) {
        if(this.importObjects == null)
            this.importObjects = new HashMap<>();
        this.importObjects.put(importObject.getFilename(), importObject);
    }

    public void addIdx(String idx){
        if(this.idxList == null)
            this.idxList = new ArrayList<>();
        this.idxList.add(idx);
    }

    public void resetToPreviousState() throws Exception {
        switch (getStatus()) {
            case failed:
            case aborted:
                resetFailedImportObjects();
                super.resetToPreviousState();
                break;
            case validating:
                resetStatus(waiting);
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
    public Future<Job> executeCreateUploadUrl(int urlCount) {
        try {
            isValidForCreateUrl();
        }
        catch (HttpException e) {
            return Future.failedFuture(e);
        }
        try {
            for (int i = 0; i < urlCount; i++) {
                addImportObject(CService.jobS3Client.generateUploadURL(this));
            }

            //Store Urls in Job-Config
            return CService.jobConfigClient.update(getMarker(), this);
        }
        catch (IOException e){
            logger.error(getMarker(), "job[{}] cant create S3 Upload-URL(s).", getId(), e);
            return Future.failedFuture(new HttpException(BAD_GATEWAY, "Can`t create S3 Upload-URL(s)"));
        }
    }

    @Override
    protected Future<Job> isValidForStart() {
        return super.isValidForStart()
            .compose(job -> checkRunningJobs());
    }

    @Override
    public String getQueryIdentifier() {
        return "import_hint";
    }

    @Override
    public boolean needRdsCheck() {
        //In next stage we perform the import
        if (getStatus().equals(Job.Status.prepared))
            return true;
        //In next stage we create indices + other finalizations
        if (getStatus().equals(Job.Status.executed))
            return true;
        return false;
    }

    public Future<Job> executeStart() {
        return isValidForStart()
            .compose(job -> prepareStart())
            .onSuccess(job -> CService.importQueue.addJob(job));
    }

    @Override
    public void execute() {
        if (!executing.compareAndSet(false, true))
            return;

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

            //Compressed processing of 9,5GB leads into ~120 GB RDS Mem
            long curFileSize = Long.valueOf(importObjects.get(key).isCompressed() ? (importObjects.get(key).getFilesize() * 12)  : importObjects.get(key).getFilesize());
            double maxMemInGB = RDSStatus.calculateMemory(CService.rdsLookupCapacity.get(getTargetConnector()));

            logger.info("job[{}] IMPORT_MEMORY {}/{} = {}% of max", getId(), NODE_EXECUTED_IMPORT_MEMORY, (maxMemInGB * 1024 * 1024 * 1024) , (NODE_EXECUTED_IMPORT_MEMORY/ (maxMemInGB * 1024 * 1024 * 1024)));

            //TODO: Also view RDS METRICS?
            if (NODE_EXECUTED_IMPORT_MEMORY < CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES){
                importObjects.get(key).setStatus(ImportObject.Status.processing);
                NODE_EXECUTED_IMPORT_MEMORY += curFileSize;
                logger.info("job[{}] start execution of {}! mem: {}", getId(), importObjects.get(key).getS3Key(getId(), key), NODE_EXECUTED_IMPORT_MEMORY);

                importFutures.add(
                    CService.jdbcImporter.executeImport(this, getTargetConnector(), defaultSchema, getTargetTable(),
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
                        int failedImports = 0;
                        boolean filesWaiting = false;

                        for (String key : importObjects.keySet()) {
                            if (importObjects.get(key).getStatus() == ImportObject.Status.failed)
                                failedImports++;
                            if (importObjects.get(key).getStatus() == ImportObject.Status.waiting) {
                                /** Some Imports are still queued - execute again */
                                updateJobStatus(this, Job.Status.prepared);
                                filesWaiting = true;
                            }
                        }

                        if (!filesWaiting) {
                            if (failedImports == importObjects.size())
                                setErrorDescription(Import.ERROR_DESCRIPTION_ALL_IMPORTS_FAILED);
                            else if(failedImports > 0 && failedImports < importObjects.size())
                                setErrorDescription(Import.ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED);
                            updateJobStatus(this, Job.Status.executed);
                        }
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
                    logger.info("job[{}] finalization finished!", getId());
                    if (getErrorDescription() != null)
                        return updateJobStatus(this, failed);

                    return updateJobStatus(this, finalized);
                });
    }
}
