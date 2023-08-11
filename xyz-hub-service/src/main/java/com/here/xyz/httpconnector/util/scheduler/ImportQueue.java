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
package com.here.xyz.httpconnector.util.scheduler;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.validate.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.hub.Core;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.pgclient.PgException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Job-Queue for IMPORT-Jobs (S3 -> RDS)
 */
public class ImportQueue extends JobQueue{
    private static final Logger logger = LogManager.getLogger();
    public static volatile long NODE_EXECUTED_IMPORT_MEMORY;
    protected static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE, Core.newThreadFactory("import-queue"));

    protected void process() throws InterruptedException, CannotDecodeException {

        for (Job job : getQueue()){
            if (!(job instanceof Import))
                return;

            isProcessingPossible(job)
                    .compose(j -> loadCurrentConfig(job))
                    .compose(currentJobConfig -> {
                        /**
                         * Job-Life-Cycle:
                         * waiting -> (validating) -> validated ->  queued -> (preparing) -> prepared -> (executing) -> executed -> (finalizing) -> finalized
                         * all stages can end up in failed
                         **/

                        if(currentJobConfig == null)
                            return Future.succeededFuture();

                        switch (currentJobConfig.getStatus()){
                            case finalized:
                                logger.info("job[{}] is finalized!", currentJobConfig.getId());
                                break;
                            case failed:
                                logger.info("job[{}] has failed!", currentJobConfig.getId());
                                break;
                            case waiting:
                                updateJobStatus(currentJobConfig,Job.Status.validating)
                                        .compose(j -> {
                                            Import validatedJob = validateJob(j);
                                            /** Set status of validation */
                                            return updateJobStatus(validatedJob);
                                        });
                                break;
                            case validated:
                                /** Reflect that the Job is loaded into job-queue */
                                updateJobStatus(currentJobConfig,Job.Status.queued);
                                break;
                            case queued:
                                updateJobStatus(currentJobConfig, Job.Status.preparing)
                                        .onSuccess(j ->
                                                 addReadOnlyLockToSpace(j)
                                                        .onSuccess(f2 -> prepareJob(j))
                                                        .onFailure(f -> setJobFailed(job, Import.ERROR_DESCRIPTION_READONLY_MODE_FAILED, Job.ERROR_TYPE_PREPARATION_FAILED))
                                        );
                                break;
                            case prepared:
                                updateJobStatus(currentJobConfig,Job.Status.executing)
                                        .onSuccess(j -> executeJob(j));
                                break;
                            case executed:
                                updateJobStatus(currentJobConfig,Job.Status.finalizing)
                                        .onSuccess(j -> finalizeJob(j));
                                break;
                        }

                        return Future.succeededFuture();
                    })
                    .onFailure(e -> logError(e, job.getId()));
        }
    }

    @Override
    protected Import validateJob(Job job){
        return ImportValidator.validateImportObjects((Import)job);
    }

    @Override
    protected void prepareJob(Job j){
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        CService.jdbcImporter.prepareImport(defaultSchema, (Import)j)
                .compose(
                        f2 ->  updateJobStatus(j ,Job.Status.prepared))
                .onFailure(f -> {
                    logger.warn("job[{}] preparation has failed!", j.getId(), f);

                    if (f instanceof PgException && ((PgException) f).getCode() != null) {
                        if (((PgException) f).getCode().equalsIgnoreCase("42P01")) {
                            logger.info("job[{}] TargetTable '{}' does not exist!", j.getId(), j.getTargetTable());
                            setJobFailed(j, Import.ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS, Job.ERROR_TYPE_PREPARATION_FAILED);
                            return;
                        }
                    }else if (f.getMessage() != null && f.getMessage().equalsIgnoreCase("SequenceNot0"))
                        setJobFailed(j, Import.ERROR_DESCRIPTION_SEQUENCE_NOT_0, Job.ERROR_TYPE_PREPARATION_FAILED);
                    else
                        setJobFailed(j, Import.ERROR_DESCRIPTION_UNEXPECTED, Job.ERROR_TYPE_PREPARATION_FAILED);
                });
    }

    @Override
    protected void executeJob(Job j){
        j.setExecutedAt(Core.currentTimeMillis() / 1000L);
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());
        List<Future> importFutures = new ArrayList<>();

        Map<String, ImportObject> importObjects = ((Import) j).getImportObjects();
        for (String key : importObjects.keySet()) {
            if(!importObjects.get(key).isValid())
                continue;

            if(importObjects.get(key).getStatus().equals(ImportObject.Status.imported)
                || importObjects.get(key).getStatus().equals(ImportObject.Status.failed))
                continue;

            /** compressed processing of 9,5GB leads into ~120 GB RDS Mem */
            long curFileSize = Long.valueOf(importObjects.get(key).isCompressed() ? (importObjects.get(key).getFilesize() * 12)  : importObjects.get(key).getFilesize());
            double maxMemInGB = new RDSStatus.Limits(CService.rdsLookupCapacity.get(j.getTargetConnector())).getMaxMemInGB();

            logger.info("job[{}] IMPORT_MEMORY {}/{} = {}% of max", j.getId(), NODE_EXECUTED_IMPORT_MEMORY, (maxMemInGB * 1024 * 1024 * 1024) , (NODE_EXECUTED_IMPORT_MEMORY/ (maxMemInGB * 1024 * 1024 * 1024)));

            //TODO: Also view RDS METRICS?
            if(NODE_EXECUTED_IMPORT_MEMORY < CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES){
                importObjects.get(key).setStatus(ImportObject.Status.processing);
                NODE_EXECUTED_IMPORT_MEMORY += curFileSize;
                logger.info("job[{}] start execution of {}! mem: {}", j.getId(), importObjects.get(key).getS3Key(j.getId(), key), NODE_EXECUTED_IMPORT_MEMORY);

                importFutures.add(
                        CService.jdbcImporter.executeImport(j.getId(), j.getTargetConnector(), defaultSchema, j.getTargetTable(),
                                        CService.configuration.JOBS_S3_BUCKET, importObjects.get(key).getS3Key(j.getId(), key), CService.configuration.JOBS_REGION, curFileSize, j.getCsvFormat() )
                                .onSuccess(result -> {
                                            NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                            logger.info("job[{}] Import of '{}' succeeded!", j.getId(), importObjects.get(key));

                                            importObjects.get(key).setStatus(ImportObject.Status.imported);
                                            if(result != null && result.indexOf("imported") !=1) {
                                                //242579 rows imported int…sv.gz of 56740921 bytes
                                                importObjects.get(key).setDetails(result.substring(0,result.indexOf("imported")+8));
                                            }
                                        }
                                )
                                .onFailure(e -> {
                                            NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                            logger.warn("JOB[{}] Import of '{}' failed - mem: {}!", j.getId(), importObjects.get(key), NODE_EXECUTED_IMPORT_MEMORY, e);
                                            importObjects.get(key).setStatus(ImportObject.Status.failed);
                                        }
                                )
                );
            }else {
                importObjects.get(key).setStatus(ImportObject.Status.waiting);
            }
        }

        CompositeFuture.join(importFutures)
                .onComplete(
                        t -> {
                            if(t.failed()){
                                logger.warn("job[{}] Import of '{}' failed! ", j.getId(), j.getTargetSpaceId(), t);

                                if(t.cause().getMessage() != null && t.cause().getMessage().equalsIgnoreCase("Fail to read any response from the server, the underlying connection might get lost unexpectedly."))
                                    setJobAborted(j);
                                else if(t.cause().getMessage() != null && t.cause().getMessage().contains("duplicate key value violates unique constraint")){
                                    setJobFailed(j, Import.ERROR_DESCRIPTION_IDS_NOT_UNIQUE, Job.ERROR_TYPE_EXECUTION_FAILED);
                                }else
                                    setJobFailed(j, Import.ERROR_DESCRIPTION_UNEXPECTED, Job.ERROR_TYPE_EXECUTION_FAILED);
                            }else{
                                int cntInvalid = 0;

                                for (String key : importObjects.keySet()) {
                                    if(importObjects.get(key).getStatus() == ImportObject.Status.failed)
                                        cntInvalid++;
                                    if(importObjects.get(key).getStatus() == ImportObject.Status.waiting){
                                        /** Some Imports are still queued - execute again */
                                        updateJobStatus(j, Job.Status.prepared);
                                    }
                                }

                                if(cntInvalid == importObjects.size()) {
                                    j.setErrorDescription(Import.ERROR_DESCRIPTION_ALL_IMPORTS_FAILED);
                                }
                                else if(cntInvalid > 0 && cntInvalid < importObjects.size()) {
                                    j.setErrorDescription(Import.ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED);
                                }

                                updateJobStatus(j, Job.Status.executed);
                            }
                    });
    }

    @Override
    protected void finalizeJob(Job j){
        String getDefaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        //@TODO: Limit parallel Creations
        CService.jdbcImporter.finalizeImport(j, getDefaultSchema)
                .onFailure(f -> {
                    logger.warn("job[{}] finalization failed!", j.getId(), f);

                    if(f.getMessage().equalsIgnoreCase(Import.ERROR_TYPE_ABORTED))
                        setJobAborted(j);
                    else
                        setJobFailed(j, null, Job.ERROR_TYPE_EXECUTION_FAILED);
                })
                .compose(
                    f -> {
                        j.setFinalizedAt(Core.currentTimeMillis() / 1000L);
                        logger.info("job[{}] finalization finished!", j.getId());
                        if(j.getErrorDescription() != null)
                            return updateJobStatus(j, Job.Status.failed);

                        return updateJobStatus(j, Job.Status.finalized);
                });
    }

    @Override
    protected boolean needRdsCheck(Job job) {
        /** In next stage we perform the import */
        if(job.getStatus().equals(Job.Status.prepared))
            return true;
        /** In next stage we create indices + other finalizations */
        if(job.getStatus().equals(Job.Status.executed))
            return true;
        return false;
    }

    /**
     * Begins executing the ImportQueue processing - periodically and asynchronously.
     *
     * @return This check for chaining
     */
    public JobQueue commence() {
        if (!commenced) {
            logger.info("Start!");
            commenced = true;
            this.executionHandle = this.executorService.scheduleWithFixedDelay(this, 0, CService.configuration.JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    @Override
    public void run() {
        try {
            this.executorService.submit(() -> {
                try {
                    process();
                }
                catch (InterruptedException | CannotDecodeException ignored) {
                    //Nothing to do here.
                }
            });
        }catch (Exception e) {
            logger.error("Error when executing ImportQueue! ", e);
        }
    }
}
