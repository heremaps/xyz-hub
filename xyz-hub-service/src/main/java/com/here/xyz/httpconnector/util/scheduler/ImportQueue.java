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
        //@TODO CleanUp Jobs: Check all Jobs which are finished/failed and delete them after some time.
        //@TODO Lost Jobs: Find possible orphaned Jobs (system crash) and add them to the queue again

        for (int i = 0; i <  getQueue().size(); i++) {
            Job job = getQueue().get(i);
            if(!(job instanceof Import))
                return;

            /** Check Capacity */
            isProcessingOnRDSPossible(i, job)
                    .onSuccess(canProcess -> {
                        /** Execution is currently not possible */
                        if (!canProcess)
                            return;

                        /** Check if JDBC Client is available */
                        if(!isTargetJDBCClientLoaded(job))
                            return;

                        /** Run first Job Queue (FIFO) */
                        Import importJob = (Import) job;

                        /**
                         * Job-Life-Cycle:
                         * waiting -> (validating) -> validated ->  queued -> (preparing) -> prepared -> (executing) -> executed -> (finalizing) -> finalized
                         * all stages can end up in failed
                         **/

                        switch (importJob.getStatus()){
                            case finalized:
                                logger.info("JOB[{}] is finalized!", importJob.getId());
                                /** Remove Job from Queue */
                                removeJob(importJob);
                                break;
                            case failed:
                                logger.info("JOB[{}] has failed!", importJob.getId());
                                /** Remove Job from Queue - in some cases the user is able to retry */
                                removeJob(importJob);
                                releaseReadOnlyLockFromSpace(importJob);
                                break;
                            case waiting:
                                updateJobStatus(importJob,Job.Status.validating)
                                        .onSuccess(f -> validateJob(importJob));
                                break;
                            case validated:
                                updateJobStatus(importJob,Job.Status.queued);
                                break;
                            case queued:
                                updateJobStatus(importJob, Job.Status.preparing)
                                        .onSuccess(f ->
                                                addReadOnlyLockToSpace(importJob)
                                                        .onSuccess(f2 -> prepareJob(importJob))
                                        );
                                break;
                            case prepared:
                                updateJobStatus(importJob,Job.Status.executing)
                                        .onSuccess(f -> executeJob(importJob));
                                break;
                            case executed:
                                updateJobStatus(importJob,Job.Status.finalizing)
                                        .onSuccess(f -> finalizeJob(importJob));
                                break;
                            default: {
                                logger.info("JOB[{}] is currently '{}' - current Queue-size: {}",importJob.getId(), importJob.getStatus(), queueSize());
                            }
                        }
                    })
                    .onFailure(e -> logger.info(e.getMessage()));
            }

    }

    @Override
    protected void validateJob(Job job){
        ImportValidator.validateImportJob((Import)job);
        updateJobStatus(job, null);
    }

    @Override
    protected void prepareJob(Job j){
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        CService.jdbcImporter.prepareImport(defaultSchema, (Import)j)
                .compose(
                        f2 -> {
                            j.setStatus(Job.Status.prepared);
                            return CService.jobConfigClient.update(null, j);
                        })
                .onFailure(f -> {
                    logger.warn("JOB[{}] preparation has failed!", j.getId(), f);

                    if (f instanceof PgException && ((PgException) f).getCode() != null) {
                        if (((PgException) f).getCode().equalsIgnoreCase("42P01")) {
                            logger.info("TargetTable '" + j.getTargetTable() + "' does not Exists!");
                            failJob(j, Import.ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS, Job.ERROR_TYPE_PREPARATION_FAILED);
                            return;
                        }
                    }
                    //@TODO: Collect more possible error-cases
                    if (f.getMessage() != null && f.getMessage().equalsIgnoreCase("SequenceNot0"))
                        failJob(j, Import.ERROR_DESCRIPTION_SEQUENCE_NOT_0, Job.ERROR_TYPE_PREPARATION_FAILED);
                    else
                        failJob(j, Import.ERROR_DESCRIPTION_UNEXPECTED, Job.ERROR_TYPE_PREPARATION_FAILED);
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

            logger.info("JOB[{}] IMPORT_MEMORY {}/{} = {}% of max", j.getId(), NODE_EXECUTED_IMPORT_MEMORY, (maxMemInGB * 1024 * 1024 * 1024) , (NODE_EXECUTED_IMPORT_MEMORY/ (maxMemInGB * 1024 * 1024 * 1024)));

            //TODO: Also view RDS METRICS?
            if(NODE_EXECUTED_IMPORT_MEMORY < CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES){
                importObjects.get(key).setStatus(ImportObject.Status.processing);
                NODE_EXECUTED_IMPORT_MEMORY += curFileSize;
                logger.info("JOB[{}] start execution of {}! mem: {}", j.getId(), importObjects.get(key).getS3Key(), NODE_EXECUTED_IMPORT_MEMORY);

                importFutures.add(
                        CService.jdbcImporter.executeImport(j.getId(), j.getTargetConnector(), defaultSchema, j.getTargetTable(),
                                        CService.configuration.JOBS_S3_BUCKET, importObjects.get(key).getS3Key(), CService.configuration.JOBS_REGION, curFileSize, j.getCsvFormat() )
                                .onSuccess(result -> {
                                            NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                            logger.info("JOB[{}] Import of '{}' succeeded!", j.getId(), importObjects.get(key));

                                            importObjects.get(key).setStatus(ImportObject.Status.imported);
                                            if(result != null && result.indexOf("imported") !=1) {
                                                //242579 rows imported int…sv.gz of 56740921 bytes
                                                importObjects.get(key).setDetails(result.substring(0,result.indexOf("imported")+8));
                                            }
                                        }
                                )
                                .onFailure(f -> {
                                            NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                                            logger.warn("JOB[{}] Import of '{}' failed ", j.getId(), importObjects.get(key), f);
                                            logger.warn("JOB[{}] failed execution. queue {} - mem: {} ",j.getId(), key, NODE_EXECUTED_IMPORT_MEMORY);

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
                            int cntInvalid = 0;

                            for (String key : importObjects.keySet()) {
                                if(importObjects.get(key).getStatus() == ImportObject.Status.failed)
                                    cntInvalid++;
                                if(importObjects.get(key).getStatus() == ImportObject.Status.waiting){
                                    /** Some Imports are still queued - execute again */
                                    j.setStatus(Job.Status.prepared);
                                    CService.jobConfigClient.update(null, j);
                                    return;
                                }
                            }

                            j.setStatus(Job.Status.executed);

                            if(cntInvalid == importObjects.size()) {
                                j.setErrorDescription(Import.ERROR_DESCRIPTION_ALL_IMPORTS_FAILED);
                            }
                            else if(cntInvalid > 0 && cntInvalid < importObjects.size()) {
                                j.setErrorDescription(Import.ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED);
                            }

                            CService.jobConfigClient.update(null, j);
                    });
    }

    @Override
    protected void finalizeJob(Job j){
        String getDefaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        //@TODO: Limit parallel Creations
        CService.jdbcImporter.finalizeImport(j, getDefaultSchema)
                .onFailure(f -> {
                    logger.warn("JOB[{}] finalization failed!", j.getId(), f);
                    failJob(j, null , Import.ERROR_TYPE_FINALIZATION_FAILED);
                })
                .compose(f -> releaseReadOnlyLockFromSpace(j))
                .compose(
                    f -> {
                        j.setFinalizedAt(Core.currentTimeMillis() / 1000L);
                        logger.info("JOB[{}] finalization finished!", j.getId());
                        if(j.getErrorDescription() != null)
                            j.setStatus(Job.Status.failed);
                        else
                            j.setStatus(Job.Status.finalized);

                        return CService.jobConfigClient.update(null, j);
                });
    }

    /**
     * Begins executing the JobQueue processing - periodically and asynchronously.
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
            logger.error("{}: Error when executing JobQueue!", this.getClass().getSimpleName(), e);
        }
    }
}
