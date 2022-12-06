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
package com.here.xyz.httpconnector.util.scheduler;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Job-Queue for IMPORT-Jobs (S3 -> RDS)
 */
public class ImportQueue extends JobQueue{
    private static final Logger logger = LogManager.getLogger();
    public static volatile long NODE_EXECUTED_IMPORT_MEMORY;
    public static volatile HashMap<String, RDSStatus> RDS_STATUS_MAP = new HashMap<>();

    protected Future<Void> isProcessingOnRDSPossible(Integer i, Job job){
        Promise<Void> p = Promise.promise();

        /** Check how many jobs are currently running */
        if(i != null && i > CService.configuration.JOB_MAX_RUNNING_JOBS) {
            logger.info("Maximum number of parallel running Jobs reached!");
            p.complete();
            return p.future();
        }

        /** Those statuses are always possible to process */
        switch (job.status){

            case queued:
            case prepared:
            case executed:
            case aborted:
                break;
            default:
                /** for all other states it's not relevant to check RDS resources */
                //executing,preparing,validating,finalizing, finalized,partially_failed,failed,waiting,validated
                p.complete();
                return p.future();
        }

        logger.info("[{}] - Check if execution on RDS is possible.",job.getId());

        JDBCImporter.getRDSStatus(job.getTargetConnector())
                .onSuccess(rdsStatus -> {
                    RDS_STATUS_MAP.put(job.getTargetConnector(), rdsStatus);

                    if(rdsStatus.getCurrentMetrics().getCapacityUnits() > CService.configuration.JOB_MAX_RDS_CAPACITY) {
                        p.fail("JOB_MAX_RDS_CAPACITY to high "+rdsStatus.getCurrentMetrics().getCapacityUnits()+" >"+CService.configuration.JOB_MAX_RDS_CAPACITY);
                        return;
                    }
                    if(rdsStatus.getCurrentMetrics().getCpuLoad() > CService.configuration.JOB_MAX_RDS_CPU_LOAD) {
                        p.fail("JOB_MAX_RDS_CPU_LOAD to high "+rdsStatus.getCurrentMetrics().getCpuLoad()+" > "+CService.configuration.JOB_MAX_RDS_CPU_LOAD);
                        return;
                    }
                    if(rdsStatus.getCurrentMetrics().getTotalInflightImportBytes() > CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES) {
                        p.fail("JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES to high "+rdsStatus.getCurrentMetrics().getTotalInflightImportBytes()+" > "+CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES);
                        return;
                    }
                    if(rdsStatus.getCurrentMetrics().getTotalRunningIDXQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS) {
                        p.fail("JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS to high "+rdsStatus.getCurrentMetrics().getTotalInflightImportBytes()+" > "+CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS);
                        return;
                    }
                    if(rdsStatus.getCurrentMetrics().getTotalRunningImportQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORTS) {
                        p.fail("JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS to high "+rdsStatus.getCurrentMetrics().getTotalRunningImportQueries()+" > "+CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORTS);
                        return;
                    }
                    p.complete();
                }).onFailure(f -> {
                    p.fail("Cant get RDS Status! "+f.getMessage());
                });

        return p.future();
    }

    protected void process() throws InterruptedException, CannotDecodeException {
        //@TODO CleanUp Jobs: Check all Jobs which are finished/failed and delete them after some time.
        //@TODO Lost Jobs: Find possible orphaned Jobs (system crash) and add them to the queue again

        HashSet<Job> queue = getQueue();

        if (!queue.isEmpty()){
            int i = 0;
            for(Job job : getQueue()){
                /** Check Capacity */
                isProcessingOnRDSPossible(i++, job)
                        .onSuccess(ff -> {
                            /** Run first Job Queue (FIFO) */
                            Import importJob = (Import) job;

                            /**
                             * Job-Life-Cycle:
                             * waiting -> (validating) -> validated ->  queued -> (preparing) -> prepared -> (executing) -> executed -> (finalizing) -> finalized
                             * all stages can end up in failed
                             **/

                            if(JDBCImporter.getClient(importJob.getTargetConnector()) == null) {
                                /** Maybe client is not initialized - in this case job need to get restarted */
                                //TODO: Check if we can retry automatically (problem: missing ECPS)
                                importJob.setErrorType(Import.ERROR_TYPE_NO_DB_CONNECTION);
                                updateJobStatus(importJob, Job.Status.failed);
                                return;
                            }

                            switch (importJob.status){
                                case finalized:
                                    logger.info("JOB[{}] is finalized!",importJob.id);
                                    removeJob(importJob);
                                    break;
                                case partially_failed:
                                case failed:
                                    logger.info("JOB[{}] has failed!",importJob.id);
                                    /** Remove Job from Queue - in some cases the user is able to retry */
                                    removeJob(importJob);
                                    break;
                                case waiting:
                                    updateJobStatus(importJob,Job.Status.validating)
                                            .onSuccess(f -> validateJob(importJob));
                                    break;
                                case validated:
                                    updateJobStatus(importJob,Job.Status.queued);
                                    break;
                                case queued:
                                    updateJobStatus(importJob,Job.Status.preparing)
                                            .onSuccess(f -> prepareJob(importJob));
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
                                    logger.info("JOB[{}] is currently '{}' - current Queue-size: {}",importJob.id, importJob.status, queueSize());
                                }
                            }
                        })
                        .onFailure(e -> logger.info(e.getMessage()));
            }

        }else{
            logger.info("--- ImportQueue is empty -----------------");
        }
    }

    @Override
    protected void validateJob(Job job){
        ImportValidator.validateImportJob((Import)job);
        updateJobStatus((Import)job, null);
    }

    @Override
    protected void prepareJob(Job j){
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        CService.jdbcImporter.prepareImport(j.getTargetConnector(), defaultSchema,  j.getTargetTable(), ((Import)j).isEnabledUUID(),  j.getCsvFormat())
            .compose(
                f2 -> {
                    j.setStatus(Job.Status.prepared);
                    return CService.jobConfigClient.update(null, j);
                })
            .onFailure(f -> {
                    logger.warn("JOB[{}] preparation has failed!", j.getId(), f);
                    j.setErrorType(Import.ERROR_TYPE_PREPARATION_FAILED);

                    if(f instanceof PgException && ((PgException)f).getCode() != null) {
                        if (((PgException)f).getCode().equalsIgnoreCase("42P01")) {
                            logger.info("TargetTable '"+j.getTargetTable()+"' does not Exists!");
                            j.setErrorDescription(Import.ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS);
                            failJob(j);
                            return;
                        }
                    }
                    //@TODO: Collect possible error-cases
                    j.setErrorDescription( Import.ERROR_DESCRIPTION_UNEXPECTED);
                    failJob(j);
                });
    }

    @Override
    protected void executeJob(Job j){
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

            logger.info("IMPORT_MEMORY {}/{} = {}% of max", NODE_EXECUTED_IMPORT_MEMORY, (maxMemInGB * 1024 * 1024 * 1024) , (NODE_EXECUTED_IMPORT_MEMORY/ (maxMemInGB * 1024 * 1024 * 1024)));

            //TODO: Also view RDS METRICS?
            //if(NODE_EXECUTED_IMPORT_MEMORY < (maxMemInGB * 1024 * 1024 * 1024)){
            if(NODE_EXECUTED_IMPORT_MEMORY < CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES){
                importObjects.get(key).setStatus(ImportObject.Status.processing);
                NODE_EXECUTED_IMPORT_MEMORY += curFileSize;
                logger.info("JOB[{}] start execution of {}! mem: {}", j.getId(), importObjects.get(key).getS3Key(), NODE_EXECUTED_IMPORT_MEMORY);

                importFutures.add(
                        CService.jdbcImporter.executeImport(j.getTargetConnector(), defaultSchema, j.getTargetTable(),
                                        CService.configuration.JOBS_S3_BUCKET, importObjects.get(key).getS3Key(), CService.configuration.JOBS_S3_BUCKET_REGION, curFileSize, j.getCsvFormat() )
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
                                            logger.warn("JOB[{}] Import of '{}' failed ", j.getId(), importObjects.get(key), f);
                                            importObjects.get(key).setStatus(ImportObject.Status.failed);
                                        }
                                )
                );
            }else {
                logger.info("[{}] - queue {} - mem {} ",j.getId(), key, NODE_EXECUTED_IMPORT_MEMORY);
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

                            if(cntInvalid == 0)
                                j.setStatus(Job.Status.executed);
                            else if(cntInvalid == importObjects.size()) {
                                j.setErrorType(Import.ERROR_TYPE_EXECUTION_FAILED);
                                j.setErrorDescription(Import.ERROR_DESCRIPTION_ALL_IMPORTS_FAILED);
                                j.setStatus(Job.Status.failed);
                            }
                            else if(cntInvalid < importObjects.size()) {
                                //TODO: Partially failed - How to deal with finalization?
                                j.setErrorDescription(Import.ERROR_DESCRIPTION_IMPORTS_PARTIALLY_FAILED);
                                j.setStatus(Job.Status.partially_failed);
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
                    j.setErrorType(Import.ERROR_TYPE_FINALIZATION_FAILED);
                    failJob(j);
                }).compose(
                    f2 -> {
                        logger.info("JOB[{}] finalization finished!", j.getId());
                        if(j.getErrorDescription() != null)
                            j.setStatus(Job.Status.failed);
                        else
                            j.setStatus(Job.Status.finalized);

                        return CService.jobConfigClient.update(null, j);
                });
    }

    @Override
    protected void failJob(Job j){
        //TODO: set TTL
        j.setStatus(Job.Status.failed);

        CService.jobConfigClient.update(null , j)
                .onFailure(t -> logger.warn("JOB[{}] update failed!", j.getId()))
                .onSuccess(s -> { removeJob(j); return; });
    }

    protected Future<Future> updateJobStatus(Import j, Job.Status status){
        Promise<Future> p = Promise.promise();
        if(status != null)
            j.setStatus(status);

        CService.jobConfigClient.update(null , j)
                .onFailure(t -> {
                    logger.warn("Failed to update Job.",t);
                    p.fail(t);
                })
                .onSuccess(f -> {
                    p.complete();
                });
        return p.future();
    }
}
