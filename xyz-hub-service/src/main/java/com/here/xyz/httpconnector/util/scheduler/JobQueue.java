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
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.*;

public abstract class JobQueue implements Runnable {
    protected static final Logger logger = LogManager.getLogger();

    /** Queue for import and export Jobs*/
    private volatile static HashSet<Job> JOB_QUEUE = new HashSet<>();

    private volatile HashMap<String, RDSStatus> RDS_STATUS_MAP = new HashMap<>();

    protected boolean commenced = false;
    protected ScheduledFuture<?> executionHandle;

    public static int CORE_POOL_SIZE = 30;

    protected abstract void process() throws InterruptedException, CannotDecodeException;

    protected abstract void validateJob(Job j);

    protected abstract void prepareJob(Job j);

    protected abstract void executeJob(Job j);

    protected abstract void finalizeJob(Job j0);

    protected void failJob(Job j, String errorDescription, String errorType){
        j.setStatus(Job.Status.failed);

        /** Overrides potentially existing values */
        if(errorDescription != null)
            j.setErrorDescription(errorDescription);
        if(errorType != null)
            j.setErrorType(errorType);

        CService.jobConfigClient.update(null , j)
                .onFailure(t -> logger.warn("JOB[{}] update failed!", j.getId()));
    };

    protected Future<Void> isProcessingOnRDSPossible(Integer i, Job job){
        Promise<Void> p = Promise.promise();

        /** Check how many jobs are currently running */
        if(i != null && i > CService.configuration.JOB_MAX_RUNNING_JOBS) {
            logger.info("Maximum number of parallel running Jobs reached!");
            p.complete();
            return p.future();
        }

        /** Those statuses are always possible to process */
        switch (job.getStatus()){

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

    public static Job hasJob(Job job){
        return JOB_QUEUE.stream()
                .filter(j -> j.getId().equalsIgnoreCase(job.getId()))
                .findAny()
                .orElse(null);
    }

    public static void addJob(Job job){
        logger.info("[{}] added to JobQueue!", job.getId());
        JOB_QUEUE.add(job);
    }

    public static void removeJob(Job job){
        logger.info("[{}] removed from JobQueue!", job.getId());
        JOB_QUEUE.remove(job);
    }

    public static String checkRunningImportJobsOnSpace(String targetSpaceId){
        for (Job j : JOB_QUEUE ) {
            if(targetSpaceId != null  && j.getTargetSpaceId() != null
                && targetSpaceId.equalsIgnoreCase(j.getTargetSpaceId())){
                return j.getId();
            }
        }
        return null;
    }

    public static HashSet<Job> getQueue(){
        return JOB_QUEUE;
    }

    public static void printQueue(){
        JOB_QUEUE.forEach(job -> logger.info(job.getId()));
    }

    protected static int queueSize(){
        return JOB_QUEUE.size();
    }

    protected Future<Job> updateJobStatus(Job j, Job.Status status){
        if(status != null)
            j.setStatus(status);

        return CService.jobConfigClient.update(null , j)
                .onFailure(t -> {
                    logger.warn("Failed to update Job.",t);
                    Future.failedFuture(t);
                });
    }

    protected Future<Void> releaseReadOnlyLockFromSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", false), job.getTargetSpaceId())
                .onFailure(f -> {
                    job.setErrorDescription( Import.ERROR_DESCRIPTION_READONLY_MODE_FAILED);
                    updateJobStatus(job, Job.Status.failed);
                });
    }

    protected Future<Void> addReadOnlyLockToSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", true), job.getTargetSpaceId())
                .onFailure(f -> {
                    job.setErrorDescription( Import.ERROR_DESCRIPTION_READONLY_MODE_FAILED);
                    updateJobStatus(job,Job.Status.failed);
                });
    }

    protected boolean isTargetJDBCClientLoaded(Job job){
        if(JDBCImporter.getClient(job.getTargetConnector()) == null) {
            /** Maybe client is not initialized - in this case job need to get restarted */
            //TODO: Check if we can retry automatically (problem: missing ECPS)
            job.setErrorType(Import.ERROR_TYPE_NO_DB_CONNECTION);
            updateJobStatus(job, Job.Status.failed);
            return false;
        }
        return true;
    }
}
