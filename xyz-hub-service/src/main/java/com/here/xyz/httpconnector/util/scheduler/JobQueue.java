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
import com.here.xyz.httpconnector.config.JDBCClients;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

public abstract class JobQueue implements Runnable {
    protected static final Logger logger = LogManager.getLogger();

    protected static final String PROCESSING_NOT_POSSIBLE = "processing_not_possible";
    protected static final String CLIENT_MISSING = "client_missing";

    /** Queue for import and export Jobs*/
    private volatile static ArrayList<Job> JOB_QUEUE = new ArrayList<>();

    private volatile HashMap<String, RDSStatus> RDS_STATUS_MAP = new HashMap<>();

    protected boolean commenced = false;

    protected ScheduledFuture<?> executionHandle;

    public static int CORE_POOL_SIZE = 30;

    protected abstract void process() throws InterruptedException, CannotDecodeException;

    protected abstract Job validateJob(Job j);

    protected abstract void prepareJob(Job j);

    protected abstract boolean needRdsCheck(Job j0);

    protected Future<Void> isProcessingPossible(Job job){
        if(!needRdsCheck(job)){
            /** No RDS check needed - no database intensive stage. */
            return Future.succeededFuture();
        }
        return isProcessingOnRDSPossible(job);
    }

    protected Future<Job> loadCurrentConfig(Job job) {
        /** Check if JDBC Client is available */
        if (!isTargetJDBCClientLoaded(job)) {
            JDBCClients.addClientIfRequired(job.getTargetConnector());
            return Future.failedFuture(CLIENT_MISSING);
        }

        return CService.jobConfigClient.get(null, job.getId())
                .compose(currentJobConfig -> {
                    if (currentJobConfig == null) {
                        /** only corner-case - if deletion take more time as expected*/
                        removeJob(job);
                        return Future.failedFuture("[" + job.getId() + "] Cant find job-config");
                    }
                    return Future.succeededFuture(currentJobConfig);
                });
    }

    protected void logError(Throwable e, String jobId){
        if(e == null)
            return;
        if(e.getMessage() != null && e.getMessage().equalsIgnoreCase(PROCESSING_NOT_POSSIBLE))
            logger.info("job[{}] waits on free resources", jobId);
        else if(e.getMessage() != null &&  e.getMessage().equalsIgnoreCase(CLIENT_MISSING))
            logger.info("job[{}] free client_missing", jobId);
        else
            logger.warn("job[{}] ",jobId, e);
    }

    protected Future<Void> isProcessingOnRDSPossible(Job job){
        return JDBCImporter.getRDSStatus(job.getTargetConnector())
                .compose(rdsStatus -> {
                    RDS_STATUS_MAP.put(job.getTargetConnector(), rdsStatus);

                    if(rdsStatus.getCurrentMetrics().getCapacityUnits() > CService.configuration.JOB_MAX_RDS_CAPACITY) {
                        logger.info("job[{}] JOB_MAX_RDS_CAPACITY to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getCapacityUnits(), CService.configuration.JOB_MAX_RDS_CAPACITY);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }else if(rdsStatus.getCurrentMetrics().getCpuLoad() > CService.configuration.JOB_MAX_RDS_CPU_LOAD) {
                        logger.info("job[{}] JOB_MAX_RDS_CPU_LOAD to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getCpuLoad(), CService.configuration.JOB_MAX_RDS_CPU_LOAD);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }
                    if(job instanceof Import && rdsStatus.getCurrentMetrics().getTotalInflightImportBytes() > CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES) {
                        logger.info("job[{}] JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getTotalInflightImportBytes(), CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }
                    if(job instanceof Import && rdsStatus.getCurrentMetrics().getTotalRunningIDXQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS) {
                        logger.info("job[{}] JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getTotalRunningIDXQueries(), CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }
                    if(job instanceof Import && rdsStatus.getCurrentMetrics().getTotalRunningImportQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES) {
                        logger.info("job[{}] JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getTotalRunningImportQueries(), CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }
                    if(job instanceof Export && rdsStatus.getCurrentMetrics().getTotalRunningExportQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES) {
                        logger.info("job[{}] JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES to high {} > {}", job.getId(), rdsStatus.getCurrentMetrics().getTotalRunningImportQueries(), CService.configuration.JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES);
                        return Future.failedFuture(PROCESSING_NOT_POSSIBLE);
                    }
                    return Future.succeededFuture();
                });
    }

    public static Job hasJob(Job job){
        return JOB_QUEUE.stream()
                .filter(j -> j.getId().equalsIgnoreCase(job.getId()))
                .findAny()
                .orElse(null);
    }

    public synchronized static void addJob(Job job){
        if(hasJob(job) == null) {
            logger.info("job[{}] added to JobQueue! {}", job.getId(), job);
            JOB_QUEUE.add(job);
        }else
            logger.info("job[{}] is already present in queue! {}", job.getId(), job);
    }

    public synchronized static void refreshJob(Job job){
        if(hasJob(job) != null) {
            JOB_QUEUE.remove(hasJob(job));
            JOB_QUEUE.add(job);
        }
    }

    public synchronized static void removeJob(Job job){
        logger.info("job[{}] removed from JobQueue! {}", job.getId(), job);
        if(hasJob(job) != null)
            JOB_QUEUE.remove(hasJob(job));
    }

    public synchronized static void abortAllJobs(){
        for(Job job :JobQueue.getQueue()){
            setJobFailed(job , Job.ERROR_TYPE_FAILED_DUE_RESTART , null);
        }
    }

    public static String checkRunningJobsOnSpace(String targetSpaceId){
        /** Check only for imports */
        for (Job j : JOB_QUEUE ) {
            if(targetSpaceId != null  && j.getTargetSpaceId() != null
                && targetSpaceId.equalsIgnoreCase(j.getTargetSpaceId())
                && j instanceof Import){
                return j.getId();
            }
        }
        return null;
    }

    public static ArrayList<Job> getQueue(){
        return JOB_QUEUE;
    }

    public static void printQueue(){
        JOB_QUEUE.forEach(job -> logger.info(job.getId()));
    }

    protected static int queueSize(){
        return JOB_QUEUE.size();
    }

    public static Future<Job> setJobFailed(Job j, String errorDescription, String errorType){
        return updateJobStatus(j, Job.Status.failed, errorDescription, errorType);
    }

    public static Future<Job> setJobAborted(Job j){
        removeJob(j);
        return updateJobStatus(j, Job.Status.aborted);
    }

    protected static Future<Job> updateJobStatus(Job j) {
        return updateJobStatus(j, j.getStatus(), j.getErrorDescription(), j.getErrorType());
    }

    public static Future<Job> updateJobStatus(Job j, Job.Status status) {
        return updateJobStatus(j, status, null, null);
    }

    protected static Future<Job> updateJobStatus(Job j, Job.Status status, String errorDescription, String errorType ){
        if(status != null)
            j.setStatus(status);
        if(errorType != null)
            j.setErrorType(errorType);
        if(errorDescription != null)
            j.setErrorDescription(errorDescription);

        /** All end-states */
        if(status.equals(Job.Status.failed) || status.equals(Job.Status.finalized)){
            if(j instanceof  Import)
                releaseReadOnlyLockFromSpace(j)
                    .onFailure(f -> {
                        /** Currently we are only logging this issue */
                        logger.warn("[{}] READONLY_RELEASE_FAILED!",j.getId());
                    });
            /** Remove job from queue */
            removeJob(j);
        }else{
            /** only for display purpose in system endpoint */
            refreshJob(j);
        }

        return CService.jobConfigClient.update(null , j);
    }

    protected static Future<Void> releaseReadOnlyLockFromSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", false), job.getTargetSpaceId());
    }

    protected Future<Void> addReadOnlyLockToSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", true), job.getTargetSpaceId());
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
