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

import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.aborted;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCClients;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.concurrent.ScheduledFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JobQueue implements Runnable {
    protected static final Logger logger = LogManager.getLogger();

    //Queue for import and export Jobs
    private volatile static ArrayList<Job> JOB_QUEUE = new ArrayList<>();

    protected boolean commenced = false;

    protected ScheduledFuture<?> executionHandle;

    public static int CORE_POOL_SIZE = 30;

    protected abstract void process() throws InterruptedException, CannotDecodeException;

    protected abstract Job validateJob(Job j);

    protected abstract void prepareJob(Job j);



    protected synchronized Future<Job> loadCurrentConfig(Job job) {
        return CService.jobConfigClient.get(null, job.getId())
            .compose(currentJobConfig -> {
                if (currentJobConfig == null) {
                    //Only corner-case - if deletion take more time as expected
                    removeJob(job);
                    return Future.failedFuture("[" + job.getId() + "] Cant find job-config");
                }
                return Future.succeededFuture(currentJobConfig);
            });
    }

    protected void logError(Throwable e, String jobId) {
        if (e == null)
            return;
        if (e instanceof Job.ProcessingNotPossibleException)
            logger.info("job[{}] {}", jobId, e.getMessage());
        else
            logger.warn("job[{}] ", jobId, e);
    }

    public synchronized static Job hasJob(Job job) {
        return JOB_QUEUE.stream()
                .filter(j -> j.getId().equalsIgnoreCase(job.getId()))
                .findAny()
                .orElse(null);
    }

    public synchronized static void addJob(Job job) {
        if (hasJob(job) == null) {
            logger.info("job[{}] added to JobQueue! {}", job.getId(), job);
            if(job.getTargetConnector() != null)
                JDBCClients.addClientsIfRequired(job.getTargetConnector());
            JOB_QUEUE.add(job);
        }
        else
            logger.info("job[{}] is already present in queue! {}", job.getId(), job);
    }

    private synchronized static void refreshJob(Job job) {
        if (hasJob(job) != null) {
            JOB_QUEUE.remove(hasJob(job));
            JOB_QUEUE.add(job);
        }
    }

    public synchronized static void removeJob(Job job) {
        logger.info("job[{}] removed from JobQueue! {}", job.getId(), job);
        if (hasJob(job) != null)
            JOB_QUEUE.remove(hasJob(job));
    }

    public synchronized static void abortAllJobs() {
        for (Job job :JobQueue.getQueue()) {
            setJobFailed(job , Job.ERROR_TYPE_FAILED_DUE_RESTART , null);
        }
    }

    public static String checkRunningJobsOnSpace(String targetSpaceId) {
        //Check only for imports
        for (Job j : JOB_QUEUE ) {
            if(targetSpaceId != null  && j.getTargetSpaceId() != null
                && targetSpaceId.equalsIgnoreCase(j.getTargetSpaceId())
                && j instanceof Import){
                return j.getId();
            }
        }
        return null;
    }

    public synchronized static ArrayList<Job> getQueue() {
        return JOB_QUEUE;
    }

    public static void printQueue() {
        JOB_QUEUE.forEach(job -> logger.info(job.getId()));
    }

    protected static int queueSize() {
        return JOB_QUEUE.size();
    }

    public static Future<Job> setJobFailed(Job job, String errorDescription, String errorType){
        logger.info("job[{}] has failed!", job.getId());
        return updateJobStatus(job, failed, errorDescription, errorType);
    }

    public static Future<Job> setJobAborted(Job j) {
        return updateJobStatus(j, Job.Status.aborted);
    }

    protected static Future<Job> updateJobStatus(Job j) {
        return updateJobStatus(j, j.getStatus(), j.getErrorDescription(), j.getErrorType());
    }

    public static Future<Job> updateJobStatus(Job j, Job.Status status) {
        return updateJobStatus(j, status, null, null);
    }

    protected static Future<Job> updateJobStatus(Job job, Job.Status status, String errorDescription, String errorType) {
        if(hasJob(job) == null) {
            logger.warn("[{}] Job is already removed from queue, dont update status {}!", job.getId(), job.getStatus());
            return Future.failedFuture("Job " + job.getId() + " is not in queue anymore!");
        }

        if (status != null)
            job.setStatus(status);
        if (errorType != null)
            job.setErrorType(errorType);
        if (errorDescription != null)
            job.setErrorDescription(errorDescription);

        //All end-states
        if (status.equals(failed) || status.equals(finalized) || status.equals(aborted)) {
            //FIXME: Shouldn't that be done also for status == aborted? If yes, we can simply use status.isFinal()
            if (job instanceof Import) {
                releaseReadOnlyLockFromSpace(job)
                        .onFailure(f -> {
                            //Currently we are only logging this issue
                            logger.warn("[{}] READONLY_RELEASE_FAILED!", job.getId());
                        });
            }
            //Remove job from queue
            removeJob(job);
        }
        else
            //Only for display purpose in system endpoint
            refreshJob(job);

        return CService.jobConfigClient.update(null, job);
    }

    protected static Future<Void> releaseReadOnlyLockFromSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", false), job.getTargetSpaceId());
    }

    protected Future<Void> addReadOnlyLockToSpace(Job job){
        return HubWebClient.updateSpaceConfig(new JsonObject().put("readOnly", true), job.getTargetSpaceId());
    }

}
