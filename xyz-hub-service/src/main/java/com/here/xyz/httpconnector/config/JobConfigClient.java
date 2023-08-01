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

package com.here.xyz.httpconnector.config;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.config.Initializable;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;

/**
 * Client for reading and writing Jobs
 */
public abstract class JobConfigClient implements Initializable {

    private static final Logger logger = LogManager.getLogger();

    public static JobConfigClient getInstance() {
        if (CService.configuration.JOBS_DYNAMODB_TABLE_ARN != null) {
            return new DynamoJobConfigClient(CService.configuration.JOBS_DYNAMODB_TABLE_ARN);
        } else {
            return JDBCJobConfigClient.getInstance();
        }
    }

    public Future<Job> get(Marker marker, String jobId) {
        return getJob(marker, jobId)
                .onSuccess(job -> {
                    if (job == null) {
                        logger.info(marker, "Get - job[{}]: not found!", jobId);
                    }else {
                        logger.info(marker, "job[{}]: successfully loaded!", jobId);
                    }
                })
                .onFailure(t -> logger.error(marker, "job[{}]: failed to load!", jobId, t));
    }

    public Future<List<Job>> getList(Marker marker, Job.Type type, Job.Status status, String targetSpaceId) {
        return getJobs(marker, type, status, targetSpaceId)
                .onSuccess(jobList -> {
                    logger.info(marker, "Successfully loaded '{}' jobs!", jobList.size());
                })
                .onFailure(t -> logger.error(marker, "Failed to load jobList!", t));
    }

    public Future<String> getRunningJobsOnSpace(Marker marker, String targetSpaceId, Job.Type type) {
        return findRunningJobOnSpace(marker, targetSpaceId, type)
                .onSuccess(jobList -> {
                    logger.info(marker, "Successfully loaded '{}' jobs!");
                })
                .onFailure(t -> logger.error(marker, "Failed to load jobList!", t));
    }

    /**
     * Set <i>stateUpdate</i> to 'true', when the state of the job is changed.
     * This triggers state update notification, defined in <i>stateUpdateUrl</i>
     */
    public Future<Job> update(Marker marker, Job job, boolean stateUpdate) {
        return update(marker, job)
                .onSuccess(j -> {
                    if(stateUpdate && job.getStateUpdateUrl() != null )
                        notifyStateUpdate(job);
                });
    }

    public Future<Job> update(Marker marker, Job job) {
        job.setUpdatedAt(Core.currentTimeMillis() / 1000L);

        return storeJob(marker, job, true)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}] / status[{}]: successfully updated!", job.getId(), job.getStatus());
                })
                .onFailure(t -> {
                    logger.error(marker, "job[{}]: failed to update!", job.getId(), t);
                });
    }

    public Future<Job> store(Marker marker, Job job) {
        /** A newly created Job waits for an execution */
        if(job.getStatus() == null)
            job.setStatus(Job.Status.waiting);

        return storeJob(marker, job, false)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}]: successfully stored!", job.getId());
                })
                .onFailure(t -> {
                    logger.error(marker, "job[{}]: failed to store!", job.getId(), t);
                });
    }

    public Future<Job> delete(Marker marker, String jobId, boolean force) {
        return getJob(marker, jobId)
                .onSuccess(j -> {
                            if (j == null) {
                                logger.info(marker, "jobId[{}]: not found. Nothing to delete!", jobId);
                            }else if ( !isValidForDelete(j, force) ) {
                                logger.info(marker, "jobId[{}]: not in end state. Nothing to delete!", jobId);
                            } else {     
                                deleteJob(marker, j)
                                        .onSuccess(job -> logger.info(marker, "job[{}]: successfully deleted!", jobId))
                                        .onFailure(t -> logger.error(marker, "job[{}]: Failed delete job:", jobId, t));
                            }
                })
                .onFailure(t -> logger.error(marker, "job[{}]: Failed delete job:", jobId, t));
    }

    /**
     * Executes state update trigger when <i>stateUpdateUrl</i> is provided in the Job <br>
     * Performs {@code 'POST <stateUpdateUrl>?jobId=<jobId>&jobStatus=<status>'}
     */
    private void notifyStateUpdate(Job job) {
        logger.info("Triggering job state update [{}] - {} ", job.getStatus(), job.getStateUpdateUrl());
        CService.webClient.postAbs(job.getStateUpdateUrl())
                .addQueryParam("jobId", job.getId())
                .addQueryParam("jobStatus", job.getStatus().toString())
                .send()
                .onComplete(res -> {
                    if(res.succeeded() && res.result().statusCode() > 300)
                        logger.warn("Failed to notify state update due to HTTP error: [{}] - {} ", res.result().statusCode(), res.result().bodyAsString());
                    else
                        logger.warn("Failed to notify state update", res.cause());
                });
    }

    protected abstract Future<Job> getJob(Marker marker, String jobId);

    protected abstract Future<List<Job>> getJobs( Marker marker, Job.Type type, Job.Status status, String targetSpaceId);

    protected abstract Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, Job.Type type);

    protected abstract Future<Job> storeJob(Marker marker, Job job, boolean isUpdate);

    protected abstract Future<Job> deleteJob(Marker marker, Job job);

    public boolean isValidForDelete(Job job, boolean force) {
        if(force)
            return true;
        switch (job.getStatus()){
            case waiting: case finalized: case aborted: case failed: return true;
            default: return false;
        }

    }

}
