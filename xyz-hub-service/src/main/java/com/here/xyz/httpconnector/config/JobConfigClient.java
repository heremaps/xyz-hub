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
                        logger.debug(marker, "job[{}]: not found!", jobId);
                    }
                })
                .onFailure(e -> logger.error(marker, "job[{}]: failed to load! ", jobId, e));
    }

    public Future<List<Job>> getList(Marker marker, Job.Type type, Job.Status status, String targetSpaceId) {
        return getJobs(marker, type, status, targetSpaceId)
                .onFailure(e -> logger.error(marker, "Failed to load jobList! ", e));
    }

    public Future<String> getRunningJobsOnSpace(Marker marker, String targetSpaceId, Job.Type type) {
        return findRunningJobOnSpace(marker, targetSpaceId, type)
                .onFailure(e -> logger.error(marker, "Failed to load jobList! ", e ));
    }

    public Future<Job> update(Marker marker, Job job) {
        /** We are updating jobs in JobHandler (config changes + JobQueue (state changes)*/
        job.setUpdatedAt(Core.currentTimeMillis() / 1000L);

        return storeJob(marker, job, true)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}] / status[{}]: successfully updated!", job.getId(), job.getStatus());
                })
                .onFailure(e -> {
                    logger.error(marker, "job[{}]: failed to update! ", job.getId(), e);
                });
    }

    public Future<Job> store(Marker marker, Job job) {
        /** A newly created Job waits for an execution */
        if(job.getStatus() == null)
            job.setStatus(Job.Status.waiting);

        return storeJob(marker, job, false)
                .onFailure(e -> {
                    logger.error(marker, "job[{}]: failed to store! ", job.getId(), e);
                });
    }

    public Future<Job> delete(Marker marker, String jobId, boolean force) {
        return getJob(marker, jobId)
                .onSuccess(j -> {
                            if (j == null) {
                                logger.debug(marker, "job[{}]: nothing to delete!", jobId);
                            }else {
                                deleteJob(marker, j)
                                        .onFailure(e -> logger.error(marker, "job[{}]: failed delete! ", jobId, e));
                            }
                })
                .onFailure(e -> logger.error(marker, "job[{}]: failed to delete! ", jobId, e));
    }

    protected abstract Future<Job> getJob(Marker marker, String jobId);

    protected abstract Future<List<Job>> getJobs( Marker marker, Job.Type type, Job.Status status, String targetSpaceId);

    protected abstract Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, Job.Type type);

    protected abstract Future<Job> storeJob(Marker marker, Job job, boolean isUpdate);

    protected abstract Future<Job> deleteJob(Marker marker, Job job);

}
