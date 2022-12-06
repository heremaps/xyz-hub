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
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client for reading and writing Jobs
 */
public abstract class JobConfigClient implements Initializable {

    private static final Logger logger = LogManager.getLogger();

    public static final ExpiringMap<String, Job> cache = ExpiringMap.builder()
            .expirationPolicy(ExpirationPolicy.CREATED)
            .expiration(10, TimeUnit.MINUTES)
            .build();

    public static JobConfigClient getInstance() {
        if (CService.configuration.JOBS_DYNAMODB_TABLE_ARN != null) {
            return new DynamoJobConfigClient(CService.configuration.JOBS_DYNAMODB_TABLE_ARN);
        } else {
            return JobConfigClient.getInstance();
        }
    }

    public Future<Job> get(Marker marker, String jobId) {
        Job cached = cache.get(jobId);
        if (cached != null) {
            logger.info(marker, "space[{}]: loaded from cache", jobId);
            return Future.succeededFuture(cached);
        }

        return getJob(marker, jobId)
                .onSuccess(job -> {
                    if (job == null) {
                        logger.info(marker, "Get - job[{}]: not found!", jobId);
                    }else {
                        logger.info(marker, "job[{}]: successfully loaded!", jobId);
                        cache.put(jobId, job);
                    }
                })
                .onFailure(t -> logger.error(marker, "job[{}]: failed to load!", jobId, t));
    }

    public Future<List<Job>> getList(Marker marker, Job.Type type, Job.Status status) {
        return getJobs(marker, type, status)
                .onSuccess(jobList -> {
                    for (Job job: jobList) {
                        cache.put(job.getId(), job);
                    }
                    logger.info(marker, "Successfully loaded '{}' jobs!", jobList.size());
                })
                .onFailure(t -> logger.error(marker, "Failed to load jobList!", t));
    }


    public Future<Job> update(Marker marker, Job job) {
        job.updatedAt = Core.currentTimeMillis();

        return storeJob(marker, job)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}] / status[{}]: successfully updated!", job.getId(), job.getStatus());
                    cache.remove(job.getId());
                })
                .onFailure(t -> {
                    logger.error(marker, "job[{}]: failed to update!", job.getId(), t);
                });
    }

    public Future<Job> store(Marker marker, Job job) {
        return storeJob(marker, job)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}]: successfully stored!", job.getId());
                    cache.remove(job.getId());
                })
                .onFailure(t -> {
                    logger.error(marker, "job[{}]: failed to store!", job.getId(), t);
                });
    }

    public Future<Job> delete(Marker marker, String jobId) {
        return deleteJob(marker, jobId)
                .onSuccess(job -> {
                    if (job == null) {
                        logger.info(marker, "jobId[{}]: not found. Nothing to delete!", jobId);
                    }else {
                        logger.info(marker, "job[{}]: successfully deleted!", jobId);
                        cache.remove(jobId);
                    }
                })
                .onFailure(t -> logger.error(marker, "job[{}]: Failed delete job:", jobId, t));
    }

    protected abstract Future<Job> getJob(Marker marker, String jobId);

    protected abstract Future<List<Job>> getJobs( Marker marker, Job.Type type, Job.Status status);

    protected abstract Future<Job> storeJob(Marker marker, Job job);

    protected abstract Future<Job> deleteJob(Marker marker, String jobId);
}
