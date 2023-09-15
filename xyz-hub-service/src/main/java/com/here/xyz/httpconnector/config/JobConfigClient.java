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
import com.here.xyz.httpconnector.util.jobs.Job.Status;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.config.Initializable;
import io.vertx.core.Future;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * Client for reading and writing Jobs
 */
public abstract class JobConfigClient implements Initializable {

    private static final Logger logger = LogManager.getLogger();

    public enum DatasetDirection {
      SOURCE,
      TARGET,
      BOTH
    }

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

    public Future<List<Job>> getList(Marker marker, String type, Status status, String targetSpaceId) {
        return getJobs(marker, type, status, targetSpaceId)
                .onFailure(e -> logger.error(marker, "Failed to load jobList! ", e));
    }

  /**
   * Returns a list of all jobs which are having the dataset, specified by type and key, as target, source or both.
   * @param marker
   * @param key The dataset key
   * @param direction Find all jobs which have the dataset as target, source or both
   * @return All jobs which are reading and or writing into the specified dataset description
   */
    public Future<List<Job>> getList(Marker marker, Status status, String key, DatasetDirection direction) {
      //TODO: implement by searching for the key in the stored key of the dataset description. The key will be stored as part of the DatasetDescription within the "key" property @see DatasetDescription#getKey()
      return getJobs(marker, status, key, direction)
              .onFailure(e -> logger.error(marker, "Failed to load jobList! ", e));
    }

    public Future<String> getRunningJobsOnSpace(Marker marker, String targetSpaceId, String type) {
        return findRunningJobOnSpace(marker, targetSpaceId, type)
                .onFailure(e -> logger.error(marker, "Failed to load jobList! ", e ));
    }

    public Future<Job> update(Marker marker, Job job) {
        /** We are updating jobs in JobHandler (config changes + JobQueue (state changes)*/
        job.setUpdatedAt(Core.currentTimeMillis() / 1000L);
      if (job.isChildJob())
        return Future.succeededFuture(job);//TODO: Replace that hack once the scheduler flow was refactored

        return storeJob(marker, job, true)
                .onSuccess(v -> {
                    logger.info(marker, "job[{}] / status[{}]: successfully updated!", job.getId(), job.getStatus());
                })
                .onFailure(e -> {
                    logger.error(marker, "job[{}]: failed to update! ", job.getId(), e);
                });
    }

    public Future<Job> store(Marker marker, Job job) {
      if (job.isChildJob())
        return Future.succeededFuture(job);//TODO: Replace that hack once the scheduler flow was refactored

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

    protected abstract Future<List<Job>> getJobs(Marker marker, String type, Status status, String targetSpaceId);
    protected abstract Future<List<Job>> getJobs(Marker marker, Status status, String key, DatasetDirection direction);

    protected abstract Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, String type);

    protected abstract Future<Job> storeJob(Marker marker, Job job, boolean isUpdate);

    protected abstract Future<Job> deleteJob(Marker marker, Job job);

}
