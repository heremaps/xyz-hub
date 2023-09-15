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

package com.here.xyz.httpconnector.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

import com.google.common.collect.ImmutableMap;
import com.here.xyz.XyzSerializable.Mappers;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JobConfigClient;
import com.here.xyz.httpconnector.rest.HApiParam.HQuery.Command;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Difference.DiffMap;
import com.here.xyz.hub.util.diff.Patcher;
import io.vertx.core.Future;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class JobHandler {
    private static final Logger logger = LogManager.getLogger();
    private static List<String> MODIFICATION_WHITELIST = Arrays.asList("description", "csvFormat");
    private static Map<String,String> MODIFICATION_IGNORE_MAP = ImmutableMap.of(
        "createdAt","createdAt",
        "updatedAt","updatedAt"/*,
        "status", "status"*/);

    public static Future<Job> loadJob(String jobId, Marker marker) {
        return CService.jobConfigClient.get(marker, jobId)
            .compose(loadedJob -> {
                if (loadedJob == null)
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+jobId+"' does not exists!"));
                return Future.succeededFuture(loadedJob);
            })
            .onFailure( e -> Future.failedFuture(new HttpException(BAD_GATEWAY, "Can't load '" + jobId + "' from backend!")));
    }

    public static Future<List<Job>> loadJobs(Marker marker, String type, Job.Status status, String targetSpaceId) {
        return CService.jobConfigClient.getList(marker, type, status, targetSpaceId)
            .onFailure( e -> Future.failedFuture(new HttpException(BAD_GATEWAY, "Can't load jobs from backend!")));
    }

    public static Future<List<Job>> loadJobs(Marker marker, Job.Status status, String key) {
        return CService.jobConfigClient.getList(marker, status, key, JobConfigClient.DatasetDirection.BOTH)
            .onFailure( e -> Future.failedFuture(new HttpException(BAD_GATEWAY, "Can't load jobs from backend!")));
    }

    public static Future<Job> postJob(Job job, Marker marker) {
        return CService.jobConfigClient.get(marker, job.getId())
            .compose(loadedJob -> {
                if (loadedJob != null)
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '" + job.getId() + "' already exists!"));
                else
                  return job.init()
                      .compose(j -> job.validate())
                      .compose(j -> CService.jobConfigClient.store(marker, job));
            });
    }

  public static Future<Job> patchJob(Job job, Marker marker) {
      return loadJob(job.getId(), marker)
          .compose(loadedJob -> {
              Map oldJobMap = asMap(loadedJob);
              DiffMap diffMap = (DiffMap) Patcher.calculateDifferenceOfPartialUpdate(oldJobMap, asMap(job), MODIFICATION_IGNORE_MAP,
                  true);

              if (diffMap == null)
                  return Future.succeededFuture(loadedJob);
              else {
                  try {
                      validateChanges(diffMap);
                      Patcher.patch(oldJobMap, diffMap);
                      loadedJob = asJob(marker, oldJobMap);

                      //Store patched Config
                      return CService.jobConfigClient.update(marker, loadedJob);
                  }
                  catch (HttpException e) {
                      return Future.failedFuture(e);
                  }
              }
          });
    }

    public static Future<Job> deleteJob(String jobId, boolean force, Marker marker) {
        return loadJob(jobId, marker)
            .compose(job -> {
                if (!Job.isValidForDelete(job, force))
                    return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Job is not in end state - current status: "+ job.getStatus()) );
                else {
                    if (force) {
                        //In force mode abort running SQLs
                        return job.abortIfPossible()
                                .onSuccess(f -> {
                                    //Clean S3 Job Folder
                                    CService.jobS3Client.cleanJobData(job, force);
                                }).compose(f -> CService.jobConfigClient.delete(marker, job.getId(), force));
                    }
                    else
                        return CService.jobConfigClient.delete(marker, job.getId(), force);
                }
            });
    }

    public static Future<Job> executeCommand(String jobId, Command command, int urlCount, Marker marker) {
        return loadJob(jobId, marker)
            .compose(job -> switch (command) {
              case ABORT -> job.executeAbort();
              case CREATEUPLOADURL -> job.executeCreateUploadUrl(urlCount);
              case RETRY -> job.executeRetry();
              case START -> job.executeStart();
            });
    }

  private static Map asMap(Object object) {
        try {
            return Mappers.DEFAULT_MAPPER.get().convertValue(object, Map.class);
        }
        catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static Job asJob(Marker marker, Map object) throws HttpException {
        try {
            return Mappers.DEFAULT_MAPPER.get().convertValue(object, Job.class);
        }
        catch (Exception e) {
            logger.error(marker, "Could not convert resource.", e.getCause());
            throw new HttpException(BAD_GATEWAY, "Could not convert resource.");
        }
    }

    private static void validateChanges(Difference.DiffMap diffMap) throws HttpException{
        //Check if modification is allowed
        Set<Object> objects = diffMap.keySet();
        for (Object key : objects) {
            if (MODIFICATION_WHITELIST.indexOf((String) key) == -1)
                throw new HttpException(BAD_REQUEST, "The property '" + key + "' is immutable!");
        }
    }
}
