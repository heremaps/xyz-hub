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

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.validate.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.io.IOException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class ImportHandler extends JobHandler{
    private static final Logger logger = LogManager.getLogger();

    protected static Future<Job> postJob(Import job, Marker marker){
        try{
            ImportValidator.setImportDefaults(job);
            ImportValidator.validateImportCreation(job);
        }catch (Exception e){
            return Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage()));
        }

        return HubWebClient.getSpaceStatistics(job.getTargetSpaceId())
                .compose( statistics-> {
                    Long value = statistics.getCount().getValue();
                    if(value != null && value != 0) {
                        return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Layer is not empty!"));
                    }
                    return CService.jobConfigClient.store(marker, job);
                });
    }

    protected static Future<Job> execute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                         boolean enableHashedSpaceId, boolean enableUUID, int urlCount, Marker marker){

        /** At this point we only have jobs which are allowed for executions. */

        /** Load current JobConfig */
        return loadJob(jobId, marker)
                .compose(loadedJob -> {
                    Import importJob = (Import) loadedJob;
                    try {
                        /** Load DB-Client an inject config values */
                        loadClientAndInjectConfigValues(importJob, command, connectorId, ecps, passphrase, enableHashedSpaceId, enableUUID, null, null);
                        return Future.succeededFuture(importJob);
                    }catch (HttpException e){
                        return Future.failedFuture(e);
                    }
                }) .compose(importJob -> {
                    if(command.equals(HApiParam.HQuery.Command.ABORT)){
                        /** Job will fail - because SQL Queries are getting terminated */
                        return abortJob(importJob)
                                .compose(job -> Future.succeededFuture(importJob));
                    }
                    return Future.succeededFuture(importJob);
                })
                .compose(importJob -> {
                    if(command.equals(HApiParam.HQuery.Command.CREATEUPLOADURL)){
                        try {
                            for (int i = 0; i < urlCount; i++) {
                                importJob.addImportObject(CService.jobS3Client.generateUploadURL(importJob));
                            }

                            /** Store Urls in Job-Config */
                            return CService.jobConfigClient.update(marker, importJob);
                        }catch (IOException e){
                            logger.error(marker, "[{}] Can`t create S3 Upload-URL(s)",importJob.getId(), e);
                            return Future.failedFuture(new HttpException(BAD_GATEWAY, "Can`t create S3 Upload-URL(s)"));
                        }
                    }
                    return Future.succeededFuture(importJob);
                })
                .compose(importJob -> {
                    if(command.equals(HApiParam.HQuery.Command.RETRY)){
                        /** Reset to Current State */
                        try {
                            importJob.resetToPreviousState();
                            return CService.jobConfigClient.update(marker, importJob)
                                    .onSuccess(f -> checkAndAddJobToQueueFuture(marker, importJob, command));
                        } catch (Exception e) {
                            return Future.failedFuture(new HttpException(BAD_REQUEST, "Job has no lastStatus - cant retry!"));
                        }
                    }
                    return Future.succeededFuture(importJob);
                })
                .compose(importJob -> {
                    if(command.equals(HApiParam.HQuery.Command.START)){
                        return checkAndAddJobToQueueFuture(marker, importJob, command);
                    }
                    return Future.succeededFuture(importJob);
                });
    }

    protected static Future<String> checkRunningImportJobs(Marker marker, Job job, HApiParam.HQuery.Command command){
        /** Check in node memory */
        String jobId = CService.importQueue.checkRunningJobsOnSpace(job.getTargetSpaceId());

        if(jobId != null) {
            return Future.succeededFuture(jobId);
        }else{
            if(command.equals(HApiParam.HQuery.Command.RETRY))
                return Future.succeededFuture(null);
            /** Check if other import is running on target */
            return CService.jobConfigClient.getRunningJobsOnSpace(marker, job.getTargetSpaceId(), Job.Type.Import);
        }
    }

    protected static Future<Job> checkAndAddJobToQueueFuture(Marker marker, Job job, HApiParam.HQuery.Command command){
        /** States are getting set in JobQueue */

        return checkRunningImportJobs(marker, job, command)
                .compose( runningJobs -> {
                    if(runningJobs == null){
                        CService.importQueue.addJob(job);
                        return Future.succeededFuture(job);
                    }else{
                        return  Future.failedFuture(new HttpException(CONFLICT, "Job '"+runningJobs+"' is already running on target!"));
                    }
                });
    }
}
