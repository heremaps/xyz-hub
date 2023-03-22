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
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.io.IOException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class ImportHandler extends JobHandler{
    private static final Logger logger = LogManager.getLogger();

    public static Future<Job> postJob(Import job, Marker marker){
        try{
            ImportValidator.setImportDefaults(job);
            ImportValidator.validateImportCreation(job);
        }catch (Exception e){
            return Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage()));
        }

        return CService.jobConfigClient.get(marker, job.getId())
                .compose( loadedJob -> {
                    if(loadedJob == null)
                        return CService.jobConfigClient.store(marker, job);
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+job.getId()+"' already exists!"));
                })
                .compose( f -> HubWebClient.getSpaceStatistics(job.getTargetSpaceId()))
                .compose( statistics-> {
                    Long value = statistics.getCount().getValue();
                    if(value != null && value != 0) {
                        return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Layer is not empty!"));
                    }
                    return CService.jobConfigClient.store(marker, job);
                });
    }

    public static Future<Job> execute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                          boolean enableHashedSpaceId, boolean enableUUID, Marker marker){

        Promise<Job> p = Promise.promise();

        /** Load JobConfig */
        CService.jobConfigClient.get(marker, jobId)
                .onSuccess(j -> {
                    if(!isJobStateValid(j, jobId, command, p))
                        return;

                    Import importJob = (Import) j;
                    importJob.setEnabledUUID(enableUUID);

                    try{
                        loadClientAndInjectDefaults(importJob,command,connectorId,ecps,passphrase,enableHashedSpaceId);
                    }catch (HttpException e){
                        p.fail(e);
                        return;
                    }

                    switch (command){
                        case ABORT:
                            p.fail(new HttpException(NOT_IMPLEMENTED,"NA"));
                            return;
                        case CREATEUPLOADURL:
                            try {
                                addUploadURL(importJob);
                                CService.jobConfigClient.update(marker, importJob)
                                        .onFailure( t-> p.fail(new HttpException(BAD_GATEWAY, t.getMessage())))
                                        .onSuccess( f-> p.complete(importJob));
                            } catch (IOException e) {
                                logger.error("Can`t create S3 URL ",e);
                                p.fail(new HttpException(BAD_GATEWAY, e.getMessage()));
                            }
                            return;
                        case RETRY:
                        case START:
                            checkAndAddJob(marker, importJob, p);
                    }
                });

        return p.future();
    }

    public static void addUploadURL(Import job) throws IOException {
        if(job.getStatus().equals(Job.Status.failed)){
            if(job.getErrorDescription() != null && job.getErrorDescription().equals(Import.ERROR_DESCRIPTION_UPLOAD_MISSING)) {
                /** Reset due to creation of upload URL */
                job.setStatus(Job.Status.waiting);
                job.setErrorType(null);
            }
        }
        job.addImportObject(CService.jobS3Client.generateUploadURL(job));
    }

    private static boolean isJobStateValid(Job job, String jobId, HApiParam.HQuery.Command command, Promise<Job> p) {

        if (job == null) {
            p.fail(new HttpException(NOT_FOUND, "Job with Id " + jobId + " not found"));
            return false;
        }

        if(job.getStatus().equals(Job.Status.failed) && !command.equals(HApiParam.HQuery.Command.RETRY) && !command.equals(HApiParam.HQuery.Command.CREATEUPLOADURL)){
            p.fail(new HttpException(PRECONDITION_FAILED, "Job has failed - maybe its possible to retry"));
            return false;
        }

        switch (command){
            case CREATEUPLOADURL:
                switch (job.getStatus()){
                    case waiting:
                        return true;
                    case failed:
                        if(job.getErrorDescription() != null && job.getErrorDescription().equals(Import.ERROR_DESCRIPTION_UPLOAD_MISSING)) {
                            /** Reset due to creation of upload URL */
                            job.setStatus(Job.Status.waiting);
                            job.setErrorDescription(null);
                            job.setErrorType(null);
                            return true;
                        }
                        p.fail(new HttpException(PRECONDITION_FAILED, "Invalid state: "+job.getStatus()));
                        return false;
                    default:
                        p.fail(new HttpException(PRECONDITION_FAILED, "Job got already started - current status: "+job.getStatus()));
                        return false;
                }
            case RETRY:
                switch (job.getStatus()){
                    case failed:
                        if(job.getErrorType() != null && job.getErrorType().equals(Import.ERROR_TYPE_VALIDATION_FAILED)) {
                            /** Reset due to creation of upload URL */
                            job.setStatus(Job.Status.waiting);
                            job.setErrorDescription(null);
                            job.setErrorType(null);
                            return true;
                        }
                        p.fail(new HttpException(BAD_REQUEST, "Retry not possible - please check error!"));
                        return false;
                    case waiting:
                        p.fail(new HttpException(BAD_REQUEST, "Retry not possible - job needs to get started!"));
                        return false;
                    case validating:
                        job.setStatus(Job.Status.waiting);
                        job.setErrorType(null);
                        return true;
                    case preparing:
                        job.setStatus(Job.Status.validated);
                        job.setErrorType(null);
                        return true;
                    case executed:
                        return true;
                    case executing: //to allow this we need to check potentially the running exports first
                    case finalizing://to allow this we need to check potentially the idx creations first
                    default:
                        if(job.getErrorType() != null) {
                            p.fail(new HttpException(BAD_REQUEST, "Retry not possible - please check error!"));
                            return false;
                        }
                        p.fail(new HttpException(BAD_REQUEST, "Retry not possible - please check state!"));
                        return false;
                }
            case START:
                try {
                    isValidForStart(job);
                    return true;
                }catch (HttpException e){
                    p.fail(e);
                    return false;
                }
            default:
                return false;
        }
    }
}
