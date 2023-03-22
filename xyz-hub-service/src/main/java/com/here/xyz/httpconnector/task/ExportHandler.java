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
import com.here.xyz.httpconnector.util.jobs.*;
import com.here.xyz.httpconnector.util.jobs.validate.ExportValidator;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.io.IOException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class ExportHandler extends JobHandler{
    private static final Logger logger = LogManager.getLogger();

    public static Future<Job> postJob(Export job, Marker marker){
        try{
            ExportValidator.setExportDefaults(job);
            ExportValidator.validateExportCreation(job);
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
                    //TODO: Check what to do if statistics not available
                    Long value = statistics.getCount().getValue();
                    if(value != null && value != 0) {
                        job.setEstimatedFeatureCount(value);
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
                    /** Check State */
                    if(!isJobStateValid(j, jobId, command, p))
                        return;

                    Export exportJob = (Export) j;

                    try{
                        loadClientAndInjectDefaults(exportJob,command,connectorId,ecps,passphrase,enableHashedSpaceId);
                    }catch (HttpException e){
                        p.fail(e);
                        return;
                    }

                    switch (command){
                        case ABORT:
                            p.fail(new HttpException(NOT_IMPLEMENTED,"NA"));
                            return;
                        case CREATEUPLOADURL:
                            p.fail(new HttpException(NOT_IMPLEMENTED, "For Export not required!"));
                            return;
                        case RETRY:
                        case START:
                            checkAndAddJob(marker, exportJob, p);
                    }
                });

        return p.future();
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
            case ABORT:
                return true;
            case CREATEUPLOADURL:
                p.fail(new HttpException(NOT_IMPLEMENTED, "For Export not available!"));
                return false;
            case RETRY:
                p.fail(new HttpException(NOT_IMPLEMENTED, "TBD"));
                return false;
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
