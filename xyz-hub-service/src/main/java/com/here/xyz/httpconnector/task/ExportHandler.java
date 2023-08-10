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

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.*;
import com.here.xyz.httpconnector.util.jobs.validate.ExportValidator;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

import java.util.HashMap;

public class ExportHandler extends JobHandler{
    private static final Logger logger = LogManager.getLogger();

    protected static Future<Job> postJob(Export job, Marker marker){
        try{
            ExportValidator.setExportDefaults(job);
            ExportValidator.validateExportCreation(job);
        }catch (Exception e){
            return Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage()));
        }

        return HubWebClient.getSpaceStatistics(job.getTargetSpaceId())
                .compose( statistics-> {
                    Long value = statistics.getCount().getValue();
                    if(value != null && value != 0) {
                        /** Store count of features which are in source layer */
                        job.setEstimatedFeatureCount(value);
                    }
                    
                    HashMap<String, Long> srchProp = new HashMap<String, Long>();
                    for ( PropertyStatistics pStat : statistics.getProperties().getValue())
                     if( pStat.isSearchable() )
                      srchProp.put(pStat.getKey(), pStat.getCount() );

                    if( !srchProp.isEmpty() )
                     job.setSearchableProperties(srchProp);

                    if(   job.getEstimatedFeatureCount() > 1000000 /** searchable limit without index*/
                       && job.getPartitionKey() != null 
                       && !"id".equals(job.getPartitionKey()) 
                       && !srchProp.containsKey(job.getPartitionKey().replaceFirst("^(p|properties)\\." ,""))
                       )
                        return Future.failedFuture(new HttpException(BAD_REQUEST, "partitionKey ["+ job.getPartitionKey() +"] is not a searchable property"));  

                    return CService.jobConfigClient.store(marker, job);
                });
    }

    protected static Future<Job> execute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                      boolean enableHashedSpaceId, boolean enableUUID, ApiParam.Query.Incremental incremental, ContextAwareEvent.SpaceContext _context, Marker marker){

        /** At this point we only have jobs which are allowed for executions. */

        /** Load JobConfig */
        return loadJob(jobId, marker)
                .compose(loadedJob -> {
                    Export exportJob = (Export) loadedJob;
                    try {
                        /** Load DB-Client, inject and store config values */
                        loadClientAndInjectConfigValues(exportJob, command, connectorId, ecps, passphrase, enableHashedSpaceId, null, incremental, _context);

                        return CService.jobConfigClient.update(marker, exportJob);
                    }catch (HttpException e){
                        return Future.failedFuture(e);
                    }
                })
                .compose(exportJob -> {
                    if(command.equals(HApiParam.HQuery.Command.ABORT)){
                        return abortJob(exportJob);
                    }
                    return Future.succeededFuture(exportJob);
                })
                .compose(exportJob -> {
                    if(command.equals(HApiParam.HQuery.Command.CREATEUPLOADURL)){
                        return Future.failedFuture(new HttpException(NOT_IMPLEMENTED, "For Export not required!"));
                    }
                    return Future.succeededFuture(exportJob);
                })
                .compose(exportJob -> {
                    if(command.equals(HApiParam.HQuery.Command.RETRY)){
                        try {
                            exportJob.resetToPreviousState();
                            return CService.jobConfigClient.update(marker, exportJob)
                                    .onSuccess(f -> CService.exportQueue.addJob(exportJob));
                        } catch (Exception e) {
                            return Future.failedFuture(new HttpException(BAD_REQUEST, "Job has no lastStatus - cant retry!"));
                        }
                    }
                    return Future.succeededFuture(exportJob);
                })
                .compose(exportJob -> {
                    if(command.equals(HApiParam.HQuery.Command.START)){
                        CService.exportQueue.addJob(exportJob);
                    }
                    return Future.succeededFuture(exportJob);
                });
    }

    protected static void loadClientAndInjectExportDefaults(Export job, ApiParam.Query.Incremental incremental)
            throws HttpException {

        if(incremental.equals(ApiParam.Query.Incremental.FULL)) {
            /** We have to check if the super layer got exported in a persistent way */
            if(!job.isSuperSpacePersistent())
                throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Incremental Export requires persistent superLayer!");
            /** Add path to persistent Export */
            addSuperExportPathToJob(job.extractSuperSpaceId(), (job));
        }
    }

    private static void addSuperExportPathToJob(String superSpaceId, Export job) throws HttpException {
        String superExportPath = CService.jobS3Client.checkPersistentS3ExportOfSuperLayer(superSpaceId, job);
        if (superExportPath == null)
            throw new HttpException(PRECONDITION_FAILED, "Persistent Base-Layer export is missing!");

        /** Add path to params tobe able to load already exported data from */
        job.addParam("superExportPath", superExportPath);
    }
}
