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

package com.here.xyz.httpconnector.task;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Type;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.util.Hasher;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.io.IOException;
import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class ImportHandler {
    private static final Logger logger = LogManager.getLogger();
    private static ArrayList<String> MODIFICATION_WHITELIST = new ArrayList<String>(){{  add("description"); add("enabledUUID"); add("csvFormat"); add("importObjects"); }};
    private static Map<String,String> MODIFICATION_IGNORE_MAP = new HashMap<String,String>(){{put("createdAt","createdAt");put("updatedAt","updatedAt");}};

    public static Future<Job> postJob(Job job, Marker marker){
        Promise<Job> p = Promise.promise();

        if (job.id == null) {
            job.id = RandomStringUtils.randomAlphanumeric(6);
        }

        if(job instanceof Import){
            String error = ImportValidator.validateImportCreation((Import)job);
            if(error != null)
                return Future.failedFuture(new HttpException(BAD_REQUEST, error));
        }

        CService.jobConfigClient.get(marker, job.getId())
                .compose( loadedJob -> {
                    if(loadedJob == null)
                        return CService.jobConfigClient.store(marker, job);
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+job.getId()+"' already exists!"));
                })
                .onSuccess(
                        storedJob -> p.complete(storedJob)
                ).onFailure(t -> {
                    if (t instanceof HttpException) {
                        p.fail(t);
                    }else if (t instanceof AmazonDynamoDBException){
                        p.fail(new HttpException(BAD_REQUEST, "Payload is wrong!"));
                    }else {
                        logger.warn(marker, "Unexpected Error during saving a Job", t);
                        p.fail(new HttpException(BAD_GATEWAY, t.getMessage()));
                    }
                });
        return p.future();
    }

    public static Future<Job> patchJob(Job job, Marker marker){
        Promise<Job> p = Promise.promise();

        CService.jobConfigClient.get(marker, job.getId())
                .compose( loadedJob -> {
                    if(loadedJob != null) {
                        Map oldJobMap = asMap(loadedJob);

                        Difference.DiffMap diffMap = (Difference.DiffMap) Patcher.calculateDifferenceOfPartialUpdate(oldJobMap, asMap(job), MODIFICATION_IGNORE_MAP, true);

                        if (diffMap == null) {
                            return Future.succeededFuture(job);
                        } else {
                            try {
                                validateChanges(diffMap);
                                Patcher.patch(oldJobMap, diffMap);
                                loadedJob = asJob(marker, oldJobMap);

                                return CService.jobConfigClient.update(marker, loadedJob);
                            }catch (HttpException e){
                                return Future.failedFuture(e);
                            }
                        }
                    }
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+job.getId()+"' does not exists!"));
                })
                .onSuccess(
                        updatedJob -> p.complete(updatedJob)
                ).onFailure(t -> {
                    if (t instanceof AmazonDynamoDBException) {
                        p.fail(new HttpException(BAD_REQUEST, "Payload is wrong!"));
                    }else if (t instanceof HttpException){
                        p.fail(t);
                    }else {
                        logger.warn(marker, "Unexpected Error during Job Update", t);
                        p.fail(new HttpException(BAD_GATEWAY, t.getMessage()));
                    }
                });
        return p.future();
    }

    public static Future<List<Job>> getJobs(Marker marker, Type type, Job.Status status, String targetSpaceId){
        Promise<List<Job>> p = Promise.promise();

        CService.jobConfigClient.getList(marker, type, status, targetSpaceId).onSuccess(
                j -> p.complete(j)
        ).onFailure(t -> p.fail(new HttpException(BAD_GATEWAY, t.getMessage())));

        return p.future();
    }

    public static Future<Job> getJob(String jobId, Marker marker){
        Promise<Job> p = Promise.promise();

        CService.jobConfigClient.get(marker, jobId).onSuccess(j -> {
            if(j == null){
                p.fail(new HttpException(NOT_FOUND, "Job with Id "+jobId+" not found"));
            }else{
                p.complete(j);
            }
        }).onFailure(t -> p.fail(new HttpException(BAD_GATEWAY, t.getMessage())));

        return p.future();
    }

    public static Future<Job> deleteJob(String jobId, Marker marker){
        Promise<Job> p = Promise.promise();

        CService.jobConfigClient.delete(marker, jobId).onSuccess(j -> {
            if(j == null){
                p.fail(new HttpException(NOT_FOUND, "Job with Id "+jobId+" not found"));
            }else{
                p.complete(j);
            }
        }).onFailure(t -> p.fail(new HttpException(BAD_GATEWAY, t.getMessage())));

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

    public static Future<Job> postExecute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                          boolean enableHashedSpaceId, boolean enableUUID, Marker marker){

        Promise<Job> p = Promise.promise();

        CService.jobConfigClient.get(marker, jobId).onSuccess(j -> {
            if(!isJobStateValid(j, jobId, command, p))
                return;

            if(j != null && (j instanceof Import)){
                Import importJob = (Import) j;

                if(importJob.getTargetTable() == null){
                    importJob.setTargetTable(enableHashedSpaceId ? Hasher.getHash(importJob.getTargetSpaceId()) : importJob.getTargetSpaceId());
                }

                if(command.equals(HApiParam.HQuery.Command.START) || command.equals(HApiParam.HQuery.Command.RETRY)){
                    /** Add Client if missing or reload client if config has changed */

                    try {
                        JDBCImporter.addClientIfRequired(connectorId, ecps, passphrase);
                    }catch (CannotDecodeException e){
                        p.fail(new HttpException(PRECONDITION_FAILED, "Can not decode ECPS!"));
                        return;
                    }


                    /** Need connectorId as JDBC-clientID for scheduled processing in ImportQueue */
                    importJob.setTargetConnector(connectorId);
                    importJob.setEnabledUUID(enableUUID);
                }

                switch (command){
                    case ABORT:
                        p.fail(new HttpException(NOT_IMPLEMENTED,"NA"));
                        return;
                    case CREATEUPLOADURL:
                        try {
                            addUploadURL(importJob);
                            p.complete(importJob);
                            return;
                        } catch (IOException e) {
                            e.printStackTrace();
                            p.fail(new HttpException(BAD_GATEWAY, e.getMessage()));
                            return;
                        }
                    case RETRY:
                        importJob.setErrorType(null);
                        importJob.setErrorDescription(null);

                        /** Actively push job to local JOB-Queue */
                        CService.importQueue.addJob(importJob);
                        p.complete(importJob);
                        return;
                    case START:
                        if(importJob.getStatus().equals(Job.Status.waiting)){
                            /** Actively push job to local JOB-Queue */
                            CService.importQueue.addJob(importJob);
                            p.complete(importJob);
                        }else
                           p.fail(new HttpException(PRECONDITION_FAILED, "Job cant get started. Invalid Job Status '"+importJob.getStatus()+"' !"));

                        return;
                }
            }
        }).compose(
                jobStatusUpdate -> CService.jobConfigClient.update(marker, jobStatusUpdate));

        return p.future();
    }

    private static Map asMap(Object object) {
        try {
            return DatabindCodec.mapper().convertValue(object, Map.class);
        }
        catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static Job asJob(Marker marker, Map object) throws HttpException {
        try {
            return DatabindCodec.mapper().convertValue(object, Job.class);
        }
        catch (Exception e) {
            logger.error(marker, "Could not convert resource.", e.getCause());
            throw new HttpException(BAD_GATEWAY, "Could not convert resource.");
        }
    }

    public static void validateChanges(Difference.DiffMap diffMap) throws HttpException{
        /** Check if modification is allowed */
        Set<Object> objects = diffMap.keySet();
        for(Object key : objects){
            if(MODIFICATION_WHITELIST.indexOf((String)key) == -1)
                throw new HttpException(BAD_REQUEST, "The propertey '"+key+"' is immutable!");
        }
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
                    case partially_failed:
                    case failed:
                        if(job.getErrorDescription() != null && job.getErrorDescription().equals(Import.ERROR_DESCRIPTION_UPLOAD_MISSING)) {
                            /** Reset due to creation of upload URL */
                            job.setStatus(Job.Status.waiting);
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
                switch (job.getStatus()){
                    case finalized:
                        p.fail(new HttpException(PRECONDITION_FAILED, "Job is already finalized !"));
                        return false;
                    case partially_failed:
                    case failed:
                        if(!command.equals(HApiParam.HQuery.Command.RETRY))
                            p.fail(new HttpException(PRECONDITION_FAILED, "Failed - check error and retry!"));
                        return false;
                    case queued:
                    case validating:
                    case validated:
                    case preparing:
                    case prepared:
                    case executing:
                    case executed:
                    case finalizing:
                        p.fail(new HttpException(PRECONDITION_FAILED, "Job is already running - current status: "+job.getStatus()));
                        return false;
                    default:
                        return true;
                }
            default:
                return false;
        }
    }
}
