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
import com.here.xyz.httpconnector.config.JDBCExporter;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Type;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.util.Hasher;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class JobHandler {
    private static final Logger logger = LogManager.getLogger();
    private static ArrayList<String> MODIFICATION_WHITELIST = new ArrayList<String>(){{  add("description"); add("enabledUUID"); add("csvFormat"); }};
    private static Map<String,String> MODIFICATION_IGNORE_MAP = new HashMap<String,String>(){{put("createdAt","createdAt");put("updatedAt","updatedAt");}};

    public static Future<Job> patchJob(Job job, Marker marker){
        return CService.jobConfigClient.get(marker, job.getId())
                .compose( loadedJob -> {
                    if(loadedJob != null) {
                        Map oldJobMap = asMap(loadedJob);

                        Difference.DiffMap diffMap = (Difference.DiffMap) Patcher.calculateDifferenceOfPartialUpdate(oldJobMap, asMap(job), MODIFICATION_IGNORE_MAP, true);

                        if (diffMap == null) {
                            return Future.succeededFuture(loadedJob);
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
                });
    }

    public static Future<List<Job>> getJobs(Marker marker, Type type, Job.Status status, String targetSpaceId){
        return CService.jobConfigClient.getList(marker, type, status, targetSpaceId);
    }

    public static Future<Job> getJob(String jobId, Marker marker){
        return CService.jobConfigClient.get(marker, jobId)
                .compose(j -> {
                    if(j == null){
                        return Future.failedFuture(new HttpException(NOT_FOUND, "Job with Id "+jobId+" not found"));
                    }else{
                        return Future.succeededFuture(j);
                    }
                 });
    }

    public static Future<Job> deleteJob(Job job, boolean force, Marker marker){
        return CService.jobConfigClient.delete(marker, job.getId(), force)
                .compose(j -> {
                    if(j == null){
                        return Future.failedFuture(new HttpException(NOT_FOUND, "Job with Id "+job.getId()+" not found"));
                    } else if ( !CService.jobConfigClient.isValidForDelete(j, force)) {
                        return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Job is not in end state - current status: "+ j.getStatus()) );
                    } else {    
                        /** Clean S3 Job Folder */
                        CService.jobS3Client.cleanJobData(job, force);
                        return Future.succeededFuture(j);
                    }
                });
    }

    public static Future<Job> postExecute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                          boolean enableHashedSpaceId, boolean enableUUID, ApiParam.Query.Incremental incremental, int urlCount,
                                          ContextAwareEvent.SpaceContext _context, Marker marker){

        /** Load JobConfig */
        return CService.jobConfigClient.get(marker, jobId)
                .compose(job -> {
                    if (job == null) {
                       return Future.failedFuture(new HttpException(NOT_FOUND, "Job with Id " + jobId + " not found"));
                    }

                    return Future.succeededFuture(job);
                }).compose(job -> {
                    if(job instanceof Import)
                        return ImportHandler.execute(jobId,connectorId,ecps,passphrase,command,enableHashedSpaceId,enableUUID,urlCount,marker);
                    else //export
                        return ExportHandler.execute(jobId,connectorId,ecps,passphrase,command,enableHashedSpaceId,enableUUID,incremental,_context,marker);
                });
    }

    protected static void loadClientAndInjectDefaults(Job job, HApiParam.HQuery.Command command, String connectorId, String ecps,
                                                      String passphrase, boolean enableHashedSpaceId, Boolean enableUUID,
                                                      ApiParam.Query.Incremental incremental, ContextAwareEvent.SpaceContext _context)
            throws HttpException {

        if(job.getTargetTable() == null){
            /** Only for debugging purpose */
            job.setTargetTable(enableHashedSpaceId ? Hasher.getHash(job.getTargetSpaceId()) : job.getTargetSpaceId());
        }

        if(job.getTargetConnector() == null){
            /** Need connectorId as JDBC-clientID for scheduled processing in ImportQueue */
            job.setTargetConnector(connectorId);
        }

        job.addParam("enableHashedSpaceId",enableHashedSpaceId);

        if(enableUUID != null)
            job.addParam("enableUUID", enableUUID);
        if(incremental != null)
            job.addParam("incremental", incremental.toString());
        if(_context != null)
            job.addParam("context", _context.toString());

        if(command.equals(HApiParam.HQuery.Command.START) || command.equals(HApiParam.HQuery.Command.RETRY)){
            /** Add Client if missing or reload client if config has changed */
            try {
                if(connectorId == null || ecps == null)
                    JDBCImporter.addClientIfRequired(connectorId);
                else
                    JDBCImporter.addClientIfRequired(connectorId, ecps, passphrase);
            }catch (CannotDecodeException e){
                throw new HttpException(PRECONDITION_FAILED, "Can not decode ECPS!");
            }catch (UnsupportedOperationException e){
                throw new HttpException(BAD_REQUEST, "Connector is not supported!");
            }
        }
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

    private static void validateChanges(Difference.DiffMap diffMap) throws HttpException{
        /** Check if modification is allowed */
        Set<Object> objects = diffMap.keySet();
        for(Object key : objects){
            if(MODIFICATION_WHITELIST.indexOf((String)key) == -1)
                throw new HttpException(BAD_REQUEST, "The propertey '"+key+"' is immutable!");
        }
    }

    protected static void checkAndAddJob(Marker marker, Job job, Promise<Job> p){
        checkRunningJobs(marker,job)
                .onSuccess( runningJob -> {
                    if(runningJob == null){
                        /** Update JobConfig */
                        CService.jobConfigClient.update(marker, job)
                                .onFailure( t -> p.fail(new HttpException(BAD_GATEWAY, t.getMessage())))
                                .onSuccess( f -> {
                                    /** Actively push job to local JOB-Queue */
                                    if(job instanceof Import)
                                        CService.importQueue.addJob(job);
                                    else
                                        CService.exportQueue.addJob(job);
                                    p.complete();
                                });
                    }else{
                        p.fail(new HttpException(CONFLICT, "Job '"+runningJob+"' is already running on target!"));
                    }
                }).onFailure(f -> p.fail(new HttpException(BAD_GATEWAY, "Unexpected error!")));
    }

    protected static Future<String> checkRunningJobs(Marker marker, Job job){
        /** Check in node memory */
        String jobId = CService.importQueue.checkRunningJobsOnSpace(job.getTargetSpaceId());

        if(jobId != null) {
            return Future.succeededFuture(jobId);
        }else{
            /** Check only for imports*/
            return CService.jobConfigClient.getRunningJobsOnSpace(marker, job.getTargetSpaceId(), Type.Import);
        }
    }

    protected static void isValidForStart(Job job) throws HttpException{
        switch (job.getStatus()){
            case finalized:
                throw new HttpException(PRECONDITION_FAILED, "Job is already finalized !");
            case failed:
                throw new HttpException(PRECONDITION_FAILED, "Failed - check error and retry!");
            case queued:
            case validating:
            case validated:
            case preparing:
            case prepared:
            case executing:
            case executed:
            case executing_trigger:
            case trigger_executed:
            case collectiong_trigger_status:
            case trigger_status_collected:
            case finalizing:
                throw new HttpException(PRECONDITION_FAILED, "Job is already running - current status: "+job.getStatus());
        }
    }

    protected static void isValidForAbort(Job job) throws HttpException{
        switch (job.getStatus()){
            case executing:
                return;
            default:
                throw new HttpException(PRECONDITION_FAILED, "Not allowed to abort a job with status["+job.getStatus()+"]");
        }
    }


    static void abortJob(Marker marker, Job job, Promise<Job> p) throws HttpException {
        isValidForAbort(job);

        JDBCExporter.abortJobsByJobId(job)
                .onComplete(f -> p.complete());
    }
}
