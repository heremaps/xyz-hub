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
import com.here.xyz.httpconnector.config.JDBCClients;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Type;
import com.here.xyz.httpconnector.util.jobs.validate.ExportValidator;
import com.here.xyz.httpconnector.util.jobs.validate.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.validate.Validator;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.util.Hasher;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Set;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class JobHandler {
    private static final Logger logger = LogManager.getLogger();
    private static ArrayList<String> MODIFICATION_WHITELIST = new ArrayList<String>(){{  add("description"); add("enabledUUID"); add("csvFormat"); }};
    private static Map<String,String> MODIFICATION_IGNORE_MAP = new HashMap<String,String>(){{put("createdAt","createdAt");put("updatedAt","updatedAt");}};

    public static Future<Job> loadJob(String jobId, Marker marker){
        return CService.jobConfigClient.get(marker, jobId)
                .compose( loadedJob -> {
                    if(loadedJob == null)
                        return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+jobId+"' does not exists!"));
                    return Future.succeededFuture(loadedJob);
                }).onFailure( e -> {
                    Future.failedFuture(new HttpException(BAD_GATEWAY, "Cant load'" + jobId + "' from backend!"));
                });
    }

    public static Future<List<Job>> loadJobs(Marker marker, Type type, Job.Status status, String targetSpaceId){
        return CService.jobConfigClient.getList(marker, type, status, targetSpaceId)
                .onFailure( e -> {
                    Future.failedFuture(new HttpException(BAD_GATEWAY, "Cant load jobs from backend!"));
                });
    }

    public static Future<Job> postJob(Job job, Marker marker){
        return CService.jobConfigClient.get(marker, job.getId())
                .compose( loadedJob -> {
                    if(loadedJob != null)
                        return Future.failedFuture(new HttpException(BAD_REQUEST, "Job with id '"+job.getId()+"' already exists!"));
                    else{
                        if(job instanceof Export)
                            return ExportHandler.postJob((Export)job, marker);
                        return ImportHandler.postJob((Import)job, marker);
                    }
                });
    }

    public static Future<Job> patchJob(Job job, Marker marker){
        return  loadJob(job.getId(), marker)
                .compose( loadedJob -> {
                    Map oldJobMap = asMap(loadedJob);
                    Difference.DiffMap diffMap = (Difference.DiffMap) Patcher.calculateDifferenceOfPartialUpdate(oldJobMap, asMap(job), MODIFICATION_IGNORE_MAP, true);

                    if (diffMap == null) {
                        return Future.succeededFuture(loadedJob);
                    } else {
                        try {
                            validateChanges(diffMap);
                            Patcher.patch(oldJobMap, diffMap);
                            loadedJob = asJob(marker, oldJobMap);

                            /** Store patched Config */
                            return CService.jobConfigClient.update(marker, loadedJob);
                        }catch (HttpException e){
                            return Future.failedFuture(e);
                        }
                    }
                });
    }

    public static Future<Job> deleteJob(String jobId, boolean force, Marker marker){
        return loadJob(jobId, marker)
                .compose(job -> {
                    if ( !Validator.isValidForDelete(job, force)) {
                        return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Job is not in end state - current status: "+ job.getStatus()) );
                    }else {
                        if(force){
                            /** In force mode abort running SQLs */
                            return abortJobIfPossible(job)
                                    .onSuccess(f -> {
                                        /** Clean S3 Job Folder */
                                        CService.jobS3Client.cleanJobData(job, force);
                                    }).compose(f -> CService.jobConfigClient.delete(marker, job.getId(), force));
                        }else {
                            return CService.jobConfigClient.delete(marker, job.getId(), force);
                        }
                    }
                });
    }

    public static Future<Job> postExecute(String jobId, String connectorId, String ecps, String passphrase, HApiParam.HQuery.Command command,
                                          boolean enableHashedSpaceId, boolean enableUUID, ApiParam.Query.Incremental incremental, int urlCount,
                                          ContextAwareEvent.SpaceContext _context, Marker marker){
        /** Load JobConfig */
        return loadJob(jobId, marker)
                .compose(job -> {
                    try {
                        /** Validate */
                        Validator.isValidForExecution(job, command, incremental);
                        return Future.succeededFuture(job);
                    } catch (HttpException e) {
                        return Future.failedFuture(e);
                    }
                }).
                compose(job -> {
                    /** Execute */
                    if(job instanceof Import)
                        return ImportHandler.execute(jobId,connectorId,ecps,passphrase,command,enableHashedSpaceId,enableUUID,urlCount,marker);
                    else
                        return ExportHandler.execute(jobId,connectorId,ecps,passphrase,command,enableHashedSpaceId,enableUUID,incremental,_context,marker);
                });
    }

    protected static void loadClientAndInjectConfigValues(Job job, HApiParam.HQuery.Command command, String connectorId, String ecps,
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

        if(job instanceof Export)
            ExportHandler.loadClientAndInjectExportDefaults((Export)job, incremental);

        if(command.equals(HApiParam.HQuery.Command.START) || command.equals(HApiParam.HQuery.Command.RETRY) || command.equals(HApiParam.HQuery.Command.ABORT)){
            /** Add Client if missing or reload client if config has changed */
            try {
                if(connectorId == null || ecps == null)
                    JDBCClients.addClientIfRequired(job.getTargetConnector());
                else
                    JDBCClients.addClientIfRequired(connectorId, ecps, passphrase);
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
                throw new HttpException(BAD_REQUEST, "The property '"+key+"' is immutable!");
        }
    }

    protected static void isValidForAbort(Job job) throws HttpException{
        if(job instanceof Import)
            ImportValidator.isValidForAbort(job);
        else if(job instanceof Export)
            ExportValidator.isValidForAbort(job);
    }

    protected static Future<Void> abortJobIfPossible(Job job) {
        try {
            isValidForAbort(job);
            /** Target: we want to terminate all running sqlQueries */
            return JDBCClients.abortJobsByJobId(job);
        }catch (HttpException e){
            /** Job is not in state "executing" ignore */
            return Future.succeededFuture();
        }
    }

    protected static Future<Job> abortJob(Job job){
           return JDBCClients.abortJobsByJobId(job)
                .compose(f -> Future.succeededFuture(job))
                .onFailure(
                        e -> new HttpException(BAD_GATEWAY, "Abort failed ["+job.getStatus()+"]"));
    }
}
