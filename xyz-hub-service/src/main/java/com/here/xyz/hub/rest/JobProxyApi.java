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
package com.here.xyz.hub.rest;

import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Map;

import static com.here.xyz.hub.auth.XyzHubAttributeMap.SPACE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class JobProxyApi extends Api{
    private static final Logger logger = LogManager.getLogger();
    private static int JOB_API_TIMEOUT = 29_000;

    public JobProxyApi(RouterBuilder rb) {
        rb.operation("postJob").handler(this::postJob);
        rb.operation("patchJob").handler(this::patchJob);
        rb.operation("getJobs").handler(this::getJobs);
        rb.operation("getJob").handler(this::getJob);
        rb.operation("deleteJob").handler(this::deleteJob);
        rb.operation("postExecute").handler(this::postExecute);
    }

    private void postJob(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        try {
            Job job = HApiParam.HQuery.getJobInput(context);
            JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                    .onSuccess(auth -> {
                        Service.spaceConfigClient.get(Api.Context.getMarker(context), spaceId)
                                .onFailure(t ->  this.sendErrorResponse(context, new HttpException(BAD_REQUEST, "The resource ID does not exist!")))
                                .compose(headSpace -> {
                                    if (headSpace == null) {
                                        return Future.failedFuture(new HttpException(BAD_REQUEST, "The resource ID does not exist!"));
                                    }
                                    if (job instanceof Import && headSpace.getVersionsToKeep() != 1) {
                                        return Future.failedFuture(new HttpException(BAD_REQUEST, "Versioning is not supported!"));
                                    }

                                    return Future.succeededFuture(headSpace);
                                })
                                .compose(headSpace -> {
                                    job.setTargetSpaceId(spaceId);
                                    job.setTargetConnector(headSpace.getStorage().getId());
                                    job.addParam("versionsToKeep",headSpace.getVersionsToKeep());
                                    job.addParam("persistExport", headSpace.isPersistExport());
                                    Promise<Map> p = Promise.promise();
                                    if (headSpace.getExtension() != null) {
                                        /** Evaluate if we have 2nd extension */
                                        Service.spaceConfigClient.get(Api.Context.getMarker(context), headSpace.getExtension().getSpaceId())
                                                .onSuccess(baseSpace -> {
                                                    p.complete(headSpace.resolveCompositeParams(baseSpace));
                                                })
                                                .onFailure(e -> p.fail(e));
                                    }else
                                        p.complete(null);
                                    return p.future();
                                }).compose(extension -> {
                                    if(extension != null) {
                                        /** Add extends to jobConfig */
                                        job.addParam("extends",extension.get("extends"));
                                    }
                                    return Future.succeededFuture();
                                })
                                .onSuccess(f -> {
                                    Future.succeededFuture(Service.webClient
                                            .postAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT + "/jobs")
                                            .timeout(JOB_API_TIMEOUT)
                                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                                            .sendBuffer(Buffer.buffer(Json.encode(job)))
                                            .onSuccess(res -> jobAPIResultHandler(context, res, spaceId))
                                            .onFailure(e -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!"))));
                                })
                                .onFailure(f -> this.sendErrorResponse(context, f));
                    })
                    .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
        }catch (HttpException e){
            this.sendErrorResponse(context, e);
        }
    }

    private void patchJob(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        String jobId = context.pathParam(HApiParam.Path.JOB_ID);

        JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                .onSuccess(auth -> {
                    Service.webClient.getAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                        .timeout(JOB_API_TIMEOUT)
                        .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                        .send()
                        .onSuccess(res -> {
                            if (res.statusCode() != 200) {
                                /** Job not found - proxy response */
                                this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
                                return;
                            }
                            if(!checkSpaceId(res,spaceId)) {
                                this.sendErrorResponse(context, new HttpException(FORBIDDEN, "This job belongs to another space!"));
                                return;
                            }

                            try{
                                Job job = HApiParam.HQuery.getJobInput(context);
                                Service.webClient
                                        .patchAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                                        .timeout(JOB_API_TIMEOUT)
                                        .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                                        .sendBuffer(Buffer.buffer(Json.encode(job)))
                                        .onSuccess(res2 -> jobAPIResultHandler(context,res2,spaceId))
                                        .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                            }catch (HttpException e){
                                this.sendErrorResponse(context, e);
                            }
                        })
                        .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                })
                .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
    }

    private void getJob(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        String jobId = context.pathParam(HApiParam.Path.JOB_ID);

        JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                .onSuccess(auth -> {
                    Service.webClient.getAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                            .timeout(JOB_API_TIMEOUT)
                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                            .send()
                            .onSuccess(res -> jobAPIResultHandler(context,res,spaceId))
                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                })
                .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
    }

    private void getJobs(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        Job.Status status = HApiParam.HQuery.getJobStatus(context);

        JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                .onSuccess(auth -> {
                    Service.webClient.getAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs?targetSpaceId="+spaceId+(status != null ? "&status="+status : ""))
                            .timeout(JOB_API_TIMEOUT)
                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                            .send()
                            .onSuccess(res -> jobAPIListResultHandler(context,res,spaceId))
                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                })
                .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
    }

    private void deleteJob(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        String jobId = context.pathParam(HApiParam.Path.JOB_ID);

        JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                .onSuccess(auth -> {
                    Service.webClient.getAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                            .timeout(JOB_API_TIMEOUT)
                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                            .send()
                            .onSuccess(res -> {
                                    if (res.statusCode() != 200) {
                                        /** Job not found - proxy response */
                                        this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
                                        return;
                                    }
                                    if(!checkSpaceId(res,spaceId)) {
                                        this.sendErrorResponse(context, new HttpException(FORBIDDEN, "This job belongs to another space!"));
                                        return;
                                    }
                                    Service.webClient.deleteAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                                            .timeout(JOB_API_TIMEOUT)
                                            .send()
                                            .onSuccess(res2 -> jobAPIResultHandler(context,res2,spaceId))
                                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));

                            })
                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                })
                .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
    }

    private void postExecute(final RoutingContext context) {
        String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
        String jobId = context.pathParam(HApiParam.Path.JOB_ID);
        String command = ApiParam.Query.getString(context, HApiParam.HQuery.H_COMMAND, null);

        JobAuthorization.authorizeManageSpacesRights(context,spaceId)
                .onSuccess(auth -> {
                    Service.webClient.getAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/jobs/"+jobId)
                            .timeout(JOB_API_TIMEOUT)
                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                            .send()
                            .onSuccess(res -> {
                                if (res.statusCode() != 200) {
                                    /** Job not found  - proxy response */
                                    this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
                                }

                                if(!checkSpaceId(res,spaceId)) {
                                    this.sendErrorResponse(context, new HttpException(FORBIDDEN, "This job belongs to another space!"));
                                    return;
                                }

                                Job job = res.bodyAsJson(Job.class);

                                //TODO - resolve composite Connectors
                                Service.connectorConfigClient.get( Api.Context.getMarker(context), job.getTargetConnector())
                                    .onFailure(t ->  this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject()))
                                    .onSuccess(connector -> {

                                        if(connector != null && connector.params != null){
                                            String ecps = (String) connector.params.get("ecps");
                                            /** We do not modify the job-config at this place - so we need to pass connector related information */

                                            Boolean enableHashedSpaceId = connector.params.get("enableHashedSpaceId") == null ? false : (Boolean) connector.params.get("enableHashedSpaceId");

                                            String postUrl = (Service.configuration.HTTP_CONNECTOR_ENDPOINT
                                                    +"/jobs/{jobId}/execute?"
                                                    +"&connectorId={connectorId}"
                                                    +"&ecps={ecps}"
                                                    +"&enableHashedSpaceId="+enableHashedSpaceId
                                                    +"&enableUUID={enableUUID}"
                                                    +"&command={command}")
                                                        .replace("{jobId}",jobId)
                                                        .replace("{connectorId}",connector.id)
                                                        .replace("{ecps}",ecps)
                                                        /**deprecated */
                                                        .replace("{enableUUID}","false")
                                                        .replace("{command}",command);

                                            Service.webClient
                                                    .postAbs(postUrl)
                                                    .timeout(JOB_API_TIMEOUT)
                                                    .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                                                    .send()
                                                    .onSuccess(postRes -> jobAPIResultHandler(context,postRes, spaceId))
                                                    .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                                        }
                                    });
                            })
                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                })
                .onFailure(f -> this.sendErrorResponse(context, new HttpException(FORBIDDEN, "No access to this space!")));
    }

    private void jobAPIResultHandler(final RoutingContext context, HttpResponse<Buffer> res, String spaceId){
        if (res.statusCode() < 500) {
            try {
                if(!checkSpaceId(res,spaceId)){
                    this.sendErrorResponse(context, new HttpException(FORBIDDEN, "This job belongs to another space!"));
                    return;
                }
            }catch (DecodeException e) {}

            try{
                this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()),
                        Json.decodeValue(DatabindCodec.mapper().writerWithView(Job.Public.class).writeValueAsString(res.bodyAsJson(Job.class))));
                return;
            }catch (Exception e){}
            try{
                this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
                return;
            }catch (Exception e){}
        }

        this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!"));
    }

    private void jobAPIListResultHandler(final RoutingContext context, HttpResponse<Buffer> res, String spaceId){
        if (res.statusCode() < 500) {
            try{
                this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()),
                        Json.decodeValue(DatabindCodec.mapper().writerWithView(Job.Public.class).writeValueAsString(res.bodyAsJson(Job[].class))));
                return;
            }catch (Exception e){}

            try{
                this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
            }catch (Exception e){}
        }

        this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!"));
    }

    private Boolean checkSpaceId(HttpResponse<Buffer> res, String spaceId) throws DecodeException{
        Job job = res.bodyAsJson(Job.class);

        if(job == null || job.getTargetSpaceId() == null)
            throw new DecodeException("");

        if(!job.getTargetSpaceId().equalsIgnoreCase(spaceId))
            return false;
        return true;
    }

    public static class JobAuthorization extends Authorization {
        public static Future<Void> authorizeManageSpacesRights(RoutingContext context, String spaceId) {
            final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
                    .manageSpaces(new XyzHubAttributeMap().withValue(SPACE, spaceId));
            try {
                evaluateRights(Context.getMarker(context), requestRights, Context.getJWT(context).getXyzHubMatrix());
                return Future.succeededFuture();
            } catch (HttpException e) {
                return Future.failedFuture(e);
            }
        }
    }
}
