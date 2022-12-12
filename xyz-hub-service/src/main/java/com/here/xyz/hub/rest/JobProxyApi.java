package com.here.xyz.hub.rest;

import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;

import static com.here.xyz.hub.auth.XyzHubAttributeMap.SPACE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class JobProxyApi extends Api{
    private static final Logger logger = LogManager.getLogger();
    private static int JOB_API_TIMEOUT = 29_000;

    public JobProxyApi(RouterBuilder rb) {
        rb.operation("postJob").handler(this::postJob);
        rb.operation("patchJob").handler(this::patchJob);
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
                                .onSuccess(headSpace -> {
                                    if (headSpace == null) {
                                        this.sendErrorResponse(context, new HttpException(BAD_REQUEST, "The space does not exist!"));
                                        return;
                                    }
                                    job.setTargetSpaceId(spaceId);
                                    job.setTargetConnector(headSpace.getStorage().getId());

                                    Service.webClient
                                            .postAbs(Service.configuration.HTTP_CONNECTOR_ENDPOINT + "/jobs")
                                            .timeout(JOB_API_TIMEOUT)
                                            .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                                            .sendBuffer(Buffer.buffer(Json.encode(job)))
                                            .onSuccess(res -> jobAPIResultHandler(context, res, spaceId))
                                            .onFailure(f -> this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!")));
                                });
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

                                Service.connectorConfigClient.get( Api.Context.getMarker(context), job.getTargetConnector())
                                    .onFailure(t ->  this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject()))
                                    .onSuccess(connector -> {

                                        if(connector != null && connector.params != null){
                                            String ecps = (String) connector.params.get("ecps");
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

            this.sendResponse(context, HttpResponseStatus.valueOf(res.statusCode()), res.bodyAsJsonObject());
        }
        else {
            this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Job-Api not ready!"));
        }
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
