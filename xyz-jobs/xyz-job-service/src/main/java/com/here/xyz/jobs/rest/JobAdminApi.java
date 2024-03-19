package com.here.xyz.jobs.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class JobAdminApi extends Api {

    private static final String ADMIN_JOB_STEPS = "/admin/jobs/:jobId/steps";
    private static final String ADMIN_JOB_STEP_ID = "/admin/jobs/:jobId/steps/:stepId";
    public JobAdminApi(Router router) {
        router.route(HttpMethod.POST, ADMIN_JOB_STEPS)
                .handler(BodyHandler.create())
                .handler(handleErrors(this::postStep));

        router.route(HttpMethod.GET, ADMIN_JOB_STEP_ID)
                .handler(handleErrors(this::getStep));
    }

    private void postStep(RoutingContext context) throws HttpException {
        String jobId = ApiParam.getPathParam(context, ApiParam.Path.JOB_ID);
        Step step = getStepFromBody(context);
        Job.load(jobId)
                .compose(job -> job.updateStep(step).map(job))
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void getStep(RoutingContext context) {
        String jobId = ApiParam.getPathParam(context, ApiParam.Path.JOB_ID);
        String stepId = ApiParam.getPathParam(context, ApiParam.Path.STEP_ID);
        Job.load(jobId)
                .compose(job -> {
                    Step step = job.getStepById(stepId);
                    if(step == null) return Future.failedFuture(new HttpException(NOT_FOUND, "Step is not present in the job"));
                    return Future.succeededFuture(step);
                })
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    public static class ApiParam {

        public static class Path {
            static final String JOB_ID = "jobId";
            static final String STEP_ID = "stepId";
        }

        public static class Query {

        }
        public static String getPathParam(RoutingContext context, String param) {
            return context.pathParam(param);
        }

    }

    private Step getStepFromBody(RoutingContext context) throws HttpException {
        try {
            return XyzSerializable.deserialize(context.body().asString(), Step.class);
        }
        catch (JsonProcessingException e) {
            throw new HttpException(BAD_REQUEST, "Error parsing request");
        }
    }
}
