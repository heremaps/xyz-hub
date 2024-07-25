/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.service;

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.service.JobApi.ApiParam.Path.JOB_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.JobApi.ApiParam;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class JobAdminApi extends Api {
  private static final String ADMIN_JOBS = "/admin/jobs";
  private static final String ADMIN_JOB = ADMIN_JOBS + "/:jobId";
  private static final String ADMIN_JOB_STEPS = ADMIN_JOB + "/steps";
  private static final String ADMIN_JOB_STEP = ADMIN_JOB_STEPS + "/:stepId";
  private static final String ADMIN_STATE_MACHINE_EVENTS = "/admin/state/events";

  public JobAdminApi(Router router) {
    router.route(HttpMethod.GET, ADMIN_JOBS).handler(handleErrors(this::getJobs));
    router.route(HttpMethod.GET, ADMIN_JOB).handler(handleErrors(this::getJob));
    router.route(HttpMethod.DELETE, ADMIN_JOBS).handler(handleErrors(this::deleteJob));
    router.route(HttpMethod.POST, ADMIN_JOB_STEPS).handler(handleErrors(this::postStep));
    router.route(HttpMethod.GET, ADMIN_JOB_STEP).handler(handleErrors(this::getStep));
    router.route(HttpMethod.POST, ADMIN_STATE_MACHINE_EVENTS).handler(handleErrors(this::postStateEvent));
  }

  private void getJobs(RoutingContext context) {
    Job.load(getState(context), getResource(context))
        .onSuccess(res -> sendInternalResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getJob(RoutingContext context) {
    loadJob(jobId(context))
        //TODO: Use internal serialization here
        .onSuccess(job -> sendResponseWithXyzSerialization(context, OK, job))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  private static Future<Job> loadJob(String jobId) {
    return Job.load(jobId)
        .compose(job -> job == null ? Future.failedFuture(new HttpException(NOT_FOUND, "The requested job does not exist.")) : Future.succeededFuture(job));
  }

  private void deleteJob(RoutingContext context) throws HttpException {
    getJobFromBody(context).deleteJobResources()
        .onSuccess(v -> sendResponseWithXyzSerialization(context, NO_CONTENT, null))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  private void postStep(RoutingContext context) throws HttpException {
    Step step = getStepFromBody(context);
    loadJob(jobId(context))
        .compose(job -> job.updateStep(step).mapEmpty())
        .onSuccess(v -> sendResponseWithXyzSerialization(context, OK, null))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  private void getStep(RoutingContext context) {
    loadJob(jobId(context))
        .compose(job -> {
          Step step = job.getStepById(stepId(context));
          return step == null
              ? Future.failedFuture(new HttpException(NOT_FOUND, "Step is not present in the job"))
              : Future.succeededFuture(step);
        })
        .onSuccess(step -> sendResponse(context, OK, step))
        .onFailure(t -> sendErrorResponse(context, t));
  }


  /**
   * The sample event format in the request:
   * {
   *   "id": "315c1398-40ff-a850-213b-158f73e60175",
   *   "detail-type": "Step Functions Execution Status Change",
   *   "source": "aws.states",
   *   "account": "123456789012",
   *   "time": "2019-02-26T19:42:21Z",
   *   "region": "us-east-1",
   *   "resources": ["arn:aws:states:us-east-1:123456789012:execution:state-machine-name:execution-name"],
   *   "detail": {
   *     "executionArn": "arn:aws:states:us-east-1:123456789012:execution:state-machine-name:execution-name",
   *     "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:state-machine",
   *     "name": "execution-name",
   *     "status": "RUNNING",
   *     "startDate": 1551225271984,
   *     "stopDate": null,
   *     "input": "{}",
   *     "output": null
   *   }
   * }
   *
   */
  private void postStateEvent(RoutingContext context) throws HttpException {
    JsonObject event = context.body().asJsonObject();

    if (event.containsKey("detail")) {
      /*
      Right now we set JobId as "name" of the state machine execution.
      If for some reason it changes, we should add the jobId to the "input" param and read it from there.
       */
      String jobId = event.getJsonObject("detail").getString("name");
      String sfnStatus = event.getJsonObject("detail").getString("status");

      if (jobId == null)
        logger.error("The state machine event does not include a Job ID: {}", event);
      else if (sfnStatus == null)
        logger.error("The state machine event does not include a status: {}", event);
      else
        loadJob(jobId)
            .compose(job -> {
              State newJobState = switch (sfnStatus) {
                case "SUCCEEDED" -> SUCCEEDED;
                case "FAILED", "TIMED_OUT" -> FAILED;
                default -> null;
              };
              if (newJobState != null) {
                if (newJobState.isFinal())
                  JobService.callFinalizeObservers(job);

                Future<Void> future = Future.succeededFuture();
                if (newJobState == SUCCEEDED)
                  JobExecutor.getInstance().deleteExecution(job.getExecutionId());
                else if (newJobState == FAILED) {
                  if ("TIMED_OUT".equals(sfnStatus)) {
                    final String existingErrCause = job.getStatus().getErrorCause();
                    job.getStatus().setErrorCause(existingErrCause != null ? "Step timeout: " + existingErrCause : "Step timeout");
                  }
                  future = Future.all(job.getSteps().stepStream().filter(step -> step.getStatus().getState() == PENDING).map(pendingStep -> {
                    pendingStep.getStatus().setState(CANCELLING);
                    pendingStep.getStatus().setState(CANCELLED);
                    return job.storeUpdatedStep(pendingStep);
                  }).toList()).recover(t -> Future.succeededFuture()).mapEmpty();
                }

                State oldState = job.getStatus().getState();
                if (oldState != newJobState)
                  job.getStatus().setState(newJobState);

                return future.compose(v -> job.storeStatus(oldState));
              }
              else
                return Future.succeededFuture();
            })
            .onFailure(t -> logger.error("Error updating the state of job {} after receiving an event from its state machine:", jobId, t));
    }
    else
      logger.error("The state machine event does not include a detail field: {}", event);

    sendResponseWithXyzSerialization(context, NO_CONTENT, null);
  }

  private Step getStepFromBody(RoutingContext context) throws HttpException {
    return deserializeFromBody(context, Step.class);
  }

  private Job getJobFromBody(RoutingContext context) throws HttpException {
    return deserializeFromBody(context, Job.class);
  }

  private <T extends XyzSerializable> T deserializeFromBody(RoutingContext context, Class<T> type) throws HttpException {
    try {
      return XyzSerializable.deserialize(context.body().asString(), type);
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Error parsing request body", e);
    }
  }

  private static String jobId(RoutingContext context) {
    return context.pathParam(JOB_ID);
  }

  private static String stepId(RoutingContext context) {
    return context.pathParam("stepId");
  }

  private State getState(RoutingContext context) {
    String stateParamValue = ApiParam.getQueryParam(context, ApiParam.Query.STATE);
    return stateParamValue != null ? State.valueOf(stateParamValue) : null;
  }

  private String getResource(RoutingContext context) {
    return ApiParam.getQueryParam(context, ApiParam.Query.RESOURCE);
  }
}
