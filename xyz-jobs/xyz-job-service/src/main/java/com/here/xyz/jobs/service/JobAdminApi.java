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

import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.JobAdminApi.ApiParam.Path;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class JobAdminApi extends Api {
  private static final String ADMIN_JOB_STEPS = "/admin/jobs/:jobId/steps";
  private static final String ADMIN_JOB_STEP_ID = "/admin/jobs/:jobId/steps/:stepId";
  private static final String ADMIN_STATE_MACHINE_EVENTS = "/admin/state/events";

  public JobAdminApi(Router router) {
    router.route(HttpMethod.POST, ADMIN_JOB_STEPS)
        .handler(handleErrors(this::postStep));

    router.route(HttpMethod.GET, ADMIN_JOB_STEP_ID)
        .handler(handleErrors(this::getStep));

    router.route(HttpMethod.POST, ADMIN_STATE_MACHINE_EVENTS)
        .handler(handleErrors(this::postStateEvent));
  }

  private void postStep(RoutingContext context) throws HttpException {
    String jobId = ApiParam.getPathParam(context, Path.JOB_ID);
    Step step = getStepFromBody(context);
    Job.load(jobId)
        .compose(job -> job.updateStep(step).mapEmpty())
        .onSuccess(v -> sendResponseWithXyzSerialization(context, OK, v))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getStep(RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, Path.JOB_ID);
    String stepId = ApiParam.getPathParam(context, Path.STEP_ID);
    Job.load(jobId)
        .compose(job -> {
          Step step = job.getStepById(stepId);
          if (step == null)
            return Future.failedFuture(new HttpException(NOT_FOUND, "Step is not present in the job"));
          return Future.succeededFuture(step);
        })
        .onSuccess(res -> sendResponse(context, OK, res))
        .onFailure(err -> sendErrorResponse(context, err));
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
      String status = event.getJsonObject("detail").getString("status");

      if (jobId == null)
        logger.error("The state machine event does not include a Job ID: {}", event);
      else if (status == null)
        logger.error("The state machine event does not include a status: {}", event);
      else
        Job.load(jobId)
            .compose(job -> {
              State newJobState = switch (status) {
                case "SUCCEEDED" -> SUCCEEDED;
                case "FAILED" -> FAILED;
                case "TIMED_OUT" -> FAILED;
                default -> null;
              };
              if (newJobState != null) {
                job.getStatus().setState(newJobState);
                return job.store();
              }
              else
                return Future.succeededFuture();
            })
            .onFailure(t -> logger.error("Error updating the state of job {} after receiving an event from its state machine:", t));
    }
    else
      logger.error("The state machine event does not include a detail field: {}", event);
  }

  private Step getStepFromBody(RoutingContext context) throws HttpException {
    try {
      return XyzSerializable.deserialize(context.body().asString(), Step.class);
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Error parsing request");
    }
  }

  public static class ApiParam {

    public static String getPathParam(RoutingContext context, String param) {
      return context.pathParam(param);
    }

    public static class Path {

      static final String JOB_ID = "jobId";
      static final String STEP_ID = "stepId";
    }

    public static class Query {

    }

  }
}
