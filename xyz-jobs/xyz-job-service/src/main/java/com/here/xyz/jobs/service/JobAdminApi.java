/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.execution.RunEmrJob.globalStepIdFromEmrJobName;
import static com.here.xyz.jobs.util.AwsClients.asyncSfnClient;
import static com.here.xyz.jobs.util.AwsClients.emrServerlessClient;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import software.amazon.awssdk.services.emrserverless.model.GetJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.JobRun;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;

public class JobAdminApi extends JobApiBase {
  private static final String ADMIN_JOBS = "/admin/jobs";
  private static final String ADMIN_JOB = ADMIN_JOBS + "/:jobId";
  private static final String ADMIN_JOB_STEPS = ADMIN_JOB + "/steps";
  private static final String ADMIN_JOB_STEP = ADMIN_JOB_STEPS + "/:stepId";
  private static final String ADMIN_STATE_MACHINE_EVENTS = "/admin/state/events";

  public JobAdminApi(Router router) {
    router.route(GET, ADMIN_JOBS).handler(handleErrors(this::getJobs));
    router.route(GET, ADMIN_JOB).handler(handleErrors(this::getJob));
    router.route(DELETE, ADMIN_JOBS).handler(handleErrors(this::deleteJob));
    router.route(POST, ADMIN_JOB_STEPS).handler(handleErrors(this::postStep));
    router.route(GET, ADMIN_JOB_STEP).handler(handleErrors(this::getStep));
    router.route(POST, ADMIN_STATE_MACHINE_EVENTS).handler(handleErrors(this::postStateEvent));
  }

  private void getJobs(RoutingContext context) {
    getJobs(context, true);
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
  private void postStateEvent(RoutingContext context) {
    JsonObject event = context.body().asJsonObject();

    if (event.containsKey("detail")) {
      String eventSource = event.getString("source");
      if ("aws.states".equals(eventSource))
        processSfnStateChangeEvent(event);
      else if ("aws.emr-serverless".equals(eventSource))
        processEmrJobStateChangeEvent(event);
      else
        logger.error("Unsupported event received: {}", event);
    }
    else
      logger.error("The event does not include a detail field: {}", event);

    sendResponseWithXyzSerialization(context, NO_CONTENT, null);
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
  private void processSfnStateChangeEvent(JsonObject event) {
    /*
    Right now we set JobId as "name" of the state machine execution.
    If for some reason it changes, we should add the jobId to the "input" param and read it from there.
     */
    JsonObject detail = event.getJsonObject("detail");
    String jobId = detail.getString("name");
    String sfnStatus = detail.getString("status");
    String executionArn = detail.getString("executionArn");

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

            Future<Void> future = Future.succeededFuture();
            if (newJobState != null) {
              if (newJobState == SUCCEEDED)
                JobExecutor.getInstance().deleteExecution(job.getExecutionId());
              else if (newJobState == FAILED) {
                if ("TIMED_OUT".equals(sfnStatus)) {
                  String existingErrCause = job.getStatus().getErrorCause();
                  job.getStatus().setErrorCause(existingErrCause != null ? "Step timeout: " + existingErrCause : "Step timeout");
                  //Set all RUNNING steps to CANCELLED, because the steps themselves might not have been informed
                  future = future.compose(v -> loadCausingStepId(executionArn)
                      .compose(causingStepId -> failStep(job, job.getStepById(causingStepId))))
                      .compose(v -> cancelSteps(job, RUNNING));
                }
                //Set all PENDING steps to CANCELLED
                future = future.compose(v -> cancelSteps(job, PENDING));
              }

              State oldState = job.getStatus().getState();
              if (oldState != newJobState) {
                job.getStatus().setState(newJobState);
              }

              future = future.compose(v -> job.storeStatus(oldState));
            }

            //Call finalize observers after setting the new state to the job status
            if (job.getStatus().getState().isFinal())
              JobService.callFinalizeObservers(job);

            return future;
          })
          .onFailure(t -> logger.error("Error updating the state of job {} after receiving an event from its state machine:", jobId, t));
  }

  /**
   * Fetches the execution history for the provided executionArn and goes back in the event history
   * until hitting "TaskStateEntered".
   * Then extracts the causing step ID from stateEnteredEventDetails.name field.
   *
   * @param executionArn The execution ARN of the state machine
   * @return The ID of the causing step if found, `null` otherwise
   */
  private static Future<String> loadCausingStepId(String executionArn) {
    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(GetExecutionHistoryRequest.builder()
            .executionArn(executionArn)
            .build()))
        .compose(executionHistory -> {
          List<HistoryEvent> events = executionHistory.events();
          HistoryEvent failingEvent = events.get(events.size() - 1);
          while (failingEvent != null && failingEvent.previousEventId() > 0 && !"TaskStateEntered".equals(failingEvent.type().toString())) {
            long causingEventId = failingEvent.previousEventId();
            failingEvent = events.stream().filter(event -> event.id().equals(causingEventId)).findAny().orElse(null);
          }
          return Future.succeededFuture(failingEvent != null && !"TaskStateEntered".equals(failingEvent.type().toString())
              && failingEvent.stateEnteredEventDetails() != null && failingEvent.stateEnteredEventDetails().name().contains(".")
              ? failingEvent.stateEnteredEventDetails().name() : null);
        });
  }

  private static Future<Void> cancelSteps(Job job, State currentState) {
    return Future.all(job.getSteps().stepStream().filter(step -> step.getStatus().getState() == currentState).map(step -> {
      step.getStatus().setState(CANCELLING);
      step.getStatus().setState(CANCELLED);
      return job.storeUpdatedStep(step);
    }).toList()).recover(t -> Future.succeededFuture()).mapEmpty();
  }

  private static Future<Void> failStep(Job job, Step step) {
    if (step.getStatus().getState().isFinal())
      return Future.succeededFuture();
    step.getStatus().setState(FAILED);
    return job.storeUpdatedStep(step);
  }

  /**
   * {
   *   "version": "0",
   *   "id": "00df3ec6-5da1-36e6-ab71-20f0de68f8a0",
   *   "detail-type": "EMR Serverless Job Run State Change",
   *   "source": "aws.emr-serverless",
   *   "account": "123456789012",
   *   "time": "2022-05-31T21:07:42Z",
   *   "region": "us-east-1",
   *   "resources": [],
   *   "detail": {
   *     "jobRunId": "00f1cbn5g4bb0c01",
   *     "jobRunName": "emr-job-run-name"
   *     "applicationId": "00f1982r1uukb925",
   *     "arn": "arn:aws:emr-serverless:us-east-1:123456789012:/applications/00f1982r1uukb925/jobruns/00f1cbn5g4bb0c01",
   *     "releaseLabel": "emr-6.6.0",
   *     "state": "RUNNING",
   *     "previousState": "SCHEDULED",
   *     "createdBy": "arn:aws:sts::123456789012:assumed-role/TestRole-402dcef3ad14993c15d28263f64381e4cda34775/6622b6233b6d42f59c25dd2637346242",
   *     "updatedAt": "2022-05-31T21:07:42.299487Z",
   *     "createdAt": "2022-05-31T21:07:25.325900Z"
   *   }
   * }
   */
  private void processEmrJobStateChangeEvent(JsonObject event) {
    String emrApplicationId = event.getJsonObject("detail").getString("applicationId");
    String emrJobName = event.getJsonObject("detail").getString("jobRunName");
    String emrJobRunId = event.getJsonObject("detail").getString("jobRunName");
    String emrJobStatus = event.getJsonObject("detail").getString("state");
    String globalStepId = globalStepIdFromEmrJobName(emrJobName);

    if (globalStepId != null) {
      String[] globalStepIdParts = globalStepId.split("\\.");

      if (globalStepIdParts.length != 2) {
        logger.error("The emrJobRunName does not have a valid globalStepId EMR job name was: {}", emrJobName);
        return;
      }

      String jobId = globalStepIdParts[0];
      String stepId = globalStepIdParts[1];

      loadJob(jobId)
          .compose(job -> {
            State newStepState = switch (emrJobStatus) {
              case "RUNNING" -> RUNNING;
              case "SUCCESS" -> SUCCEEDED;
              case "CANCELLING" -> CANCELLING;
              case "CANCELLED" -> CANCELLED;
              case "FAILED" -> FAILED;
              default -> null;
            };

            RuntimeInfo status = new RuntimeInfo().withState(newStepState);
            if (newStepState == FAILED)
              populateEmrErrorInformation(emrApplicationId, emrJobName, emrJobRunId, status);
            return job.updateStepStatus(stepId, status, false);
          });
    }
    else
      logger.error("The EMR job {} - {} is not associated with a step", emrJobName, emrJobRunId);
  }

  private void populateEmrErrorInformation(String emrApplicationId, String emrJobName, String emrJobRunId, RuntimeInfo status) {
    JobRun jobRun = emrServerlessClient().getJobRun(GetJobRunRequest.builder()
        .applicationId(emrApplicationId)
        .jobRunId(emrJobRunId)
        .build()).jobRun();

    status.setErrorMessage("EMR Job " + emrJobName + " with run ID " + emrJobRunId + " failed.");
    status.setErrorCause(jobRun.stateDetails());
    //TODO: Set error code
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

  private static String stepId(RoutingContext context) {
    return context.pathParam("stepId");
  }
}
