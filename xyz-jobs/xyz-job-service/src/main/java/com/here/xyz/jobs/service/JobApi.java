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

import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeStatus.Action.CANCEL;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Path.SPACE_ID;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.getPathParam;
import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_INPUT_SET_NAME;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputsFromJob;
import com.here.xyz.jobs.steps.inputs.InputsFromS3;
import com.here.xyz.jobs.steps.inputs.ModelBasedInput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobApi extends JobApiBase {
  protected static final Logger logger = LogManager.getLogger();
  protected JobApi() {}

  public JobApi(RouterBuilder rb) {
    rb.getRoute("postJob").setDoValidation(false).addHandler(handleErrors(this::postJob));
    rb.getRoute("getJobs").setDoValidation(false).addHandler(handleErrors(this::getJobs));
    rb.getRoute("getJob").setDoValidation(false).addHandler(handleErrors(this::getJob));
    rb.getRoute("deleteJob").setDoValidation(false).addHandler(handleErrors(this::deleteJob));
    rb.getRoute("postJobInputs").setDoValidation(false).addHandler(handleErrors(this::postJobInput));
    rb.getRoute("getJobInputs").setDoValidation(false).addHandler(handleErrors(this::getJobInputs));
    rb.getRoute("postNamedJobInputs").setDoValidation(false).addHandler(handleErrors(this::postJobInput));
    rb.getRoute("getNamedJobInputs").setDoValidation(false).addHandler(handleErrors(this::getJobInputs));
    rb.getRoute("getJobOutputs").setDoValidation(false).addHandler(handleErrors(this::getJobOutputs));
    rb.getRoute("patchJobStatus").setDoValidation(false).addHandler(handleErrors(this::patchJobStatus));
    rb.getRoute("getJobStatus").setDoValidation(false).addHandler(handleErrors(this::getJobStatus));
  }

  protected void postJob(final RoutingContext context) throws HttpException {
    createNewJob(context, getJobFromBody(context));
  }

  protected Future<Job> createNewJob(RoutingContext context, Job job) {
    logger.info(getMarker(context), "Received job creation request: {}", job.serialize(true));
    return job.create().submit()
        .compose(v -> applyInputReferences(job))
        .map(res -> job)
        .recover(t -> {
          if (t instanceof CompilationError)
            return Future.failedFuture(new DetailedHttpException("E319002", t));
          if (t instanceof ValidationException)
            return Future.failedFuture(new DetailedHttpException("E319003", t));
          return Future.failedFuture(t);
        })
        .onSuccess(res -> {
          sendResponse(context, CREATED.code(), res);
          logger.info(getMarker(context), "Job was created successfully: {}", job.serialize(true));
        })
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected Future<Void> applyInputReferences(Job job) {
    if (job.getInputs() == null)
      return Future.succeededFuture();

    if (!job.getInputs().values().stream().allMatch(input -> input instanceof InputsFromS3))
      return Future.failedFuture("Only inputs of type " + InputsFromS3.class.getSimpleName() + " are supported as inline inputs.");

    //Continue with the input scanning *asynchronously* but fail the job if something goes wrong (User can check the status)
    Future.all(job.getInputs().entrySet().stream()
        .map(inputSet -> registerInput(job, (InputsFromS3) inputSet.getValue(), inputSet.getKey()))
        .toList())
        .onFailure(t -> {
          logger.error("[{}] Error while scanning inputs for job.", job.getId(), t);
          job.getStatus()
              .withState(FAILED)
              .withErrorMessage("Error while scanning inputs.")
              .withErrorCause(t.getMessage());
          job.store();
        })
        .compose(v -> job.submit());

    /*
    Return without waiting for the input scanning to complete.
    The job will stay in state NOT_READY for some time but will proceed automatically afterwards.
     */
    return Future.succeededFuture();
  }

  protected void getJobs(final RoutingContext context) {
    getJobs(context, false);
  }

  protected void getJob(final RoutingContext context) {
    loadJob(context, jobId(context))
        .onSuccess(res -> sendResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));

  }

  protected void deleteJob(final RoutingContext context) {
    String jobId = jobId(context);
    loadJob(context, jobId)
        .compose(job -> Job.delete(jobId).map(job))
        .onSuccess(res -> sendResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected void postJobInput(final RoutingContext context) throws HttpException {
    String jobId = jobId(context);
    Input input = getJobInputFromBody(context);
    String inputSetName = inputSetName(context);

    Future<Input> inputCreatedFuture = loadJob(context, jobId).compose(job -> registerInput(context, job, input, inputSetName));

    inputCreatedFuture
        .onSuccess(res -> sendResponse(context, CREATED.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private Future<Input> registerInput(RoutingContext context, Job job, Input input, String inputSetName) {
    if (input instanceof UploadUrl uploadUrl)
      return registerInput(job, inputSetName, uploadUrl);

    if (input instanceof InputsFromS3 s3Inputs)
      return registerInput(job, s3Inputs, inputSetName);

    if (input instanceof ModelBasedInput modelBasedInput)
      return registerPipelineInput(context, job, modelBasedInput);

    if (input instanceof InputsFromJob inputsReference)
      return registerInput(context, job, inputsReference);

    throw new NotImplementedException("Input type " + input.getClass().getSimpleName() + " is not supported.");
  }

  private static Future<Input> registerInput(Job job, String inputSetName, UploadUrl uploadUrl) {
    return job.getStatus().getState() == NOT_READY
        ? Future.succeededFuture(job.createUploadUrl(uploadUrl.isCompressed(), inputSetName))
        : Future.failedFuture(new DetailedHttpException("E319004"));
  }

  private Future<Input> registerInput(RoutingContext context, Job job, InputsFromJob inputsReference) {
    //NOTE: Both jobs have to be loaded to authorize the user for both ones
    return loadJob(context, inputsReference.getJobId()).compose(referencedJob -> {
      try {
        if (!Objects.equals(referencedJob.getOwner(), job.getOwner()))
          return Future.failedFuture(new DetailedHttpException("E319008", Map.of("referencedJob", inputsReference.getJobId(), "referencingJob", job.getId())));

        inputsReference.dereference(job.getId());
        return Future.succeededFuture();
      }
      catch (Exception e) {
        return Future.failedFuture(e);
      }
    });
  }

  private static Future<Input> registerPipelineInput(RoutingContext context, Job job, ModelBasedInput modelBasedInput) {
    if (!job.isPipeline())
      return Future.failedFuture(new DetailedHttpException("E319005", Map.of("allowedType", UploadUrl.class.getSimpleName())));
    else if (job.getStatus().getState() != RUNNING)
      return Future.failedFuture(new DetailedHttpException("E319006"));
    else if (context.request().bytesRead() > 256 * 1024)
      return Future.failedFuture(new DetailedHttpException("E319007"));
    else
      return job.consumeInput(modelBasedInput).map(null);
  }

  private static Future<Input> registerInput(Job job, InputsFromS3 s3Inputs, String inputSetName) {
    if (job.getStatus().getState() != NOT_READY)
      return Future.failedFuture(new DetailedHttpException("E319004"));
    s3Inputs.dereference(job.getId(), inputSetName);
    return Future.succeededFuture(null);
  }

  protected void getJobInputs(final RoutingContext context) {
    loadJob(context, jobId(context))
        .compose(job -> job.loadInputs(inputSetName(context)))
        .onSuccess(res -> sendResponse(context, OK.code(), res, new TypeReference<List<Input>>() {}))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected void getJobOutputs(final RoutingContext context) {
    loadJob(context, jobId(context))
        .compose(job -> job.loadOutputs())
        .onSuccess(res -> sendResponse(context, OK.code(), res, new TypeReference<List<Output>>() {}))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected void patchJobStatus(final RoutingContext context) throws HttpException {
    RuntimeStatus status = getStatusFromBody(context);
    loadJob(context, jobId(context))
        .compose(job -> tryExecuteAction(context, status, job))
        .onSuccess(patchedStatus -> sendResponse(context, ACCEPTED.code(), patchedStatus))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected void getJobStatus(final RoutingContext context) {
    loadJob(context, jobId(context))
        .onSuccess(res -> sendResponse(context, OK.code(), res.getStatus()))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  protected Job getJobFromBody(RoutingContext context) throws HttpException {
    try {
      Job job = XyzSerializable.deserialize(context.body().asString(), Job.class);

      String spaceId = getPathParam(context, SPACE_ID);

      if (spaceId != null && job.getSource() instanceof DatasetDescription.Space space)
        space.setId(spaceId);

      return job;
    }
    catch (JsonProcessingException e) {
      //TODO: Decide if we want to forward the cause to the user.
      //TODO: We should generally try to "unpack" the JsonProcessingException and see if the cause is a "user facing" exception. See: Api#sendErrorResponse(RoutingContext, Throwable)
      //e.g. an invalid versionRef(4,2) will end up here - without any indication for the user at the end
      throw new DetailedHttpException("E319001", e);
    }
  }

  protected Future<Job> loadJob(RoutingContext context, String jobId) {
    return Job.load(jobId)
        .compose(job -> {
          if (job == null)
            return Future.failedFuture(new DetailedHttpException("E319009", Map.of("jobId", jobId)));
          return authorizeAccess(context, job).map(job);
        });
  }

  protected Future<Void> authorizeAccess(RoutingContext context, Job job) {
    return Future.succeededFuture();
  }

  protected String inputSetName(RoutingContext context) {
    String setName = context.pathParam("setName");
    return setName == null ? DEFAULT_INPUT_SET_NAME : setName;
  }

  protected Input getJobInputFromBody(RoutingContext context) throws HttpException {
    try {
      try {
        return XyzSerializable.deserialize(context.body().asString(), Input.class);
      }
      catch (InvalidTypeIdException e) {
        Map<String, Object> jsonInput = XyzSerializable.deserialize(context.body().asString(), Map.class);
        throw new DetailedHttpException("E319010", Map.of("jsonType", jsonInput.get("type").toString(), "expectedType", Job.class.getSimpleName()), e);
      }
    }
    catch (JsonProcessingException e) {
      throw new DetailedHttpException("E319001", e);
    }
  }

  protected RuntimeStatus getStatusFromBody(RoutingContext context) throws HttpException {
    try {
      return XyzSerializable.deserialize(context.body().asString(), RuntimeStatus.class);
    }
    catch (JsonProcessingException e) {
      throw new DetailedHttpException("E319001", e);
    }
  }

  protected Future<RuntimeStatus> tryExecuteAction(RoutingContext context, RuntimeStatus status, Job job) {
    job.getStatus().setDesiredAction(status.getDesiredAction());
    return (switch (status.getDesiredAction()) {
      case START -> job.submit();
      case CANCEL -> job.cancel();
      case RESUME -> job.resume();
    })
        .onSuccess(actionExecuted -> {
          if (status.getDesiredAction() != CANCEL || actionExecuted) {
            job.getStatus().setDesiredAction(null);
            job.storeStatus(null);
          }
        })
        .map(res -> job.getStatus());
  }
}
