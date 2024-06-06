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

import static com.here.xyz.jobs.RuntimeStatus.Action.CANCEL;
import static com.here.xyz.jobs.service.JobApi.ApiParam.Path.JOB_ID;
import static com.here.xyz.jobs.service.JobApi.ApiParam.Path.SPACE_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

public class JobApi extends Api {

  public JobApi(RouterBuilder rb) {
    rb.getRoute("postJob").setDoValidation(false).addHandler(handleErrors(this::postJob));
    rb.getRoute("getJobs").setDoValidation(false).addHandler(handleErrors(this::getJobs));
    rb.getRoute("getJob").setDoValidation(false).addHandler(handleErrors(this::getJob));
    rb.getRoute("deleteJob").setDoValidation(false).addHandler(handleErrors(this::deleteJob));
    rb.getRoute("postJobInputs").setDoValidation(false).addHandler(handleErrors(this::postJobInput));
    rb.getRoute("getJobInputs").setDoValidation(false).addHandler(handleErrors(this::getJobInputs));
    rb.getRoute("getJobOutputs").setDoValidation(false).addHandler(handleErrors(this::getJobOutputs));
    rb.getRoute("patchJobStatus").setDoValidation(false).addHandler(handleErrors(this::patchJobStatus));
    rb.getRoute("getJobStatus").setDoValidation(false).addHandler(handleErrors(this::getJobStatus));
  }

  private void postJob(final RoutingContext context) throws HttpException {
    Job job = getJobFromBody(context);
    job.create().submit()
        .map(res -> job)
        .onSuccess(res -> sendResponse(context, CREATED.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getJobs(final RoutingContext context) {
    Job.loadAll()
        .onSuccess(res -> sendResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getJob(final RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, JOB_ID);
    loadJob(jobId)
        .onSuccess(res -> sendResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));

  }

  private void deleteJob(final RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, JOB_ID);
    loadJob(jobId)
        .compose(job -> Job.delete(jobId).map(job))
        .onSuccess(res -> sendResponse(context, OK.code(), res))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void postJobInput(final RoutingContext context) throws HttpException {
    String jobId = ApiParam.getPathParam(context, JOB_ID);
    Input input = getJobInputFromBody(context);

    if (input instanceof UploadUrl) {
      loadJob(jobId)
          .compose(job -> job.getStatus().getState() == State.NOT_READY
              ? Future.succeededFuture(job)
              : Future.failedFuture(new HttpException(BAD_REQUEST, "No inputs can be created after a job was submitted.")))
          .map(job -> job.createUploadUrl())
          .onSuccess(res -> sendResponse(context, CREATED.code(), res))
          .onFailure(err -> sendErrorResponse(context, err));
    }
    else
      throw new NotImplementedException("Input type " + input.getClass().getSimpleName() + " is not supported");
  }

  private void getJobInputs(final RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, JOB_ID);

    loadJob(jobId)
        .compose(job -> job.loadInputs())
        .onSuccess(res -> sendResponse(context, OK.code(), res, new TypeReference<List<Input>>() {}))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getJobOutputs(final RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, JOB_ID);

    loadJob(jobId)
        .compose(job -> job.loadOutputs())
        .onSuccess(res -> sendResponse(context, OK.code(), res, new TypeReference<List<Output>>() {}))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void patchJobStatus(final RoutingContext context) throws HttpException {
    String jobId = ApiParam.getPathParam(context, JOB_ID);
    RuntimeStatus status = getStatusFromBody(context);
    loadJob(jobId)
        .compose(job -> tryExecuteAction(status, job))
        .onSuccess(patchedStatus -> sendResponse(context, ACCEPTED.code(), patchedStatus))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private void getJobStatus(final RoutingContext context) {
    String jobId = ApiParam.getPathParam(context, JOB_ID);
    loadJob(jobId)
        .onSuccess(res -> sendResponse(context, OK.code(), res.getStatus()))
        .onFailure(err -> sendErrorResponse(context, err));
  }

  private Job getJobFromBody(RoutingContext context) throws HttpException {
    try {
      Job job = XyzSerializable.deserialize(context.body().asString(), Job.class);

      String spaceId = ApiParam.getPathParam(context, SPACE_ID);

      if (spaceId != null && job.getSource() instanceof DatasetDescription.Space space)
        space.setId(spaceId);

      return job;
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Error parsing request", e);
    }
  }

  private Future<Job> loadJob(String jobId) {
    return Job.load(jobId)
        .compose(job -> {
          if (job == null)
            return Future.failedFuture(new HttpException(NOT_FOUND, "The requested job does not exist"));
          //TODO: Auth
          return Future.succeededFuture(job);
        });
  }

  private Input getJobInputFromBody(RoutingContext context) throws HttpException {
    try {
      return XyzSerializable.deserialize(context.body().asString(), Input.class);
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Error parsing request", e);
    }
  }

  private RuntimeStatus getStatusFromBody(RoutingContext context) throws HttpException {
    try {
      return XyzSerializable.deserialize(context.body().asString(), RuntimeStatus.class);
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Error parsing request", e);
    }
  }

  private static Future<RuntimeStatus> tryExecuteAction(RuntimeStatus status, Job job) {
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

  public static class ApiParam {

    public static String getPathParam(RoutingContext context, String param) {
      return context.pathParam(param);
    }

    public static class Path {

      static final String SPACE_ID = "spaceId";
      static final String JOB_ID = "jobId";
    }

    public static class Query {

    }

  }
}
