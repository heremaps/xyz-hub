/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.httpconnector.rest;

import com.here.xyz.httpconnector.rest.HApiParam.HQuery;
import com.here.xyz.httpconnector.rest.HApiParam.HQuery.Command;
import com.here.xyz.httpconnector.rest.HApiParam.Path;
import com.here.xyz.httpconnector.task.ImportHandler;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class JobApi extends Api {

  public JobApi(RouterBuilder rb) {
    rb.operation("postJob").handler(this::postJob);
    rb.operation("patchJob").handler(this::patchJob);
    rb.operation("getJobs").handler(this::getJobs);
    rb.operation("getJob").handler(this::getJob);
    rb.operation("deleteJob").handler(this::deleteJob);
    rb.operation("postExecute").handler(this::postExecute);
  }

  private void postJob(final RoutingContext context) {
    try {
      Job job = HApiParam.HQuery.getJobInput(context);
      ImportHandler.postJob(job, Api.Context.getMarker(context))
              .onFailure(t -> this.sendErrorResponse(context, t))
              .onSuccess(j -> this.sendResponse(context, OK, j));
    }catch (HttpException e){
      this.sendErrorResponse(context, e);
    }
  }

  private void patchJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);

    try {
      Job job = HApiParam.HQuery.getJobInput(context);
      job.setId(jobId);

      ImportHandler.patchJob(job, Api.Context.getMarker(context))
              .onFailure(t -> this.sendErrorResponse(context, t))
              .onSuccess(j -> this.sendResponse(context, OK, j));
    }catch (HttpException e){
      this.sendErrorResponse(context, e);
    }
  }

  private void getJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);

    ImportHandler.getJob(jobId, Api.Context.getMarker(context))
            .onFailure(t -> this.sendErrorResponse(context, t))
            .onSuccess(job -> this.sendResponse(context, OK, job));
  }

  private void getJobs(final RoutingContext context) {
    Job.Type jobType = HQuery.getJobType(context);
    Job.Status jobStatus = HQuery.getJobStatus(context);
    String targetSpaceId = HQuery.getString(context, HQuery.TARGET_SPACEID , null);

    ImportHandler.getJobs( Api.Context.getMarker(context), jobType, jobStatus, targetSpaceId)
            .onFailure(t -> this.sendErrorResponse(context, t))
            .onSuccess(jobs -> this.sendResponse(context, OK, jobs));
  }

  private void deleteJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);

    ImportHandler.deleteJob(jobId, Api.Context.getMarker(context))
            .onFailure(t -> this.sendErrorResponse(context, t))
            .onSuccess(job -> this.sendResponse(context, OK, job));
  }

  private void postExecute(final RoutingContext context) {
    Command command = HQuery.getCommand(context);
    boolean enableHashedSpaceId = HQuery.getBoolean(context, HApiParam.HQuery.ENABLED_HASHED_SPACE_ID , true);
    boolean enableUUID = HQuery.getBoolean(context, HQuery.ENABLED_UUID , true);
    String[] params = HApiParam.HQuery.parseMainParams(context);

    if(command == null) {
      this.sendErrorResponse(context, new HttpException(BAD_REQUEST, "Unknown command!"));
      return;
    }

    String jobId = context.pathParam(Path.JOB_ID);

    ImportHandler.postExecute(jobId, params[0], params[1], params[2], command, enableHashedSpaceId, enableUUID, Api.Context.getMarker(context))
            .onFailure(t -> this.sendErrorResponse(context, t))
            .onSuccess(job -> {

              switch (command){
                case START:
                case RETRY:
                case ABORT:
                  context.response().setStatusCode(NO_CONTENT.code()).end();
                  return;
                case CREATEUPLOADURL:
                  context.response().setStatusCode(NO_CONTENT.code()).end();
              }
            });
  }
}
