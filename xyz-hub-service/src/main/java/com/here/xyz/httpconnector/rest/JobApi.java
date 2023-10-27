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

package com.here.xyz.httpconnector.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.httpconnector.rest.HApiParam.HQuery;
import com.here.xyz.httpconnector.rest.HApiParam.HQuery.Command;
import com.here.xyz.httpconnector.rest.HApiParam.Path;
import com.here.xyz.httpconnector.task.JobHandler;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
      JobHandler.postJob(job, Api.Context.getMarker(context))
              .onFailure(e -> this.sendError(e, context))
              .onSuccess(j -> this.sendResponse(context, CREATED, j));
    }
    catch (HttpException e){
      this.sendErrorResponse(context, e);
    }
  }

  private void patchJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);

    try {
      Job job = HApiParam.HQuery.getJobInput(context);
      job.setId(jobId);

      JobHandler.patchJob(job, Api.Context.getMarker(context))
              .onFailure(e -> this.sendError(e, context))
              .onSuccess(j -> this.sendResponse(context, OK, j));
    }catch (HttpException e){
      this.sendErrorResponse(context, e);
    }
  }

  private void getJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);

    JobHandler.loadJob(jobId, Api.Context.getMarker(context))
            .onFailure(e -> this.sendError(e, context))
            .onSuccess(job -> this.sendResponse(context, OK, job));
  }

  private void getJobs(final RoutingContext context) {
    String jobType = HQuery.getJobType(context);
    Job.Status jobStatus = HQuery.getJobStatus(context);
    String targetSpaceId = HQuery.getString(context, HQuery.TARGET_SPACEID , null);

    JobHandler.loadJobs(Api.Context.getMarker(context), jobType, jobStatus, targetSpaceId)
            .onFailure(e -> this.sendError(e, context))
            .onSuccess(jobs -> this.sendResponse(context, OK, jobs));
  }

  private void deleteJob(final RoutingContext context) {
    String jobId = context.pathParam(Path.JOB_ID);
    boolean deleteData = HApiParam.HQuery.getBoolean(context, HQuery.DELETE_DATA, false);
    boolean force = HApiParam.HQuery.getBoolean(context, HApiParam.HQuery.FORCE, false);

    JobHandler.deleteJob(jobId, deleteData, force, Api.Context.getMarker(context))
            .onFailure(e -> this.sendError(e, context))
            .onSuccess(job -> this.sendResponse(context, OK, job));
  }

  private void postExecute(final RoutingContext context) {
    Command command = HQuery.getCommand(context);
    int urlCount = HQuery.getInteger(context, HQuery.URL_COUNT, 1);

    if(command == null) {
      this.sendErrorResponse(context, new HttpException(BAD_REQUEST, "Unknown command!"));
      return;
    }

    String jobId = context.pathParam(Path.JOB_ID);

    JobHandler.executeCommand(jobId, command, urlCount, Api.Context.getMarker(context))
            .onFailure(t -> {
              logger.info(Api.Context.getMarker(context),"[{}] can't execute command", jobId, t);
              this.sendErrorResponse(context, t);
            })
            .onSuccess(job -> {
              switch (command) {
                case START:
                case RETRY:
                case ABORT:
                  context.response().setStatusCode(NO_CONTENT.code()).end();
                  return;
                case CREATEUPLOADURL:
                  Import importJob = (Import)job;
                  try{
                    HashMap<String,Object> urlObj = new HashMap<>();
                    List<URL> urlList = new ArrayList<>();
                    int importObjectsSize = importJob.getImportObjects().size();

                    for(int i=urlCount; i>0; i--) {
                      String objectKey = "part_"+(importObjectsSize-i)+".csv";
                      urlList.add(importJob.getImportObjects().get(objectKey).getUploadUrl());
                    }
                    urlObj.put("urls", urlList);

                    // When 'urlCount=1', include "url" along with "urls", for backward compatibility
                    if(urlCount == 1)
                      urlObj.put("url",importJob.getImportObjects().get("part_"+(importObjectsSize-1)+".csv").getUploadUrl());

                    this.sendResponse(context, CREATED, urlObj);
                  }catch (Exception e){
                    logger.error(Api.Context.getMarker(context), "Unexpected Error during create CREATEUPLOADURL", e);
                    this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, "Unexpected Error!"));
                  }
              }
            });
  }

  private void sendError(Throwable e, RoutingContext context) {
    if (e instanceof HttpException)
      this.sendErrorResponse(context, e);
    else if (e instanceof AmazonDynamoDBException)
      this.sendErrorResponse(context, new HttpException(BAD_REQUEST, "Payload is wrong!"));
    else {
      logger.warn(Api.Context.getMarker(context), "Unexpected Error during saving a Job", e);
      this.sendErrorResponse(context, new HttpException(BAD_GATEWAY, e.getMessage()));
    }
  }
}
