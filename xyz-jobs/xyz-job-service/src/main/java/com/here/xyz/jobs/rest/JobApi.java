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

package com.here.xyz.jobs.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.util.diff.Difference;
import com.here.xyz.util.diff.Patcher;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.Api;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;

import java.util.Map;

import static com.here.xyz.jobs.rest.JobApi.ApiParam.Path.JOB_ID;
import static com.here.xyz.jobs.rest.JobApi.ApiParam.Path.SPACE_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class JobApi extends Api {

    private static Map<String,String> MODIFICATION_IGNORE_MAP = ImmutableMap.of(
            "createdAt","createdAt",
            "updatedAt","updatedAt");

    public JobApi(RouterBuilder rb) {
        rb.getRoute("postJob").setDoValidation(false).addHandler(handleErrors(this::postJob));
        rb.getRoute("getJobs").setDoValidation(false).addHandler(handleErrors(this::getJobs));
        rb.getRoute("patchJob").setDoValidation(false).addHandler(handleErrors(this::patchJob));
        rb.getRoute("getJob").setDoValidation(false).addHandler(handleErrors(this::getJob));
        rb.getRoute("deleteJob").setDoValidation(false).addHandler(handleErrors(this::deleteJob));
        rb.getRoute("postJobInputs").setDoValidation(false).addHandler(handleErrors(this::postJobInputs));
        rb.getRoute("getJobInputs").setDoValidation(false).addHandler(handleErrors(this::getJobInputs));
        rb.getRoute("getJobOutputs").setDoValidation(false).addHandler(handleErrors(this::getJobOutputs));
        rb.getRoute("patchJobStatus").setDoValidation(false).addHandler(handleErrors(this::patchJobStatus));
        rb.getRoute("getJobStatus").setDoValidation(false).addHandler(handleErrors(this::getJobStatus));
    }

    private void postJob(final RoutingContext context) {
        Job job = getJobFromBody(context);
        job.submit()
                .map(res -> job)
                .onSuccess(res -> sendResponse(context, CREATED, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void getJobs(final RoutingContext context) {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        JobConfigClient.getInstance().loadJobs(spaceId)
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void patchJob(final RoutingContext context) {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        String jobId = ApiParam.getPathParam(context, JOB_ID);
        Job job = getJobFromBody(context);
        loadJob(spaceId, jobId)
                .compose(loadedJob -> {
                    if(loadedJob.getStatus().getState() != RuntimeInfo.State.NOT_READY)
                        return Future.failedFuture(new HttpException(HttpResponseStatus.CONFLICT, "The requested job cannot be updated"));
                    Job patchedJob = patchJobDiff(loadedJob, job);
                    return patchedJob.submit().map(res -> patchedJob);
                })
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void getJob(final RoutingContext context) {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        String jobId = ApiParam.getPathParam(context, JOB_ID);
        loadJob(spaceId, jobId)
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));

    }

    private void deleteJob(final RoutingContext context) {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        String jobId = ApiParam.getPathParam(context, JOB_ID);
        loadJob(spaceId, jobId)
                .compose(job -> JobConfigClient.getInstance().deleteJob(spaceId, jobId).map(job))
                .onSuccess(res -> sendResponse(context, OK, res))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void postJobInputs(final RoutingContext context) {
        //TODO: Needs to be Implememnted
    }

    private void getJobInputs(final RoutingContext context) {
        //TODO: Needs to be Implememnted
    }

    private void getJobOutputs(final RoutingContext context) {
        //TODO: Needs to be Implememnted
    }
    private void patchJobStatus(final RoutingContext context) throws HttpException {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        String jobId = ApiParam.getPathParam(context, JOB_ID);
        RuntimeStatus status = getStatusFromBody(context);
        loadJob(spaceId, jobId)
                .compose(job -> tryExecuteAction(status, job))
                .onSuccess(res -> sendResponse(context, OK, res.getStatus()))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    private void getJobStatus(final RoutingContext context) {
        String spaceId = ApiParam.getPathParam(context, SPACE_ID);
        String jobId = ApiParam.getPathParam(context, JOB_ID);
        loadJob(spaceId, jobId)
                .onSuccess(res -> sendResponse(context, OK, res.getStatus()))
                .onFailure(err -> sendErrorResponse(context, err));
    }

    public static class ApiParam {

        public static class Path {
            static final String SPACE_ID = "spaceId";
            static final String JOB_ID = "jobId";
        }

        public static class Query {

        }
        public static String getPathParam(RoutingContext context, String param) {
            return context.pathParam(param);
        }

    }

    private Job getJobFromBody(RoutingContext context) {
        JsonObject body = context.body().asJsonObject();
        Job job = XyzSerializable.fromMap(body.getMap());

        String spaceId = ApiParam.getPathParam(context, SPACE_ID);

        if(job.getSource() instanceof DatasetDescription.Space space)
            space.setId(spaceId);

        return job;
    }

    private Job patchJobDiff(Job job, Job patch) {
        Map jobMap = XyzSerializable.toMap(job);
        Map patchMap = XyzSerializable.toMap(patch);
        Difference.DiffMap diffMap = (Difference.DiffMap) Patcher.calculateDifferenceOfPartialUpdate(jobMap, patchMap, MODIFICATION_IGNORE_MAP,
                true);
        if (diffMap == null) {
            Patcher.patch(jobMap, diffMap);
            job = (Job) XyzSerializable.fromMap(jobMap);
        }
        return job;
    }

    private RuntimeStatus getStatusFromBody(RoutingContext context) throws HttpException {
        try {
            return XyzSerializable.deserialize(context.body().asString(), RuntimeStatus.class);
        }
        catch (JsonProcessingException e) {
            throw new HttpException(BAD_REQUEST, "Error parsing request");
        }
    }

    private Future<Job> loadJob(String spaceId, String jobId) {
        return JobConfigClient.getInstance().loadJob(spaceId, jobId)
                .compose(job -> {
                    if(job == null)
                        return Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "The requested job does not exist"));
                    return Future.succeededFuture(job);
                });
    }

    private static Future<Job> tryExecuteAction(RuntimeStatus status, Job job) {
        job.getStatus().setDesiredAction(status.getDesiredAction());
        return (switch (status.getDesiredAction()) {
                    case START -> job.submit();
                    case CANCEL -> job.cancel();
                    case RESUME -> job.resume();
        }).map(res -> job)
        .onSuccess(executedJob -> {
            executedJob.getStatus().setDesiredAction(null);
            executedJob.store();
        });
    }


}
