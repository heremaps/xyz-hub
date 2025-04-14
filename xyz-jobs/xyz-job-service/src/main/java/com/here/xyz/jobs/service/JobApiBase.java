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

import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Path.JOB_ID;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.NEWER_THAN;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.getQueryParam;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.JobApiBase.ApiParam.Query;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import java.util.List;

public class JobApiBase extends Api {

  protected static String jobId(RoutingContext context) {
    return context.pathParam(JOB_ID);
  }

  protected void getJobs(final RoutingContext context, boolean internal) {
    try {
      final String newerThan = getQueryParam(context, NEWER_THAN);
      Future<List<Job>> resultFuture = newerThan != null
          ? Job.load(true, Long.parseLong(newerThan))
          : Job.load(getState(context), getResource(context));
      resultFuture
          .onSuccess(res -> {
            if (internal)
              sendInternalResponse(context, OK.code(), res);
            else
              sendResponse(context, OK.code(), res);
          })
          .onFailure(err -> sendErrorResponse(context, err));
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("Illegal value for  " + NEWER_THAN + " parameter: " + NEWER_THAN, e);
    }
  }

  protected State getState(RoutingContext context) {
    String stateParamValue = getQueryParam(context, Query.STATE);
    return stateParamValue != null ? State.valueOf(stateParamValue) : null;
  }

  protected String getResource(RoutingContext context) {
    return getQueryParam(context, Query.RESOURCE);
  }

  public static class ApiParam {
    public static String getPathParam(RoutingContext context, String param) {
      return context.pathParam(param);
    }

    public static String getQueryParam(RoutingContext context, String param) {
      return context.queryParams().get(param);
    }

    public static class Path {
      static final String SPACE_ID = "spaceId";
      static final String JOB_ID = "jobId";
    }

    public static class Query {
      static final String STATE = "state";
      static final String RESOURCE = "resource";
      static final String NEWER_THAN = "newerThan";
    }
  }
}
