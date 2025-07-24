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
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.RESOURCE;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.NEWER_THAN;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.SOURCE_TYPE;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.STATE;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.TARGET_TYPE;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.Query.PROCESS_TYPE;
import static com.here.xyz.jobs.service.JobApiBase.ApiParam.getQueryParam;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.JobConfigClient.FilteredValues;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JobApiBase extends Api {

  protected static String jobId(RoutingContext context) {
    return context.pathParam(JOB_ID);
  }

  protected void getJobs(final RoutingContext context, boolean internal) {
    try {
      // resource and type are indexed
      final FilteredValues<String> resourceKeys = getFilteredValues(context, RESOURCE);
      final FilteredValues<Long> newerThan = getFilteredValues(context, NEWER_THAN);
      final FilteredValues<String> sourceTypes = getFilteredValues(context, SOURCE_TYPE);
      final FilteredValues<String>  targetTypes = getFilteredValues(context, TARGET_TYPE);
      final FilteredValues<String>  processTypes = getFilteredValues(context, PROCESS_TYPE);
      final FilteredValues<State>  stateTypes= getFilteredValues(context, STATE);

      Future<List<Job>> resultFuture;

      if(newerThan == null && sourceTypes == null && targetTypes == null && processTypes == null){
        Set<State> stateTypeValues = stateTypes == null ? Set.of() : stateTypes.values();
        Set<String> resourceKeyValues = resourceKeys == null ? Set.of() : resourceKeys.values();
        boolean includeStateType = stateTypes == null ? true : stateTypes.include();
        boolean includeResourceKeys = resourceKeys == null ? true : resourceKeys.include();

        if(stateTypeValues.size() > 1  || resourceKeyValues.size() > 1 || !includeStateType || !includeResourceKeys) {
          //index can`t get used
          resultFuture = Job.load(newerThan, sourceTypes, targetTypes, processTypes, resourceKeys, stateTypes);
        }else {
          //index can get used
          State state = stateTypes != null ? stateTypes.values().stream().findFirst().orElse(null) : null;
          String resourceKey = resourceKeys != null ? resourceKeys.values().stream().findFirst().orElse(null) : null;

          resultFuture = Job.load(state, resourceKey);
        }
      }else {
        //scan is needed
        resultFuture = Job.load(newerThan, sourceTypes, targetTypes, processTypes, resourceKeys, stateTypes);;
      }

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

  protected FilteredValues getFilteredValues(RoutingContext context, String param) {
    String paramValue = getQueryParam(context, param);
    if (paramValue == null || paramValue.isBlank())
      return  null;

    boolean include = true;

    if(paramValue.startsWith("!")) {
      paramValue = paramValue.substring(1);
      include = false;
    }

    HashSet<String> paramList = new HashSet<>((Arrays.stream(paramValue.split(",")).toList()));

    if(param.equals(NEWER_THAN)) {
      if(paramList.size() > 1)
        throw new IllegalArgumentException("Illegal value for " + NEWER_THAN + " parameter: " + paramValue);
      return new FilteredValues<Long>(include, new HashSet<>(paramList.stream().map(Long::parseLong).toList()));
    }if(param.equals(STATE))
      return new FilteredValues<State>(include, new HashSet<>(paramList.stream().map(State::valueOf).toList()));
    return new FilteredValues<String>(include, new HashSet<>(paramList));
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
      static final String SOURCE_TYPE = "sourceType";
      static final String TARGET_TYPE = "targetType";
      static final String PROCESS_TYPE = "processType";
    }
  }
}
