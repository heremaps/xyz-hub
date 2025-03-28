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

package com.here.xyz.hub.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.hub.connectors.models.Space.InvalidExtensionException;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTaskHandler.InvalidStorageException;
import com.here.xyz.hub.task.Task;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Ref.InvalidRef;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.vertx.ext.web.RoutingContext;
import java.util.Map;

public abstract class SpaceBasedApi extends Api {
  /**
   * The default limit for the number of features to load from the connector.
   */
  protected final static int DEFAULT_FEATURE_LIMIT = 30_000;
  protected final static int MIN_LIMIT = 1;
  protected final static int HARD_LIMIT = 100_000;

  protected static SpaceContext getSpaceContext(RoutingContext context) throws HttpException {
    SpaceContext spaceContext = SpaceContext.of(Query.getString(context, Query.CONTEXT, SpaceContext.DEFAULT.toString()).toUpperCase());
    if (spaceContext == null)
      throw new DetailedHttpException("E318403");
    return spaceContext;
  }

  /**
   * Send an error response to the client when an exception occurred while processing a task.
   *
   * @param task the task for which to return an error response.
   * @param e the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
   */
  @Override
  public void sendErrorResponse(final Task task, final Throwable e) {
    if (e instanceof InvalidStorageException) {
      super.sendErrorResponse(task.context, new HttpException(NOT_FOUND, "The resource definition contains an invalid storage ID."));
    }
    else if (e instanceof InvalidExtensionException) {
      super.sendErrorResponse(task.context, new HttpException(NOT_FOUND, e.getMessage()));
    }
    else {
      super.sendErrorResponse(task.context, e);
    }
  }

  /**
   * Returns the value of the limit parameter of a default value.
   */

  protected int getLimit(RoutingContext context, int defaultLimit ) throws HttpException {
    int limit = ApiParam.Query.getInteger(context, ApiParam.Query.LIMIT, defaultLimit);

    if (limit < MIN_LIMIT || limit > HARD_LIMIT) {
      throw new HttpException(BAD_REQUEST, "The parameter limit must be between " + MIN_LIMIT + " and " + HARD_LIMIT + ".");
    }
    return limit;
  }

  protected int getLimit(RoutingContext context) throws HttpException { return getLimit(context, DEFAULT_FEATURE_LIMIT ); }

  protected final String getSpaceId(RoutingContext context) {
    return context.pathParam(Path.SPACE_ID);
  }

  protected Ref getRef(RoutingContext context) throws HttpException {
    final String version = Query.getString(context, Query.VERSION, null);
    final String versionRef = Query.getString(context, Query.VERSION_REF, version);

    try {
      return new Ref(versionRef);
    }
    catch (InvalidRef e) {
      Map<String, String> placeholders = Map.of("versionRef", versionRef, "cause", e.getMessage());
      throw new DetailedHttpException("E318404", placeholders, e);
    }
  }
}
