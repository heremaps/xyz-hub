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

package com.here.xyz.hub.rest;

import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.RevisionEvent;
import com.here.xyz.events.RevisionEvent.Operation;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.RevisionHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;

public class RevisionApi extends SpaceBasedApi {

  public RevisionApi(RouterBuilder rb) {
    rb.operation("deleteRevisions").handler(this::deleteRevisions);
  }

  /**
   * Delete revisions by revision number
   */
  private void deleteRevisions(final RoutingContext context) {
    final String space = context.pathParam(Path.SPACE_ID);
    final PropertyQuery revision = Query.getPropertyQuery(context.request().query(), Query.REV, false);

    Future<Void> future = revision != null
        ? Future.succeededFuture()
        : Future.failedFuture(new HttpException(HttpResponseStatus.BAD_REQUEST, "Query parameter rev is required"));

    future.map((nothing) -> RevisionHandler.execute(new RevisionEvent()
        .withSpace(space)
        .withRevision(revision)
        .withOperation(Operation.DELETE)))
      .onSuccess(result -> this.sendResponse(context, HttpResponseStatus.NO_CONTENT, null))
      .onFailure((ex) -> this.sendErrorResponse(context, ex));
  }
}
