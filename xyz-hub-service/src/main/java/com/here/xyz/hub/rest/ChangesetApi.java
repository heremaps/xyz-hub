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

import static com.here.xyz.events.PropertyQuery.QueryOperation.LESS_THAN;

import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.SpaceConnectorBasedHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.Marker;

public class ChangesetApi extends SpaceBasedApi {

  public ChangesetApi(RouterBuilder rb) {
    rb.operation("deleteChangesets").handler(this::deleteChangesets);
  }

  /**
   * Delete changesets by version number
   */
  private void deleteChangesets(final RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final PropertyQuery version = Query.getPropertyQuery(context.request().query(), "version", false);

    if (version == null || version.getValues().isEmpty()) {
      this.sendErrorResponse(context, new HttpException(HttpResponseStatus.BAD_REQUEST, "Query parameter version is required"));
      return;
    }
    else if (version.getOperation() != LESS_THAN) {
      this.sendErrorResponse(context, new HttpException(HttpResponseStatus.BAD_REQUEST, "Only lower-than is allowed as operation for query parameter version"));
      return;
    }

    try {
      long minVersion = Long.parseLong((String) version.getValues().get(0));
      if (minVersion <= 0)
        throw new NumberFormatException();

      SpaceConnectorBasedHandler.execute(context,
              new DeleteChangesetsEvent()
                  .withSpace(spaceId)
                  .withMinVersion(minVersion))
          .onSuccess(result -> {
            this.sendResponse(context, HttpResponseStatus.NO_CONTENT, null);
            Marker marker = Api.Context.getMarker(context);
            Service.spaceConfigClient.get(marker, spaceId)
                .compose(space -> Service.spaceConfigClient.store(marker, space.withMinVersion(minVersion)))
                .onSuccess(v -> logger.info(marker, "Updated minVersion for space {}", spaceId))
                .onFailure(t -> logger.error(marker, "Error while updating minVersion for space {}", spaceId, t));
          })
          .onFailure(t -> this.sendErrorResponse(context, t));
    }
    catch (NumberFormatException e) {
      this.sendErrorResponse(context, new HttpException(HttpResponseStatus.BAD_REQUEST, "Query parameter version must be a valid number larger than 0"));
    }
  }
}
