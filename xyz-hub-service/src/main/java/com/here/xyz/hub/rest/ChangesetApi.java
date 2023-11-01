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

package com.here.xyz.hub.rest;

import static com.here.xyz.events.PropertyQuery.QueryOperation.LESS_THAN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.SpaceConnectorBasedHandler;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.function.Function;
import org.apache.logging.log4j.Marker;

public class ChangesetApi extends SpaceBasedApi {

  public ChangesetApi(RouterBuilder rb) {
    rb.getRoute("getChangesets").setDoValidation(false).addHandler(this::getChangesets);
    rb.getRoute("getChangeset").setDoValidation(false).addHandler(this::getChangeset);
    rb.getRoute("deleteChangesets").setDoValidation(false).addHandler(this::deleteChangesets);
    rb.getRoute("getChangesetStatistics").setDoValidation(false).addHandler(this::getChangesetStatistics);
  }

  /**
   * Get changesets by version
   */
  private void getChangeset(final RoutingContext context) {
    try {
      IterateChangesetsEvent event = buildIterateChangesetsEvent(context, false);
      //TODO: Add static caching to this endpoint, once the execution pipelines have been refactored.
      SpaceConnectorBasedHandler.execute(Api.Context.getMarker(context),
                      space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space), event)
              .onSuccess(result -> sendResponse(context,result))
              .onFailure(t -> this.sendErrorResponse(context, t));

    } catch(HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Get changesets by version
   */
  private void getChangesets(final RoutingContext context) {
    try {
      IterateChangesetsEvent event = buildIterateChangesetsEvent(context, true);
      //TODO: Add static caching to this endpoint, once the execution pipelines have been refactored.
      SpaceConnectorBasedHandler.execute(Api.Context.getMarker(context),
              space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space), event)
              .onSuccess(result -> sendResponse(context,result))
              .onFailure(t -> this.sendErrorResponse(context, t));

    } catch(HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  private void sendResponse(final RoutingContext context, Object result){
    if(result instanceof Changeset && ((Changeset) result).getVersion() == -1){
      this.sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
    }else if(result instanceof ChangesetCollection && ((ChangesetCollection) result).getStartVersion() == -1 &&
          ((ChangesetCollection) result).getEndVersion() == -1){
      this.sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
    }else
      this.sendResponse(context, HttpResponseStatus.OK, result);
  }

  private IterateChangesetsEvent buildIterateChangesetsEvent(final RoutingContext context, final boolean useChangesetCollection) throws HttpException {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String pageToken = Query.getString(context, Query.PAGE_TOKEN, null);
    final long limit = Query.getLong(context, Query.LIMIT, 10_000L);

    final Long startVersion, endVersion;

    if(useChangesetCollection){
      startVersion = Query.getLong(context, Query.START_VERSION, null);
      endVersion = Query.getLong(context, Query.END_VERSION, null);
    }else{
      final String version = context.pathParam(Path.VERSION);
      startVersion = Long.parseLong(version);
      endVersion = startVersion;
    }

    validateGetChangesetsQueryParams(startVersion, endVersion, useChangesetCollection);

    return new IterateChangesetsEvent()
            .withSpace(spaceId)
            .withUseCollection(useChangesetCollection)
            .withStartVersion(startVersion)
            .withEndVersion(endVersion)
            .withPageToken(pageToken)
            .withLimit(limit);
  }

  private void validateGetChangesetsQueryParams(Long startVersion, Long endVersion, boolean useChangesetCollection)
          throws HttpException {
    if(useChangesetCollection){
      if(startVersion == null || endVersion == null)
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The parameters startVersion and endVersion are required.");
      if(startVersion < 0 || endVersion < 0)
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Invalid version specified.");
      if(startVersion > endVersion)
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The parameter startVersion needs to be smaller as endVersion.");
    }else{
      if(startVersion == null)
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "The parameters version is required.");
    }
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
      if (minVersion < 1)
        throw new NumberFormatException();

      SpaceConnectorBasedHandler.execute(Api.Context.getMarker(context),
              space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space),
              new DeleteChangesetsEvent()
                  .withSpace(spaceId)
                  .withRequestedMinVersion(minVersion))
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

  private void getChangesetStatistics(final RoutingContext context) {
    final Marker marker = Api.Context.getMarker(context);
    final Function<Space, Future<Space>> changesetAuthorization = space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space);
    final String spaceId = context.pathParam(Path.SPACE_ID);

    getChangesetStatistics(marker, changesetAuthorization, spaceId)
        .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK, result))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  public static Future<ChangesetsStatisticsResponse> getChangesetStatistics(Marker marker, Function<Space, Future<Space>> authorizationFunction, String spaceId) {
    return SpaceConnectorBasedHandler.execute(marker, authorizationFunction, new GetChangesetStatisticsEvent().withSpace(spaceId));
  }
}
