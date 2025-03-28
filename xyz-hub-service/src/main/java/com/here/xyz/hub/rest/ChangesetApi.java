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

package com.here.xyz.hub.rest;

import static com.here.xyz.events.PropertyQuery.QueryOperation.LESS_THAN;
import static com.here.xyz.hub.rest.ApiParam.Path.VERSION;
import static com.here.xyz.hub.rest.ApiParam.Query.END_VERSION;
import static com.here.xyz.hub.rest.ApiParam.Query.START_VERSION;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.SpaceConnectorBasedHandler;
import com.here.xyz.psql.query.IterateChangesets;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.Marker;

public class ChangesetApi extends SpaceBasedApi {

  public ChangesetApi(RouterBuilder rb) {
    rb.getRoute("getChangesets").setDoValidation(false).addHandler(handleErrors(this::getChangesets));
    rb.getRoute("getChangeset").setDoValidation(false).addHandler(handleErrors(this::getChangeset));
    rb.getRoute("deleteChangesets").setDoValidation(false).addHandler(handleErrors(this::deleteChangesets));
    rb.getRoute("getChangesetStatistics").setDoValidation(false).addHandler(handleErrors(this::getChangesetStatistics));
  }

  /**
   * Get changesets by version
   */
  private void getChangesets(final RoutingContext context) {
    long startVersion = getLongQueryParam(context, START_VERSION, 0);
    long endVersion = getLongQueryParam(context, END_VERSION, -1);

    if (endVersion != -1 && startVersion > endVersion)
      throw new IllegalArgumentException("The parameter \"" + START_VERSION + "\" needs to be smaller than or equal to \"" + END_VERSION + "\".");

    IterateChangesetsEvent event = buildIterateChangesetsEvent(context, startVersion, endVersion);
    //TODO: Add static caching to this endpoint, once the execution pipelines have been refactored.
    SpaceConnectorBasedHandler.execute(getMarker(context),
            space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space), event)
        .onSuccess(result -> sendResponse(context, result))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  /**
   * Get changesets by version
   */
  private void getChangeset(RoutingContext context) {
    long version = getVersionFromPathParam(context);
    IterateChangesetsEvent event = buildIterateChangesetsEvent(context, version, version);
    //TODO: Add static caching to this endpoint, once the execution pipelines have been refactored.
    SpaceConnectorBasedHandler.<IterateChangesetsEvent,ChangesetCollection>execute(getMarker(context),
            space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space), event)
        .onSuccess(result -> {
            ChangesetCollection changesets = (ChangesetCollection) result;
            if (changesets.getVersions().isEmpty())
             sendErrorResponse(context, new DetailedHttpException("E318443", Map.of("version", String.valueOf(version))));
            else
             sendResponse(context, changesets.getVersions().get(version).withNextPageToken(changesets.getNextPageToken()));

        })
        .onFailure(t -> sendErrorResponse(context, t));
  }

  /**
   * Delete changesets by version number
   */
  private void deleteChangesets(final RoutingContext context) throws HttpException {
    final String spaceId = getSpaceId(context);
    final PropertyQuery version = Query.getPropertyQuery(context.request().query(), "version", false);

    if (version == null || version.getValues().isEmpty()) {
      sendErrorResponse(context, new DetailedHttpException("E318405", Map.of("param", "version")));
      return;
    }
    else if (version.getOperation() != LESS_THAN) {
      sendErrorResponse(context,
          new HttpException(HttpResponseStatus.BAD_REQUEST, "Only lower-than is allowed as operation for query parameter version"));
      return;
    }

    try {
      long minVersion = Long.parseLong((String) version.getValues().get(0));
      if (minVersion < 1)
        throw new NumberFormatException();

      SpaceConnectorBasedHandler.execute(getMarker(context),
              space -> Authorization.authorizeManageSpacesRights(context, space.getId(), space.getOwner()).map(space),
              new DeleteChangesetsEvent()
                  .withSpace(spaceId)
                  .withRequestedMinVersion(minVersion))
          .onSuccess(result -> {
            sendResponse(context, HttpResponseStatus.NO_CONTENT, null);
            Marker marker = getMarker(context);
            Service.spaceConfigClient.get(marker, spaceId)
                .compose(space -> Service.spaceConfigClient.store(marker, space.withMinVersion(minVersion)))
                .onSuccess(v -> logger.info(marker, "Updated minVersion for space {}", spaceId))
                .onFailure(t -> logger.error(marker, "Error while updating minVersion for space {}", spaceId, t));
          })
          .onFailure(t -> sendErrorResponse(context, t));
    }
    catch (NumberFormatException e) {
      throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Query parameter version must be a valid number larger than 0");
    }
  }

  private void getChangesetStatistics(final RoutingContext context) {
    final Function<Space, Future<Space>> changesetAuthorization = space -> Authorization.authorizeManageSpacesRights(context, space.getId(),
        space.getOwner()).map(space);

    getChangesetStatistics(getMarker(context), changesetAuthorization, getSpaceId(context))
        .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK, result))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  private IterateChangesetsEvent buildIterateChangesetsEvent(final RoutingContext context, long startVersion, long endVersion) {
    String pageToken = Query.getString(context, Query.PAGE_TOKEN, null);
    long limit = Query.getLong(context, Query.LIMIT, IterateChangesets.DEFAULT_LIMIT);

    return new IterateChangesetsEvent()
        .withSpace(getSpaceId(context))
        .withStartVersion(startVersion)
        .withEndVersion(endVersion)
        .withPageToken(pageToken)
        .withLimit(limit);
  }

  private long getLongQueryParam(RoutingContext context, String paramName, long defaultValue) {
    try {
      long paramValue = Query.getLong(context, paramName);
      if (paramValue < 0)
        throw new IllegalArgumentException("The parameter \"" + paramName + "\" must be >= 0.");
      return paramValue;
    }
    catch (NullPointerException e) {
      return defaultValue;
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("The parameter \"" + paramName + "\" is not a number.", e);
    }
  }

  private void sendResponse(final RoutingContext context, Object result) {
    if (result instanceof Changeset && ((Changeset) result).getVersion() == -1)
      sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
    else if (result instanceof ChangesetCollection && ((ChangesetCollection) result).getStartVersion() == -1 &&
        ((ChangesetCollection) result).getEndVersion() == -1)
      sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
    else
      sendResponseWithXyzSerialization(context, HttpResponseStatus.OK, result);
  }

  private long getVersionFromPathParam(RoutingContext context) {
    String versionParamValue = context.pathParam(VERSION);
    if (versionParamValue == null)
      throw new IllegalArgumentException("The parameter \"" + VERSION + "\" is required.");

    try {
      return Long.parseLong(versionParamValue);
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("The parameter \"" + VERSION + "\" is not a number.", e);
    }
  }

  public static Future<ChangesetsStatisticsResponse> getChangesetStatistics(Marker marker,
      Function<Space, Future<Space>> authorizationFunction, String spaceId) {
    return SpaceConnectorBasedHandler.execute(marker, authorizationFunction, new GetChangesetStatisticsEvent().withSpace(spaceId));
  }
}
