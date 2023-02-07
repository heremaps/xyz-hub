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

import com.here.xyz.hub.config.ReaderConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.models.hub.Reader;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.Marker;

public class ReaderApi extends SpaceBasedApi {

  public ReaderApi(RouterBuilder rb) {
    rb.operation("putReader").handler(this::putReader);
    rb.operation("deleteReader").handler(this::deleteReader);
    rb.operation("getReaderVersion").handler(this::getReaderVersion);
    rb.operation("increaseReaderVersion").handler(this::increaseReaderVersion);
  }

  private void putReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String readerId = Query.getString(context, Query.READER_ID, null);

    registerReader(Api.Context.getMarker(context), spaceId, readerId)
        .onSuccess(result ->this.sendResponse(context, HttpResponseStatus.OK, result))
        .onFailure(t -> this.sendErrorResponse(context, t instanceof HttpException
            ? t
            : new HttpException(HttpResponseStatus.BAD_REQUEST, t.getMessage())));
  }

  private void deleteReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  private void getReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  private void increaseReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  public static Future<Reader> registerReader(Marker marker, String spaceId, String readerId) {
    if (spaceId == null || readerId == null)
      return Future.failedFuture("Invalid parameters");

    final Future<Space> spaceFuture = SpaceConfigClient.getInstance().get(marker, spaceId)
        .flatMap(s -> s == null ? Future.failedFuture("Resource with id " + spaceId + " not found.") : Future.succeededFuture(s));
    final Future<Long> versionFuture = Future.succeededFuture(1L); // TODO maxversion

    return CompositeFuture.all(spaceFuture, versionFuture).flatMap(cf -> {
      final Reader reader = new Reader().withId(readerId).withSpaceId(spaceId).withVersion(versionFuture.result());
      return ReaderConfigClient.getInstance().storeReader(marker, reader).map(v -> reader);
    });
  }
}
