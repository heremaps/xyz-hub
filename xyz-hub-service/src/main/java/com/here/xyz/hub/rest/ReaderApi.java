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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.config.ReaderConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.models.hub.Reader;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
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

  // TODO auth
  private void putReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String readerId = context.pathParam(Path.READER_ID);

    registerReader(Api.Context.getMarker(context), spaceId, readerId)
        .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK, result))
        .onFailure(t -> sendHttpErrorResponse(context, t));
  }

  // TODO auth
  private void deleteReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String readerId = Query.getString(context, Query.READER_ID, null);

    deregisterReader(Api.Context.getMarker(context), spaceId, readerId)
        .flatMap(result -> result == null
            ? Future.succeededFuture()
            : Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Reader not found")))
        .onSuccess(none -> sendResponse(context, HttpResponseStatus.NO_CONTENT, null))
        .onFailure(t -> sendHttpErrorResponse(context, t));
  }

  // TODO auth
  private void getReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String readerId = Query.getString(context, Query.READER_ID, null);
    final Marker marker = Api.Context.getMarker(context);

    ReaderConfigClient.getInstance().getReader(marker, readerId, spaceId)
        .flatMap(r -> r == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Reader " + readerId + " with space " + spaceId + " not found")) : Future.succeededFuture(r))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK, r))
        .onFailure(t -> sendHttpErrorResponse(context, t));

  }

  // TODO auth
  private void increaseReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String readerId = Query.getString(context, Query.READER_ID, null);
    final Marker marker = Api.Context.getMarker(context);

    final Future<Space> spaceFuture = SpaceConfigClient.getInstance().get(marker, spaceId)
        .flatMap(s -> s == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Resource with id " + spaceId + " not found.")) : Future.succeededFuture(s));
    final Future<Reader> readerFuture = ReaderConfigClient.getInstance().getReader(marker, readerId, spaceId)
        .flatMap(r -> r == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Reader " + readerId + " with space " + spaceId + " not found")) : Future.succeededFuture(r));
    final Future<Long> inputFuture = deserializeReader(context.getBodyAsString())
        .map(Reader::getVersion);

    CompositeFuture.all(spaceFuture, readerFuture, inputFuture)
        .flatMap(cf -> ReaderConfigClient.getInstance().increaseVersion(marker, spaceId, readerId, inputFuture.result()))
        .flatMap(newVersion -> newVersion == null ? Future.failedFuture("Increasing the version failed") : Future.succeededFuture(newVersion))
        .onSuccess(newVersion -> sendResponse(context, HttpResponseStatus.OK, readerFuture.result().withVersion(newVersion)))
        .onFailure(t -> sendHttpErrorResponse(context, t));
  }

  // TODO auth
  public static Future<Reader> registerReader(Marker marker, String spaceId, String readerId) {
    if (spaceId == null || readerId == null)
      return Future.failedFuture("Invalid spaceId or readerId parameters");

    final Future<Space> spaceFuture = SpaceConfigClient.getInstance().get(marker, spaceId)
        .flatMap(s -> s == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Resource with id " + spaceId + " not found.")) : Future.succeededFuture(s));
    final Future<ChangesetsStatisticsResponse> changesetFuture = ChangesetApi.getChangesetStatistics(marker, Future::succeededFuture, spaceId);

    return CompositeFuture.all(spaceFuture, changesetFuture).flatMap(cf -> {
      final Reader reader = new Reader()
          .withId(readerId)
          .withSpaceId(spaceId)
          .withVersion(changesetFuture.result().getMaxVersion());
      return ReaderConfigClient.getInstance().storeReader(marker, reader).map(v -> reader);
    });
  }

  // TODO auth
  public static Future<Reader> deregisterReader(Marker marker, String spaceId, String readerId) {
    if (spaceId == null || readerId == null)
      return Future.failedFuture("Invalid spaceId or readerId parameters");

    return SpaceConfigClient.getInstance().get(marker, spaceId)
        .flatMap(s -> s == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Resource with id " + spaceId + " not found.")) : Future.succeededFuture())
        .flatMap(none -> ReaderConfigClient.getInstance().deleteReader(marker, readerId, spaceId));
  }

  private Future<Reader> deserializeReader(String body) {
    try {
      return Future.succeededFuture(XyzSerializable.deserialize(body));
    } catch (JsonProcessingException e) {
      return Future.failedFuture("Unable to parse body");
    }
  }

  private void sendHttpErrorResponse(RoutingContext context, Throwable t) {
    if (t == null)
      t = new HttpException(HttpResponseStatus.BAD_REQUEST, "Invalid response");
    if (!(t instanceof HttpException))
      t = new HttpException(HttpResponseStatus.BAD_REQUEST, t.getMessage());
    this.sendErrorResponse(context, t);
  }
}
