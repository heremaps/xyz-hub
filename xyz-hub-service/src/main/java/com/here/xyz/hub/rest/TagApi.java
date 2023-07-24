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
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Marker;

public class TagApi extends SpaceBasedApi {

    public TagApi(RouterBuilder rb) {
        rb.getRoute("createTag").setDoValidation(false).addHandler(this::createTag);
        rb.getRoute("updateTag").setDoValidation(false).addHandler(this::updateTag);
        rb.getRoute("getTag").setDoValidation(false).addHandler(this::getTag);
        rb.getRoute("deleteTag").setDoValidation(false).addHandler(this::deleteTag);
    }

    // TODO auth
    private void createTag(RoutingContext context) {
        final String spaceId = context.pathParam(Path.SPACE_ID);

        deserializeTag(context.body().asString())
                .flatMap(tag -> createTag(Api.Context.getMarker(context), spaceId, tag.getId(), tag.getVersion()))
                .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK, result))
                .onFailure(t -> sendHttpErrorResponse(context, t));
    }

    // TODO auth
    private void deleteTag(RoutingContext context) {
        final String spaceId = context.pathParam(Path.SPACE_ID);
        final String tagId = context.pathParam(Path.TAG_ID);

        deleteTag(Api.Context.getMarker(context), spaceId, tagId)
                .flatMap(result -> result != null
                        ? Future.succeededFuture(result)
                        : Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Tag not found")))
                .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK, r))
                .onFailure(t -> sendHttpErrorResponse(context, t));
    }

    // TODO auth
    private void getTag(RoutingContext context) {
        final String spaceId = context.pathParam(Path.SPACE_ID);
        final String tagId = context.pathParam(Path.TAG_ID);
        final Marker marker = Api.Context.getMarker(context);

        TagConfigClient.getInstance().getTag(marker, tagId, spaceId)
                .flatMap(r -> r == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Reader " + tagId + " with space " + spaceId + " not found")) : Future.succeededFuture(r))
                .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK, r))
                .onFailure(t -> sendHttpErrorResponse(context, t));

    }

    // TODO auth
    private void updateTag(RoutingContext context) {
        final String spaceId = context.pathParam(Path.SPACE_ID);
        final String tagId = context.pathParam(Path.TAG_ID);
        final Marker marker = Api.Context.getMarker(context);

        final Future<Space> spaceFuture = SpaceConfigClient.getInstance().get(marker, spaceId)
                .flatMap(s -> s == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Resource with id " + spaceId + " not found.")) : Future.succeededFuture(s));
        final Future<Tag> tagFuture = TagConfigClient.getInstance().getTag(marker, tagId, spaceId)
                .flatMap(r -> r == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Reader " + tagId + " with space " + spaceId + " not found")) : Future.succeededFuture(r));
        final Future<Long> inputFuture = deserializeTag(context.body().asString())
                .map(Tag::getVersion);

        final Long version = inputFuture.result();
        CompositeFuture.all(spaceFuture, tagFuture, inputFuture)
                .flatMap(cf -> TagConfigClient.getInstance().storeTag(marker, new Tag().withId(tagId).withSpaceId(spaceId).withVersion(version)))
                .onSuccess(v -> sendResponse(context, HttpResponseStatus.OK, tagFuture.result().withVersion(version)))
                .onFailure(t -> sendHttpErrorResponse(context, t));
    }

    public static Future<Tag> createTag(Marker marker, String spaceId, String tagId) {
        return createTag(marker, spaceId, tagId, 0);
    }

    // TODO auth
    private static Future<Tag> createTag(Marker marker, String spaceId, String tagId, long version) {
        if (spaceId == null)
            return Future.failedFuture("Invalid spaceId parameter");
        if (tagId == null )
            return Future.failedFuture("Invalid tagId parameter");

        final Future<Space> spaceFuture = SpaceConfigClient.getInstance().get(marker, spaceId)
                .flatMap(s -> s == null ? Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Resource with id " + spaceId + " not found.")) : Future.succeededFuture(s));
        final Future<ChangesetsStatisticsResponse> changesetFuture = ChangesetApi.getChangesetStatistics(marker, Future::succeededFuture, spaceId);

        return CompositeFuture.all(spaceFuture, changesetFuture).flatMap(cf -> {
            final Tag tag = new Tag()
                    .withId(tagId)
                    .withSpaceId(spaceId)
                    .withVersion(version == 0 ? changesetFuture.result().getMaxVersion() : version);
            return TagConfigClient.getInstance().storeTag(marker, tag).map(v -> tag);
        });
    }

    // TODO auth
    public static Future<Tag> deleteTag(Marker marker, String spaceId, String tagId) {
        if (spaceId == null || tagId == null)
            return Future.failedFuture("Invalid spaceId or tagId parameters");

        return SpaceConfigClient.getInstance().get(marker, spaceId)
                .flatMap(none -> TagConfigClient.getInstance().deleteTag(marker, tagId, spaceId));
    }

    private static Future<Tag> deserializeTag(String body) {
      if (StringUtils.isBlank(body)) {
        return Future.failedFuture("Unable to parse body");
      }

      try {
          return Future.succeededFuture(XyzSerializable.deserialize(body, Tag.class));
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
