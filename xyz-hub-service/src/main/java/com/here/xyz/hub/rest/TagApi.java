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

import static com.here.xyz.hub.rest.ApiParam.Path.INCLUDE_SYSTEM_TAGS;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Marker;

public class TagApi extends SpaceBasedApi {

  public TagApi(RouterBuilder rb) {
    rb.getRoute("createTag").setDoValidation(false).addHandler(handleErrors(this::createTag));
    rb.getRoute("updateTag").setDoValidation(false).addHandler(handleErrors(this::updateTag));
    rb.getRoute("getTag").setDoValidation(false).addHandler(handleErrors(this::getTag));
    rb.getRoute("deleteTag").setDoValidation(false).addHandler(handleErrors(this::deleteTag));
    rb.getRoute("listTags").setDoValidation(false).addHandler(this::listTags);
  }

  // TODO auth
  private void createTag(RoutingContext context) {
    deserializeTag(context.body().asString())
        .compose(tag -> createTag(
            getMarker(context),
            getSpaceId(context),
            tag.getId(),
            tag.getVersion(),
            tag.isSystem()))
        .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK.code(), result))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void deleteTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);

    getSpace(getMarker(context), spaceId)
        .compose(s -> deleteTag(getMarker(context), spaceId, tagId))
        .compose(result -> result != null
            ? Future.succeededFuture(result)
            : Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND, "Tag not found.")))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void listTags(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final boolean includeSystemTags = Query.getBoolean(context, INCLUDE_SYSTEM_TAGS, false);
    final Marker marker = getMarker(context);

    getSpace(marker, spaceId)
        .compose(s -> Service.tagConfigClient.getTags(marker, spaceId, includeSystemTags))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void getTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);
    final Marker marker = getMarker(context);

    getSpace(marker, spaceId)
        .compose(s -> Service.tagConfigClient.getTag(marker, tagId, spaceId))
        .compose(r -> r == null ? Future.failedFuture(
            new HttpException(HttpResponseStatus.NOT_FOUND, "Tag " + tagId + " not found"))
            : Future.succeededFuture(r))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void updateTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);
    final Marker marker = getMarker(context);

    deserializeTag(context.body().asString())
        .compose(tag -> updateTag(marker, spaceId, tagId, tag.getVersion()))
        .onSuccess(tag -> sendResponse(context, HttpResponseStatus.OK.code(), tag))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  public static Future<Tag> updateTag(Marker marker, String spaceId, String tagId, long version) {
    if (spaceId == null) {
      return Future.failedFuture(new ValidationException("Invalid parameter"));
    }

    //FIXME: Neither -2 nor -1 are valid versions
    if (version < -2) {
      return Future.failedFuture(new ValidationException("Invalid version parameter"));
    }

    final Future<Space> spaceFuture = getSpace(marker, spaceId);
    final Future<Tag> tagFuture = Service.tagConfigClient.getTag(marker, tagId, spaceId)
        .compose(r -> r == null ? Future.failedFuture(
            new HttpException(HttpResponseStatus.NOT_FOUND, "Tag " + tagId + " not found"))
            : Future.succeededFuture(r));

    return Future.all(spaceFuture, tagFuture)
        .compose(cf -> Service.tagConfigClient.storeTag(marker, new Tag()
            .withId(tagId)
            .withSpaceId(spaceId)
            .withVersion(version)
            .withSystem(tagFuture.result().isSystem())))
        .map(v -> tagFuture.result().withVersion(version));
  }

  public static Future<Tag> createTag(Marker marker, String spaceId, String tagId) {
    return createTag(marker, spaceId, tagId, -2, true);
  }

  // TODO auth
  public static Future<Tag> createTag(Marker marker, String spaceId, String tagId, long version, boolean system) {
    if (spaceId == null) {
      return Future.failedFuture(new ValidationException("Invalid parameter"));
    }

    if (!Tag.isValidId(tagId)) {
      return Future.failedFuture(new ValidationException("Invalid tagId parameter"));
    }

    if (version < -2) {
      return Future.failedFuture(new ValidationException("Invalid version parameter"));
    }

    final Future<Space> spaceFuture = getSpace(marker, spaceId);
    final Future<ChangesetsStatisticsResponse> changesetFuture = ChangesetApi.getChangesetStatistics(marker, Future::succeededFuture, spaceId);
    final Future<Tag> tagFuture = Service.tagConfigClient.getTag(marker, tagId, spaceId)
        .compose(r -> r == null ? Future.succeededFuture(null) : Future.failedFuture(
            new HttpException(HttpResponseStatus.CONFLICT, "Tag " + tagId + " with space " + spaceId + " already exists")));

    return Future.all(spaceFuture, changesetFuture, tagFuture).compose(cf -> {
      final Tag tag = new Tag()
          .withId(tagId)
          .withSpaceId(spaceId)
          .withVersion(version == -2 ? changesetFuture.result().getMaxVersion() : version)
          .withSystem(system);
      return Service.tagConfigClient.storeTag(marker, tag).map(v -> tag);
    });
  }

  // TODO auth
  public static Future<Tag> deleteTag(Marker marker, String spaceId, String tagId) {
    if (spaceId == null || tagId == null) {
      return Future.failedFuture(new ValidationException("Invalid parameters"));
    }

    return Service.spaceConfigClient.get(marker, spaceId)
        .compose(none -> Service.tagConfigClient.deleteTag(marker, tagId, spaceId));
  }

  private static Future<Tag> deserializeTag(String body) {
    if (StringUtils.isBlank(body)) {
      return Future.failedFuture(new ValidationException("Unable to parse body"));
    }

    try {
      return Future.succeededFuture(XyzSerializable.deserialize(body, Tag.class));
    } catch (JsonProcessingException e) {
      return Future.failedFuture(new ValidationException("Unable to parse body"));
    }
  }

  private static Future<Space> getSpace(Marker marker, String spaceId) {
    return Service.spaceConfigClient.get(marker, spaceId)
        .compose(s -> s == null
            ? Future.failedFuture(new DetailedHttpException("E318441", Map.of("resourceId", spaceId)))
            : Future.succeededFuture(s))
        .compose(s -> !s.isActive()
            ? Future.failedFuture(new DetailedHttpException("E318451", Map.of("resourceId", spaceId)))
            : Future.succeededFuture(s));
  }
}
