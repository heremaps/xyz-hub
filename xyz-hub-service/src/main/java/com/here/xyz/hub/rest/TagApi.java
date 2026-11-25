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

import static com.here.xyz.hub.rest.ApiParam.Path.INCLUDE_SYSTEM_TAGS;
import static com.here.xyz.models.hub.Ref.HEAD;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Ref.InvalidRef;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
    String author = getRequestAuthor(context);
    deserializeTag(context.body().asString())
        .compose(tag -> createTag(
            getMarker(context),
            getSpaceId(context),
            tag.getId(),
            tag.getVersionRef(),
            tag.isSystem(),
            tag.getDescription(),
            author,
            Core.currentTimeMillis()))
        .onSuccess(result -> sendResponse(context, HttpResponseStatus.OK.code(), result))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void deleteTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);

    getSpaceIfActive(getMarker(context), spaceId)
        .compose(s -> deleteTag(getMarker(context), spaceId, tagId))
        .compose(result -> result != null
            ? Future.succeededFuture(result)
            : Future.failedFuture(new HttpException(NOT_FOUND, "Tag not found.")))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void listTags(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final boolean includeSystemTags = Query.getBoolean(context, INCLUDE_SYSTEM_TAGS, false);
    final Marker marker = getMarker(context);

    getSpaceIfActive(marker, spaceId)
        .compose(s -> Service.tagConfigClient.getTags(marker, spaceId, includeSystemTags))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void getTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);
    final Marker marker = getMarker(context);

    getSpaceIfActive(marker, spaceId)
        .compose(s -> Service.tagConfigClient.getTag(marker, tagId, spaceId))
        .compose(r -> r == null ? Future.failedFuture(
            new HttpException(NOT_FOUND, "Tag " + tagId + " not found"))
            : Future.succeededFuture(r))
        .onSuccess(r -> sendResponse(context, HttpResponseStatus.OK.code(), r))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  // TODO auth
  private void updateTag(RoutingContext context) {
    final String spaceId = getSpaceId(context);
    final String tagId = context.pathParam(Path.TAG_ID);
    final Marker marker = getMarker(context);

    String author = getRequestAuthor(context);
    deserializeTag(context.body().asString())
        .compose(tag -> updateTag(marker, spaceId, tagId, tag.getVersionRef(), author, tag.getDescription()))
        .onSuccess(tag -> sendResponse(context, HttpResponseStatus.OK.code(), tag))
        .onFailure(t -> sendErrorResponse(context, t));
  }

  /**
   * Moves a tag to another version. The version is not verified (as on creation), only HEAD is resolved.
   * The author and the creation time of the tag are kept. A tag stored without them gets the author of this request and the
   * current time.
   */
  public static Future<Tag> updateTag(Marker marker, String spaceId, String tagId, Ref versionRef, String author, String description) {
    if (spaceId == null)
      return Future.failedFuture(new ValidationException("Invalid parameter"));

    if (!Tag.isDescriptionValid(description))
      return Future.failedFuture(new ValidationException("Invalid description parameter, description must be less than 255 characters"));

    return getSpaceIfActive(marker, spaceId)
        .compose(space -> Service.tagConfigClient.getTag(marker, tagId, spaceId))
        .compose(loadedTag -> loadedTag == null
            ? Future.failedFuture(new HttpException(NOT_FOUND, "Tag " + tagId + " not found"))
            : Future.succeededFuture(loadedTag))
        .compose(loadedTag -> resolveVersionRef(marker, spaceId, versionRef)
            .compose(resolvedVersionRef -> {
              //The author and the creation time are set on creation only, an update must not reset them
              final Tag updatedTag = new Tag()
                  .withId(tagId)
                  .withSpaceId(spaceId)
                  .withVersionRef(resolvedVersionRef)
                  .withSystem(loadedTag.isSystem())
                  .withDescription(description == null ? loadedTag.getDescription() : description)
                  .withAuthor(keptAuthor(loadedTag, author))
                  .withCreatedAt(keptCreatedAt(loadedTag));
              return Service.tagConfigClient.storeTag(marker, updatedTag).map(v -> updatedTag);
            }));
  }

  public static Future<Tag> createTag(Marker marker, String spaceId, String tagId, String author) {
    return createTag(marker, spaceId, tagId, new Ref(HEAD), true, "", author, Core.currentTimeMillis());
  }

  // TODO auth
  public static Future<Tag> createTag(Marker marker, String spaceId, String tagId, Ref versionRef, boolean system,
                                      String description, String author, long createdAt) {
    if (spaceId == null) {
      return Future.failedFuture(new ValidationException("Invalid parameter"));
    }

    if (!Tag.isValidId(tagId)) {
      return Future.failedFuture(new ValidationException("Invalid tagId parameter"));
    }

    if (!Tag.isDescriptionValid(description)) {
      return Future.failedFuture(
              new ValidationException("Invalid description parameter, description must be less than 255 characters")
      );
    }

    final Future<Space> spaceFuture = getSpaceIfActive(marker, spaceId);
    final Future<Ref> resolveVersionRef = resolveVersionRef(marker, spaceId, versionRef);
    final Future<Void> checkExistingAlias = checkExistingAlias(marker, spaceId, tagId);

    return Future.all(spaceFuture, resolveVersionRef, checkExistingAlias).compose(cf -> {
      final Tag tag = new Tag()
          .withId(tagId)
          .withSpaceId(spaceId)
          .withVersionRef(resolveVersionRef.result())
          .withSystem(system)
          .withDescription(description)
          .withAuthor(author)
          .withCreatedAt(createdAt);
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

  //TODO: Check why does this return a Future ?
  static Future<Tag> deserializeTag(String body) {
    if (StringUtils.isBlank(body)) {
      return Future.failedFuture(new ValidationException("Unable to parse body"));
    }

    try {
      Tag tag = XyzSerializable.deserialize(body, Tag.class);
      if (tag.getVersionRef() == null) {
        //-2 is the default of Tag.version, i.e. no version given, which means HEAD
        if (tag.getVersion() < 0 && tag.getVersion() != -2)
          return Future.failedFuture(negativeTagVersion(String.valueOf(tag.getVersion())));
        tag.setVersionRef(tag.getVersion() == -2 ? new Ref(HEAD) : new Ref(tag.getVersion()));
      }
      return Future.succeededFuture(tag);
    } catch (JsonProcessingException e) {
      //Ref rejects a negative version, Tag.setVersionRef() a ref which is not a single version (a range, "*" or a tag)
      if (ExceptionUtils.getRootCause(e) instanceof InvalidRef) {
        final String versionRef = String.valueOf(readVersionRef(body));
        return Future.failedFuture(NEGATIVE_VERSION.matcher(versionRef).matches()
            ? negativeTagVersion(versionRef)
            : invalidTagVersionRef(versionRef));
      }
      return Future.failedFuture(new ValidationException("Unable to parse body"));
    }
  }

  private static final Pattern NEGATIVE_VERSION = Pattern.compile("-\\d+");

  private static DetailedHttpException negativeTagVersion(String version) {
    return new DetailedHttpException("E318404", Map.of("versionRef", version,
        "cause", "The version of a tag must not be lower than 0."));
  }

  private static Object readVersionRef(String body) {
    try {
      return XyzSerializable.deserialize(body, Map.class).get("versionRef");
    }
    catch (JsonProcessingException | RuntimeException e) {
      return null;
    }
  }

  private static DetailedHttpException invalidTagVersionRef(String versionRef) {
    return new DetailedHttpException("E318404", Map.of("versionRef", versionRef,
        "cause", "A tag must point to a single version or HEAD, not to a range, all versions or another tag."));
  }

  private static Future<Space> getSpaceIfActive(Marker marker, String spaceId) {
    return Service.spaceConfigClient.get(marker, spaceId)
        .compose(s -> s == null
            ? Future.failedFuture(new DetailedHttpException("E318441", Map.of("resourceId", spaceId)))
            : Future.succeededFuture(s))
        .compose(s -> !s.isActive()
            ? Future.failedFuture(new DetailedHttpException("E318451", Map.of("resourceId", spaceId)))
            : Future.succeededFuture(s));
  }

  /**
   * Resolves HEAD into the actual latest version. Other versions are not verified, so a tag can point to a version which does not
   * exist yet: the latest version known to the Hub comes from a read replica and from the head table, which may both be behind
   * the actual version of the space.
   */
  static Future<Ref> resolveVersionRef(Marker marker, String spaceId, Ref versionRef) {
    //A tag points to exactly one version
    if (versionRef.isTag() || !versionRef.isSingleVersion())
      return Future.failedFuture(invalidTagVersionRef(versionRef.toString()));
    if (!versionRef.isHead())
      return Future.succeededFuture(versionRef);
    return FeatureQueryApi.getStatistics(marker, spaceId, SpaceContext.DEFAULT, new Ref(HEAD), true, false)
        .compose(statistics -> statistics.getMaxVersion() == null
            ? Future.failedFuture(new DetailedHttpException("E318404", Map.of("versionRef", HEAD,
                "cause", "The storage of the space does not report its versions.")))
            : Future.succeededFuture(new Ref(statistics.getMaxVersion().getValue())));
  }

  /**
   * The author of a tag is the author of the request. If the request does not provide one (e.g. a call by an application
   * without an "Author" header while the author is expected in that header) the account of the token is used,
   * as a tag without an author would be reported as being created by "system".
   * The anonymous token of the DUMMY authorization (e.g. a Hub embedded behind a gateway that removes the token) identifies
   * nobody, so it is not used.
   */
  private static final String ANONYMOUS_AID = "ANONYMOUS";

  public static String getRequestAuthor(RoutingContext context) {
    return resolveAuthor(BaseHttpServerVerticle.getAuthor(context), BaseHttpServerVerticle.getJWT(context));
  }

  static String resolveAuthor(String requestAuthor, JWTPayload jwt) {
    //The DUMMY authorization's token (auth/dummyJwt.json) has the aid ANONYMOUS without the anonymous flag
    if (StringUtils.isNotBlank(requestAuthor) || jwt == null || jwt.anonymous || ANONYMOUS_AID.equals(jwt.aid))
      return requestAuthor;
    return StringUtils.firstNonBlank(jwt.aid, jwt.cid, requestAuthor);
  }

  //DynamoTagConfigClient reports a tag stored without author as "system" and without creation time as -1
  private static final String MISSING_AUTHOR = "system";

  static String keptAuthor(Tag loadedTag, String requestAuthor) {
    final String author = loadedTag.getAuthor();
    return (StringUtils.isBlank(author) || MISSING_AUTHOR.equals(author)) && StringUtils.isNotBlank(requestAuthor) ? requestAuthor : author;
  }

  static long keptCreatedAt(Tag loadedTag) {
    return loadedTag.getCreatedAt() <= 0 ? Core.currentTimeMillis() : loadedTag.getCreatedAt();
  }
}
