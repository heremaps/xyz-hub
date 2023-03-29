/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.XyzHubTask;
import com.here.xyz.hub.task.feature.ConditionalModifyFeaturesTask;
import com.here.xyz.hub.task.feature.DeleteFeaturesByTagTask;
import com.here.xyz.hub.task.feature.GetFeaturesByIdTask;
import com.here.xyz.hub.task.ModifyFeatureOp;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.util.diff.Patcher.ConflictResolution;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.EventTask;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FeatureApi extends SpaceBasedApi {

  public FeatureApi(RouterBuilder rb) {
    rb.operation("getFeature").handler(this::getFeature);
    rb.operation("getFeatures").handler(this::getFeatures);
    rb.operation("putFeature").handler(this::putFeature);
    rb.operation("putFeatures").handler(this::putFeatures);
    rb.operation("postFeatures").handler(this::postFeatures);
    rb.operation("patchFeature").handler(this::patchFeature);
    rb.operation("deleteFeature").handler(this::deleteFeature);
    rb.operation("deleteFeatures").handler(this::deleteFeatures);
  }

  /**
   * Returns either the {@link ApiResponseType#EMPTY} response type, if requested by the client, or the given default response type.
   *
   * @param context the context from which to read the {@link HttpHeaders#ACCEPT Accept} header to detect if the client does not want any
   * response (204).
   * @param defaultResponseType the default response type to return.
   * @return either the {@link ApiResponseType#EMPTY} response type, if requested by the client or the given default response type, if the
   * client did not explicitly request an empty response.
   */
  private ApiResponseType getEmptyResponseTypeOr(final RoutingContext context, ApiResponseType defaultResponseType) {
    if ("application/x-empty".equalsIgnoreCase(context.request().headers().get(HttpHeaders.ACCEPT))) {
      return ApiResponseType.EMPTY;
    }
    return defaultResponseType;
  }

  /**
   * Retrieves a feature.
   */
  private void getFeature(final RoutingContext context) {
    XyzHubTask.startTask(GetFeaturesByIdTask.class, context, ApiResponseType.FEATURE);
  }

  /**
   * Retrieves multiple features by ID.
   */
  private void getFeatures(final RoutingContext context) {
    XyzHubTask.startTask(GetFeaturesByIdTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }

  /**
   * Creates or replaces a feature.
   */
  private void putFeature(final RoutingContext context) {
    executeConditionalOperationChain(false, context, ApiResponseType.FEATURE, IfExists.REPLACE, IfNotExists.CREATE, true, ConflictResolution.ERROR);
  }

  /**
   * Creates or replaces multiple features.
   *
   * @param context the routing context
   */
  private void putFeatures(final RoutingContext context) {
    executeConditionalOperationChain(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), IfExists.REPLACE,
        IfNotExists.CREATE, true, ConflictResolution.ERROR);
  }

  /**
   * Patches a feature
   */
  private void patchFeature(final RoutingContext context) {
    executeConditionalOperationChain(true, context, ApiResponseType.FEATURE, IfExists.PATCH, IfNotExists.RETAIN, true, ConflictResolution.ERROR);
  }

  /**
   * Creates or patches multiple features.
   */
  private void postFeatures(final RoutingContext context) {
    final IfNotExists ifNotExists = IfNotExists.of(Query.getString(context, Query.IF_NOT_EXISTS, "create"));
    final IfExists ifExists = IfExists.of(Query.getString(context, Query.IF_EXISTS, "patch"));
    final ConflictResolution conflictResolution = ConflictResolution.of(Query.getString(context, Query.CONFLICT_RESOLUTION, "error"));
    boolean transactional = Query.getBoolean(context, Query.TRANSACTIONAL, true);

    executeConditionalOperationChain(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), ifExists, ifNotExists, transactional, conflictResolution);
  }

  /**
   * Deletes a feature by ID.
   */
  private void deleteFeature(final RoutingContext context) {
    Map<String, Object> featureModification = Collections.singletonMap("featureIds",
        Collections.singletonList(context.pathParam(Path.FEATURE_ID)));
    final SpaceContext spaceContext = getSpaceContext(context);
    executeConditionalOperationChain(true, context, ApiResponseType.EMPTY, IfExists.DELETE, IfNotExists.RETAIN, true, ConflictResolution.ERROR,
        Collections.singletonList(featureModification), spaceContext);
  }

  /**
   * Delete features by IDs or by tags.
   */
  private void deleteFeatures(final RoutingContext context) {
    final Set<String> featureIds = new HashSet<>(Query.queryParam(Query.FEATURE_ID, context));
    final TagsQuery tags = Query.getTags(context);
    final String accept = context.request().getHeader(ACCEPT);
    final ApiResponseType responseType = APPLICATION_GEO_JSON.equals(accept) || APPLICATION_JSON.equals(accept)
        ? ApiResponseType.FEATURE_COLLECTION : ApiResponseType.EMPTY;
    final SpaceContext spaceContext = getSpaceContext(context);

    //Delete features by IDs
    if (featureIds != null && !featureIds.isEmpty()) {
      Map<String, Object> featureModification = Collections.singletonMap("featureIds", new ArrayList<>(featureIds));

      executeConditionalOperationChain(false, context, responseType, IfExists.DELETE, IfNotExists.RETAIN, true,
          ConflictResolution.ERROR, Collections.singletonList(featureModification), spaceContext);
    }

    //Delete features by tags
    else if (!tags.isEmpty()) {
      if (checkModificationOnSuper(context, spaceContext))
        return;
      DeleteFeaturesByTagEvent event = new DeleteFeaturesByTagEvent();
      if (!tags.containsWildcard()) {
        event.setTags(tags);
      }
      new DeleteFeaturesByTagTask(event, context, responseType)
          .execute(this::sendResponse, this::sendErrorResponse);
    } else {
      context.fail(
          new HttpException(HttpResponseStatus.BAD_REQUEST, "At least one identifier should be provided as a query parameter."));
    }
  }

  /**
   * Creates and executes a ModifyFeatureOp
   */
  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr) {
    if (checkModificationOnSuper(context, getSpaceContext(context)))
      return;
    executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional, cr, null, DEFAULT);
  }

  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr,
      List<Map<String, Object>> featureModifications, SpaceContext spaceContext) {
    if (checkModificationOnSuper(context, spaceContext))
      return;

    ModifyFeaturesEvent event = new ModifyFeaturesEvent().withTransaction(transactional).withContext(spaceContext);
    int bodySize = context.getBody() != null ? context.getBody().length() : 0;
    ConditionalModifyFeaturesTask task = buildConditionalOperation(event, context, apiResponseTypeType, featureModifications, ifNotExists, ifExists, transactional, cr, requireResourceExists, bodySize);
    final List<String> addTags = Query.queryParam(Query.ADD_TAGS, context);
    final List<String> removeTags = Query.queryParam(Query.REMOVE_TAGS, context);
    task.addTags = XyzNamespace.normalizeTags(addTags);
    task.removeTags = XyzNamespace.normalizeTags(removeTags);
    XyzNamespace.fixNormalizedTags(task.addTags);
    XyzNamespace.fixNormalizedTags(task.removeTags);
    task.prefixId = Query.getString(context, Query.PREFIX_ID, null);
    task.execute(this::sendResponse, this::sendErrorResponse);
  }

  private static boolean checkModificationOnSuper(RoutingContext context, SpaceContext spaceContext) {
    if (spaceContext != null && spaceContext.equals(SUPER)) {
      context.fail(
          new HttpException(HttpResponseStatus.FORBIDDEN, "It's not permitted to perform modifications through context " + SUPER + "."));
      return true;
    }
    return false;
  }

  private ConditionalModifyFeaturesTask buildConditionalOperation(
      ModifyFeaturesEvent event,
      RoutingContext context,
      ApiResponseType apiResponseTypeType,
      List<Map<String, Object>> featureModifications,
      IfNotExists ifNotExists,
      IfExists ifExists,
      boolean transactional,
      ConflictResolution cr,
      boolean requireResourceExists,
      int bodySize) {
    if (featureModifications == null)
      return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, ifNotExists, ifExists, transactional, cr, requireResourceExists, bodySize);

    final ModifyFeatureOp modifyFeatureOp = new ModifyFeatureOp(featureModifications, ifNotExists, ifExists, transactional, cr);
    return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, modifyFeatureOp, requireResourceExists, bodySize);
  }
}
