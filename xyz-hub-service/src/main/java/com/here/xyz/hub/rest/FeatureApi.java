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
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST;
import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTask.ConditionalOperation;
import com.here.xyz.hub.task.FeatureTask.DeleteOperation;
import com.here.xyz.hub.task.FeatureTask.IdsQuery;
import com.here.xyz.hub.task.ModifyFeatureOp;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.util.diff.Patcher.ConflictResolution;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class FeatureApi extends Api {

  private static final Logger logger = LogManager.getLogger();

  public FeatureApi(OpenAPI3RouterFactory routerFactory) {
    routerFactory.addHandlerByOperationId("getFeature", this::getFeature);
    routerFactory.addHandlerByOperationId("getFeatures", this::getFeatures);
    routerFactory.addHandlerByOperationId("putFeature", this::putFeature);
    routerFactory.addHandlerByOperationId("putFeatures", this::putFeatures);
    routerFactory.addHandlerByOperationId("postFeatures", this::postFeatures);
    routerFactory.addHandlerByOperationId("patchFeature", this::patchFeature);
    routerFactory.addHandlerByOperationId("deleteFeature", this::deleteFeature);
    routerFactory.addHandlerByOperationId("deleteFeatures", this::deleteFeatures);
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
    final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
    final boolean force2D = Query.getBoolean(context, FORCE_2D, false);

    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
        .withIds(Collections.singletonList(context.pathParam(Path.FEATURE_ID)))
        .withSelection(Query.getSelection(context))
        .withForce2D(force2D);

    new IdsQuery(event, context, ApiResponseType.FEATURE, skipCache)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Retrieves multiple features by ID.
   */
  private void getFeatures(final RoutingContext context) {
    final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
    final boolean force2D = Query.getBoolean(context, FORCE_2D, false);

    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
        .withIds(Query.queryParam(Query.FEATURE_ID, context))
        .withSelection(Query.getSelection(context))
        .withForce2D(force2D);

    new IdsQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Creates or replaces a feature.
   */
  private void putFeature(final RoutingContext context) {
    executeFeatureModification(false, context, ApiResponseType.FEATURE, IfExists.REPLACE, IfNotExists.CREATE, true, ConflictResolution.ERROR);
  }

  /**
   * Creates or replaces multiple features.
   *
   * @param context the routing context
   */
  private void putFeatures(final RoutingContext context) {
    executeFeatureModification(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), IfExists.REPLACE,
        IfNotExists.CREATE, true, ConflictResolution.ERROR);
  }

  /**
   * Patches a feature
   */
  private void patchFeature(final RoutingContext context) {
    executeFeatureModification(true, context, ApiResponseType.FEATURE, IfExists.PATCH, IfNotExists.RETAIN, true, ConflictResolution.ERROR);
  }

  /**
   * Creates or patches multiple features.
   */
  private void postFeatures(final RoutingContext context) {
    final IfNotExists ifNotExists = IfNotExists.of(Query.getString(context, Query.IF_NOT_EXISTS, "create"));
    final IfExists ifExists = IfExists.of(Query.getString(context, Query.IF_EXISTS, "patch"));
    final ConflictResolution conflictResolution = ConflictResolution.of(Query.getString(context, Query.CONFLICT_RESOLUTION, "error"));
    boolean transactional = Query.getBoolean(context, Query.TRANSACTIONAL, true);

    if (APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST.equals(context.parsedHeaders().contentType().rawValue())) {
      executeHolisticFeatureModification(false, context,
          getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), ifExists, ifNotExists, transactional, conflictResolution);
    }
    else {
      executeFeatureModification(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION),
          ifExists, ifNotExists, transactional, conflictResolution);
    }
  }

  /**
   * Deletes a feature by ID.
   */
  private void deleteFeature(final RoutingContext context) {
    Map<String, Object> featureModification = Collections.singletonMap("featureIds",
        Collections.singletonList(context.pathParam(Path.FEATURE_ID)));
    executeConditionalOperationChain(true, context, ApiResponseType.EMPTY, IfExists.DELETE, IfNotExists.RETAIN, true, ConflictResolution.ERROR,
        Collections.singletonList(featureModification));
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

    //Delete features by IDs
    if (featureIds != null && !featureIds.isEmpty()) {
      Map<String, Object> featureModification = Collections.singletonMap("featureIds", new ArrayList<>(featureIds));

      executeConditionalOperationChain(false, context, responseType, IfExists.DELETE, IfNotExists.RETAIN, true,
          ConflictResolution.ERROR, Collections.singletonList(featureModification));
    }

    //Delete features by tags
    else if (!tags.isEmpty()) {
      DeleteFeaturesByTagEvent event = new DeleteFeaturesByTagEvent();
      if (!tags.containsWildcard()) {
        event.setTags(tags);
      }
      new DeleteOperation(event, context, responseType)
          .execute(this::sendResponse, this::sendErrorResponse);
    } else {
      context.fail(
          new HttpException(HttpResponseStatus.BAD_GATEWAY, "At least one tag or identifier should be provided as a query parameter."));
    }
  }

  /**
   * Creates and executes a ModifyFeatureOp
   */
  private void executeFeatureModification(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr) {
    try {
      List<Map<String, Object>> features = getObjectsAsList(context);
      if (apiResponseTypeType == ApiResponseType.FEATURE) { //TODO: Replace that evil hack
        features.get(0).put("id", context.pathParam(ApiParam.Path.FEATURE_ID));
      }
      Map<String, Object> featureCollection = Collections.singletonMap("features", features);

      executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional, cr,
          Collections.singletonList(Collections.singletonMap("featureData", featureCollection)));
    }
    catch (HttpException e) {
      sendErrorResponse(context, e);
    }
    catch (Exception e) {
      context.fail(e);
    }
  }

  /**
   * Executes a feature modification of which the operation may be any combination be of the types create, update or delete.
   * The overall modification may be transactional or not.
   * Existence-handlers and/or conflict-resolution can be provided per feature-collection.
   */
  private void executeHolisticFeatureModification(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr) {
    try {
      executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional, cr,
          getObjectsAsList(context));
    }
    catch (HttpException e) {
      sendErrorResponse(context, e);
    }
    catch (Exception e) {
      context.fail(e);
    }
  }

  /**
   * Creates and executes a ModifyFeatureOp
   */
  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr,
      List<Map<String, Object>> featureModifications) {
    ModifyFeaturesEvent event = new ModifyFeaturesEvent().withTransaction(transactional);
    ConditionalOperation task = new ConditionalOperation(event, context, apiResponseTypeType,
        new ModifyFeatureOp(featureModifications, ifNotExists, ifExists, transactional, cr), requireResourceExists);
    final List<String> addTags = Query.queryParam(Query.ADD_TAGS, context);
    final List<String> removeTags = Query.queryParam(Query.REMOVE_TAGS, context);
    task.addTags = XyzNamespace.normalizeTags(addTags);
    task.removeTags = XyzNamespace.normalizeTags(removeTags);
    XyzNamespace.fixNormalizedTags(task.addTags);
    XyzNamespace.fixNormalizedTags(task.removeTags);
    task.prefixId = Query.getString(context, Query.PREFIX_ID, null);
    task.execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Parses the body of the request as a FeatureCollection, Feature or a FeatureModificationList object and returns the features as a list.
   */
  private List<Map<String, Object>> getObjectsAsList(final RoutingContext context) throws HttpException {
    final Marker logMarker = Context.getMarker(context);
    try {
      JsonObject json = context.getBodyAsJson();
      return getJsonObjects(json, context);
    }
    catch (DecodeException e) {
      logger.warn(logMarker, "Invalid input encoding.", e);
      try {
        //Some types of exceptions could be avoided by reading the entire string.
        JsonObject json = new JsonObject(context.getBodyAsString());
        return getJsonObjects(json, context);
      }
      catch (DecodeException ex) {
        logger.info(logMarker, "Error in the provided content", ex.getCause());
        throw new HttpException(BAD_REQUEST, "Invalid JSON input string: " + ex.getMessage());
      }
    }
    catch (Exception e) {
      logger.info(logMarker, "Error in the provided content", e);
      throw new HttpException(BAD_REQUEST, "Cannot read input JSON string.");
    }
  }

  private List<Map<String, Object>> getJsonObjects(JsonObject json, RoutingContext context) throws HttpException {
    try {
      if (json == null) {
        throw new HttpException(BAD_REQUEST, "Missing content");
      }
      if ("FeatureCollection".equals(json.getString("type"))) {
        //noinspection unchecked
        return json.getJsonArray("features", new JsonArray()).getList();
      }
      if ("FeatureModificationList".equals(json.getString("type"))) {
        //noinspection unchecked
        return json.getJsonArray("modifications", new JsonArray()).getList();
      }
      if ("Feature".equals(json.getString("type"))) {
        return Collections.singletonList(json.getMap());
      }
      else {
        throw new HttpException(BAD_REQUEST, "The provided content does not have a type of FeatureCollection,"
            + " Feature or FeatureModificationList.");
      }
    }
    catch (Exception e) {
      logger.info(Context.getMarker(context), "Error in the provided content", e);
      throw new HttpException(BAD_REQUEST, "Cannot read input JSON string.");
    }
  }
}
