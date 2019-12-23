/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
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
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Marker;

public class FeatureApi extends Api {

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
    GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
        .withIds(Collections.singletonList(context.pathParam(Path.FEATURE_ID)))
        .withSelection(Query.getSelection(context));

    new IdsQuery(event, context, ApiResponseType.FEATURE, skipCache)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Retrieves multiple features by ID.
   */
  private void getFeatures(final RoutingContext context) {
    final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
    GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
        .withIds(Query.queryParam(Query.FEATURE_ID, context))
        .withSelection(Query.getSelection(context));

    new IdsQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Creates or replaces a feature.
   */
  private void putFeature(final RoutingContext context) {
    executeConditionalOperationChain(false, context, ApiResponseType.FEATURE, IfExists.REPLACE, IfNotExists.CREATE, true);
  }

  /**
   * Creates or replaces multiple features.
   *
   * @param context the routing context
   */
  private void putFeatures(final RoutingContext context) {
    executeConditionalOperationChain(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), IfExists.REPLACE,
        IfNotExists.CREATE, true);
  }

  /**
   * Patches a feature
   */
  private void patchFeature(final RoutingContext context) {
    executeConditionalOperationChain(true, context, ApiResponseType.FEATURE, IfExists.PATCH, IfNotExists.RETAIN, true);
  }

  /**
   * Creates or patches multiple features.
   */
  private void postFeatures(final RoutingContext context) {
    final IfNotExists ifNotExists = IfNotExists.of(Query.getString(context, Query.IF_NOT_EXISTS, "create"));
    final IfExists ifExists = IfExists.of(Query.getString(context, Query.IF_EXISTS, "patch"));
    boolean transactional = Query.getBoolean(context, Query.TRANSACTIONAL, true);
    executeConditionalOperationChain(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), ifExists,
        ifNotExists, transactional);
  }

  /**
   * Deletes a feature by ID.
   */
  private void deleteFeature(final RoutingContext context) {
    executeConditionalOperationChain(true, context, ApiResponseType.EMPTY, IfExists.DELETE, IfNotExists.RETAIN, true,
        Collections.singletonList(Feature.createEmptyFeature().withId(context.pathParam(Path.FEATURE_ID))));
  }

  /**
   * Delete features by IDs or by tags.
   */
  private void deleteFeatures(final RoutingContext context) {
    final List<String> featureIds = Query.queryParam(Query.FEATURE_ID, context);
    final TagsQuery tags = Query.getTags(context);
    final String accept = context.request().getHeader(ACCEPT);
    final ApiResponseType responseType = APPLICATION_GEO_JSON.equals(accept) || APPLICATION_JSON.equals(accept)
        ? ApiResponseType.FEATURE_COLLECTION : ApiResponseType.EMPTY;

    //Delete features by IDs
    if (featureIds != null && !featureIds.isEmpty()) {
      final List<Feature> features = featureIds.stream().distinct()
          .map(id -> Feature.createEmptyFeature().withId(id))
          .collect(Collectors.toList());

      executeConditionalOperationChain(false, context, responseType, IfExists.DELETE, IfNotExists.RETAIN, true, features);
    }

    //Delete features by tags
    else if (tags != null && !tags.isEmpty()) {
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
   * Creates and executes a ModifyMapOp
   */
  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType,
      IfExists ifExists, IfNotExists ifNotExists, boolean transactional) {
    try {
      FeatureCollection fc = getBodyAsFeatureCollection(context);
      if (apiResponseTypeType == ApiResponseType.FEATURE) {
        fc.getFeatures().get(0).setId(context.pathParam(ApiParam.Path.FEATURE_ID));
      }

      executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional,
          fc.getFeatures());
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    } catch (Exception e) {
      context.fail(e);
    }
  }

  /**
   * Creates and executes a ModifyMapOp
   */
  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, List<Feature> features) {
    ModifyFeaturesEvent event = new ModifyFeaturesEvent();
    ConditionalOperation task = new ConditionalOperation(event, context, apiResponseTypeType,
        new ModifyFeatureOp(features, ifNotExists, ifExists, transactional), requireResourceExists);
    final List<String> addTags = Query.queryParam(Query.ADD_TAGS, context);
    final List<String> removeTags = Query.queryParam(Query.REMOVE_TAGS, context);
    task.addTags = XyzNamespace.normalizeTags(addTags);
    task.removeTags = XyzNamespace.normalizeTags(removeTags);
    XyzNamespace.fixNormalizedTags(task.addTags);
    XyzNamespace.fixNormalizedTags(task.removeTags);
    task.prefixId = Query.getString(context, Query.PREFIX_ID, null);
    task.getEvent().setTransaction(transactional);
    task.execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Parses the body of the request as a FeatureCollection or a Feature object and returns the features as a list.
   */
  private FeatureCollection getBodyAsFeatureCollection(final RoutingContext context) throws HttpException {
    final Marker logMarker = Context.getMarker(context);
    try {
      final String text = context.getBodyAsString();
      if (text == null) {
        throw new HttpException(BAD_REQUEST, "Missing content");
      }

      final Typed input = XyzSerializable.deserialize(text);
      FeatureCollection featureCollection;
      if (input instanceof FeatureCollection) {
        featureCollection = (FeatureCollection) input;
        if (featureCollection.getFeatures() == null) {
          featureCollection.setFeatures(new ArrayList<>());
        }
      } else if (input instanceof Feature) {
        featureCollection = new FeatureCollection().withFeatures(Collections.singletonList((Feature) input));
      } else {
        throw new HttpException(BAD_REQUEST,
            "The provided content is of type '" + input.getClass().getSimpleName() + "'. Expected is a FeatureCollection or a Feature.");
      }
      Api.Context.getAccessLog(context).reqInfo.numberOfObjects = featureCollection.getFeatures().size();
      return featureCollection;
    } catch (JsonMappingException e) {
      logger().info(logMarker, "Error in the provided content ", e);
      throw new HttpException(BAD_REQUEST, "Invalid JSON type. Expected is a FeatureCollection or a Feature.");
    } catch (JsonParseException e) {
      logger().info(logMarker, "Error in the provided content ", e);
      throw new HttpException(BAD_REQUEST,
          "Invalid JSON string. Error at line " + e.getLocation().getLineNr() + ", column " + e.getLocation().getColumnNr() + ".");
    } catch (IOException e) {
      logger().info(logMarker, "Error in the provided content ", e);
      throw new HttpException(BAD_REQUEST, "Cannot read input JSON string.");
    }
  }
}
