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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static com.here.xyz.hub.task.FeatureHandler.checkReadOnly;
import static com.here.xyz.hub.task.FeatureHandler.resolveExtendedSpaces;
import static com.here.xyz.hub.task.FeatureHandler.resolveListenersAndProcessors;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.hub.XYZHubRESTVerticle;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureHandler;
import com.here.xyz.hub.task.FeatureTask.ConditionalOperation;
import com.here.xyz.hub.task.FeatureTask.IdsQuery;
import com.here.xyz.hub.task.ModifyFeatureOp;
import com.here.xyz.hub.task.ModifyFeatureOp.FeatureEntry;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.FeatureModificationList;
import com.here.xyz.models.hub.FeatureModificationList.ConflictResolution;
import com.here.xyz.models.hub.FeatureModificationList.IfExists;
import com.here.xyz.models.hub.FeatureModificationList.IfNotExists;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.TooManyRequestsException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FeatureApi extends SpaceBasedApi {
  private static final boolean USE_WRITE_FEATURES_EVENT = false;

  public FeatureApi(RouterBuilder rb) {
    rb.getRoute("getFeature").setDoValidation(false).addHandler(this::getFeature);
    rb.getRoute("getFeatures").setDoValidation(false).addHandler(this::getFeatures);
    rb.getRoute("putFeature").setDoValidation(false).addHandler(this::putFeature);
    rb.getRoute("putFeatures").setDoValidation(false).addHandler(this::putFeatures);
    rb.getRoute("postFeatures").setDoValidation(false).addHandler(this::postFeatures);
    rb.getRoute("patchFeature").setDoValidation(false).addHandler(this::patchFeature);
    rb.getRoute("deleteFeature").setDoValidation(false).addHandler(this::deleteFeature);
    rb.getRoute("deleteFeatures").setDoValidation(false).addHandler(this::deleteFeatures);
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
    if ("application/x-empty".equalsIgnoreCase(context.request().headers().get(ACCEPT))) {
      return ApiResponseType.EMPTY;
    }
    return defaultResponseType;
  }

  /**
   * Retrieves a feature.
   */
  private void getFeature(final RoutingContext context) {
    getFeatures(context, ApiResponseType.FEATURE);
  }

  /**
   * Retrieves multiple features by ID.
   */
  private void getFeatures(final RoutingContext context) {
    getFeatures(context, ApiResponseType.FEATURE_COLLECTION);
  }

  private void getFeatures(final RoutingContext context, ApiResponseType apiResponseType) {
    try {
      final List<String> ids = apiResponseType == ApiResponseType.FEATURE_COLLECTION
              ? Query.queryParam(Query.FEATURE_ID, context)
              : Collections.singletonList(context.pathParam(Path.FEATURE_ID));

      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
              .withIds(ids)
              .withSelection(Query.getSelection(context))
              .withForce2D(force2D)
              .withRef(getRef(context))
              .withContext(spaceContext)
              .withAuthor(author);

      new IdsQuery(event, context, apiResponseType, skipCache)
              .execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
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

    executeConditionalOperationChain(false, context, getEmptyResponseTypeOr(context, ApiResponseType.FEATURE_COLLECTION), ifExists, ifNotExists, transactional, conflictResolution, true);
  }

  /**
   * Deletes a feature by ID.
   */
  private void deleteFeature(final RoutingContext context) {
    try {
      Map<String, Object> featureModification = Collections.singletonMap("featureIds",
              Collections.singletonList(context.pathParam(Path.FEATURE_ID)));
      final SpaceContext spaceContext = getSpaceContext(context);
      executeConditionalOperationChain(true, context, ApiResponseType.EMPTY, IfExists.DELETE, IfNotExists.RETAIN, true, ConflictResolution.ERROR,
              Collections.singletonList(featureModification), spaceContext);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Delete features by IDs or by tags.
   */
  private void deleteFeatures(final RoutingContext context) {
    try {
      final Set<String> featureIds = new HashSet<>(Query.queryParam(Query.FEATURE_ID, context));
      final String accept = context.request().getHeader(ACCEPT);
      final ApiResponseType responseType = APPLICATION_GEO_JSON.equals(accept) || APPLICATION_JSON.equals(accept)
              ? ApiResponseType.FEATURE_COLLECTION : ApiResponseType.EMPTY;
      final SpaceContext spaceContext = getSpaceContext(context);

      //Delete features by IDs
      if (!featureIds.isEmpty()) {
        Map<String, Object> featureModification = Collections.singletonMap("featureIds", new ArrayList<>(featureIds));

        executeConditionalOperationChain(false, context, responseType, IfExists.DELETE, IfNotExists.RETAIN, true,
                ConflictResolution.ERROR, Collections.singletonList(featureModification), spaceContext);
      }
      else {
        context.fail(
                new HttpException(HttpResponseStatus.BAD_REQUEST, "At least one identifier should be provided as a query parameter."));
      }
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  private void executeWriteFeatures(RoutingContext context, ApiResponseType responseType, FeatureModificationList modificationList,
      boolean partialUpdates, SpaceContext spaceContext, String author) {
    writeFeatures(context, modificationList, partialUpdates, spaceContext, author)
        .onSuccess(featureCollection -> {
          switch (responseType) {
            case EMPTY -> sendResponse(context, 200, (XyzSerializable) null);
            case FEATURE_COLLECTION -> sendResponse(context, 200, featureCollection);
            case FEATURE -> {
              try {
                sendResponse(context, 200, featureCollection.getFeatures().get(0));
              }
              catch (JsonProcessingException e) {
                sendErrorResponse(context, e);
              }
            }
          }
        });
  }

  private Future<FeatureCollection> writeFeatures(RoutingContext context, FeatureModificationList modificationList, boolean partialUpdates,
      SpaceContext spaceContext, String author) {
    return writeFeatures(context, modificationList.getModifications().stream()
        .map(modification -> new Modification()
            .withFeatureData(modification.getFeatureData())
            .withUpdateStrategy(toUpdateStrategy(modification.getOnFeatureExists(), modification.getOnFeatureNotExists(), modification.getOnMergeConflict()))
            .withPartialUpdates(partialUpdates)).collect(Collectors.toSet()), spaceContext, author);
  }

  /**
   * Performs all the API-level checks (e.g., authorization, parameter validation, ...) before actually calling the {@link FeatureHandler}
   * to execute the modification of the features.
   * @param context
   * @param modifications
   * @param spaceContext
   * @param author
   * @return
   */
  private Future<FeatureCollection> writeFeatures(RoutingContext context, Set<Modification> modifications, SpaceContext spaceContext,
      String author) {
    return Space.resolveSpace(getMarker(context), getSpaceId(context))
        .compose(space -> {
          boolean isDelete = hasDeletion(modifications);
          boolean isWrite = hasWrite(modifications);

          try {
            //Authorize the request and check some preconditions
            if (isDelete)
              FeatureAuthorization.authorizeWrite(context, space, true);
            if (isWrite)
              FeatureAuthorization.authorizeWrite(context, space, false);
            //TODO: authorizeComposite?
            checkReadOnly(space);

            XYZHubRESTVerticle.addStreamInfo(context, "SID", space.getStorage().getId());

            return space.resolveStorage(getMarker(context))
                .compose(connector -> resolveListenersAndProcessors(getMarker(context), space))
                .compose(v -> resolveExtendedSpaces(getMarker(context), space))
                .compose(v -> enforceUsageQuotas(context, space, spaceContext, isDelete && !isWrite))
                //Perform the actual feature writing
                .compose(v -> FeatureHandler.writeFeatures(getMarker(context), space, modifications, spaceContext, author))
                .recover(t -> {
                  if (t instanceof TooManyRequestsException throttleException)
                    XYZHubRESTVerticle.addStreamInfo(context, "THR", throttleException.reason); //Set the throttling reason at the stream-info header
                  return Future.failedFuture(t);
                });
          }
          catch (TooManyRequestsException e) {
            XYZHubRESTVerticle.addStreamInfo(context, "THR", e.reason); //Set the throttling reason at the stream-info header
            return Future.failedFuture(e);
          }
          catch (Exception e) {
            return Future.failedFuture(e);
          }
        });
  }

  private boolean hasDeletion(Set<Modification> modifications) {
    for (Modification modification : modifications)
      if (modification.getUpdateStrategy().onExists() == OnExists.DELETE
          || modification.getUpdateStrategy().onVersionConflict() == OnVersionConflict.DELETE)
        return true;
    return false;
  }

  private boolean hasWrite(Set<Modification> modifications) {
    for (Modification modification : modifications)
      if (modification.getUpdateStrategy().onExists() == OnExists.REPLACE
          || modification.getUpdateStrategy().onVersionConflict() == OnVersionConflict.REPLACE
          || modification.getUpdateStrategy().onVersionConflict() == OnVersionConflict.MERGE)
        return true;
    return false;
  }

  static Future<Void> enforceUsageQuotas(RoutingContext context, Space space, SpaceContext spaceContext, boolean isDeleteOnly) {
    final long maxFeaturesPerSpace = BaseHttpServerVerticle.getJWT(context).limits != null ? BaseHttpServerVerticle.getJWT(context).limits.maxFeaturesPerSpace : -1;
    if (maxFeaturesPerSpace <= 0)
      return Future.succeededFuture();

    return FeatureHandler.getCountForSpace(getMarker(context), space, spaceContext, BaseHttpServerVerticle.getAuthor(context))
        .compose(count -> {
          try {
            //Check the quota
            FeatureHandler.checkFeaturesPerSpaceQuota(space.getId(), maxFeaturesPerSpace, count, isDeleteOnly);
            return Future.succeededFuture();
          }
          catch (HttpException e) {
            return Future.failedFuture(e);
          }
        });
  }

  /**
   * Performs the mapping from the (legacy) API-level update strategy to the {@link UpdateStrategy}
   * to be used for the {@link com.here.xyz.events.WriteFeaturesEvent}.
   * @param ifExists
   * @param ifNotExists
   * @param conflictResolution
   * @return
   */
  private UpdateStrategy toUpdateStrategy(IfExists ifExists, IfNotExists ifNotExists, ConflictResolution conflictResolution) {
    return new UpdateStrategy(
        toOnExists(ifExists),
        toOnNotExists(ifNotExists),
        toOnVersionConflict(ifExists, conflictResolution),
        toOnMergeConflict(conflictResolution)
    );
  }

  private OnVersionConflict toOnVersionConflict(IfExists ifExists, ConflictResolution conflictResolution) {
    return switch (ifExists) {
      case RETAIN -> OnVersionConflict.RETAIN;
      case ERROR -> OnVersionConflict.ERROR;
      case DELETE -> OnVersionConflict.DELETE;
      case REPLACE, PATCH -> OnVersionConflict.REPLACE;
      case MERGE -> OnVersionConflict.MERGE;
    };
  }

  private OnMergeConflict toOnMergeConflict(ConflictResolution conflictResolution) {
    return switch (conflictResolution) {
      case ERROR -> OnMergeConflict.ERROR;
      case RETAIN -> OnMergeConflict.RETAIN;
      case REPLACE -> OnMergeConflict.REPLACE;
    };
  }

  private OnExists toOnExists(IfExists ifExists) {
    return switch (ifExists) {
      case RETAIN -> OnExists.RETAIN;
      case ERROR -> OnExists.ERROR;
      case DELETE -> OnExists.DELETE;
      case REPLACE, MERGE, PATCH -> OnExists.REPLACE;
    };
  }

  private OnNotExists toOnNotExists(IfNotExists ifNotExists) {
    return switch (ifNotExists) {
      case RETAIN -> OnNotExists.RETAIN;
      case ERROR -> OnNotExists.ERROR;
      case CREATE -> OnNotExists.CREATE;
    };
  }

  /**
   * Creates and executes a ModifyFeatureOp
   */
  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr, boolean useSpaceContext )
  {
    try {
        if (checkModificationOnSuper(context, getSpaceContext(context)))
          return;
        executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional, cr, null, useSpaceContext ? getSpaceContext(context) : DEFAULT);
      } catch (HttpException e) {
        sendErrorResponse(context, e);
      }
  }

  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr) {
     executeConditionalOperationChain(requireResourceExists, context, apiResponseTypeType, ifExists, ifNotExists, transactional, cr, false);
  }

  private void executeConditionalOperationChain(boolean requireResourceExists, final RoutingContext context,
      ApiResponseType apiResponseTypeType, IfExists ifExists, IfNotExists ifNotExists, boolean transactional, ConflictResolution cr,
      List<Map<String, Object>> featureModifications, SpaceContext spaceContext) {
    if (checkModificationOnSuper(context, spaceContext))
      return;

    String author = BaseHttpServerVerticle.getAuthor(context);
    ModifyFeaturesEvent event = new ModifyFeaturesEvent()
        .withAuthor(author)
        .withTransaction(transactional)
        .withContext(spaceContext)
        .withConflictDetectionEnabled(Query.getBoolean(context, Query.CONFLICT_DETECTION, false));
    int bodySize = context.getBody() != null ? context.getBody().length() : 0;

    try {
      ConditionalOperation task = buildConditionalOperation(event, context, apiResponseTypeType, featureModifications, ifNotExists,
          ifExists, transactional, cr, requireResourceExists, bodySize);
      final List<String> addTags = new ArrayList<>(Query.queryParam(Query.ADD_TAGS, context));
      final List<String> removeTags = new ArrayList<>(Query.queryParam(Query.REMOVE_TAGS, context));
      task.addTags = XyzNamespace.normalizeTags(addTags);
      task.removeTags = XyzNamespace.normalizeTags(removeTags);
      XyzNamespace.fixNormalizedTags(task.addTags);
      XyzNamespace.fixNormalizedTags(task.removeTags);
      task.prefixId = Query.getString(context, Query.PREFIX_ID, null);
      task.author = author;
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      logger.warn(getMarker(context), e.getMessage(), e);
      context.fail(e);
    }
  }

  private static boolean checkModificationOnSuper(RoutingContext context, SpaceContext spaceContext) {
    if (spaceContext != null && spaceContext.equals(SUPER)) {
      context.fail(
          new HttpException(HttpResponseStatus.FORBIDDEN, "It's not permitted to perform modifications through context " + SUPER + "."));
      return true;
    }
    return false;
  }

  private ConditionalOperation buildConditionalOperation(
      ModifyFeaturesEvent event,
      RoutingContext context,
      ApiResponseType apiResponseTypeType,
      List<Map<String, Object>> featureModifications,
      IfNotExists ifNotExists,
      IfExists ifExists,
      boolean transactional,
      ConflictResolution cr,
      boolean requireResourceExists,
      int bodySize) throws HttpException {
    if (featureModifications == null)
      return new ConditionalOperation(event, context, apiResponseTypeType, ifNotExists, ifExists, transactional, cr, requireResourceExists, bodySize);

    final List<FeatureEntry> featureEntries = ModifyFeatureOp.convertToFeatureEntries(featureModifications, ifNotExists, ifExists, cr);
    final ModifyFeatureOp modifyFeatureOp = new ModifyFeatureOp(featureEntries, transactional);
    return new ConditionalOperation(event, context, apiResponseTypeType, modifyFeatureOp, requireResourceExists, bodySize);
  }
}