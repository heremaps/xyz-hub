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

package com.here.xyz.hub.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.hash.Hashing;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.connectors.models.Space.CacheProfile;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.FeatureTaskHandler.InvalidStorageException;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.task.TaskPipeline.C1;
import com.here.xyz.hub.task.TaskPipeline.C2;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.hub.util.diff.Patcher.ConflictResolution;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.AsyncResult;
import io.vertx.ext.web.RoutingContext;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class FeatureTask<T extends Event<?>, X extends FeatureTask<T, ?>> extends Task<T, X> {

  private static final Logger logger = LogManager.getLogger();

  /**
   * The space for this operation.
   */
  public Space space;

  /**
   * The storage connector to be used for this operation.
   */
  public Connector storage;

  /**
   * The response.
   */
  @SuppressWarnings("rawtypes")
  private XyzResponse response;

  /**
   * The calculated cache key.
   */
  private String cacheKey;

  /**
   * The number of bytes the request body is / was having initially.
   */
  public final int requestBodySize;

  public static final class FeatureKey {

    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String BBOX = "bbox";
    public static final String PROPERTIES = "properties";
    public static final String SPACE = "space";
    public static final String CREATED_AT = "createdAt";
    public static final String UPDATED_AT = "updatedAt";
    public static final String UUID = "uuid";
    public static final String PUUID = "puuid";
    public static final String MUUID = "muuid";
  }

  private FeatureTask(T event, RoutingContext context, ApiResponseType responseType, boolean skipCache) {
    this(event, context, responseType, skipCache, 0);
  }

  private FeatureTask(T event, RoutingContext context, ApiResponseType responseType, boolean skipCache, int requestBodySize) {
    super(event, context, responseType, skipCache);
    event.setStreamId(getMarker().getName());

    if (context.pathParam(ApiParam.Path.SPACE_ID) != null) {
      event.setSpace(context.pathParam(ApiParam.Path.SPACE_ID));
    }
    this.requestBodySize = requestBodySize;
  }

  public CacheProfile getCacheProfile() {
    if (space == null || storage == null) {
      return null;
    }
    return space.getCacheProfile(skipCache, storage.capabilities.enableAutoCache);
  }

  @Override
  public String getCacheKey() {
    if (cacheKey != null) {
      return cacheKey;
    }
    try {
      //noinspection UnstableApiUsage
      cacheKey = Hashing.murmur3_128().newHasher()
          .putString(getEvent().getCacheString(), Charset.defaultCharset())
          .putString(responseType.toString(), Charset.defaultCharset())
          .putLong(space.contentUpdatedAt)
          .hash()
          .toString();
      return cacheKey;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * The hook which will be called once all pre-processors have been called. The hook will get the pre-processed event as parameter. The
   * hook will *not* be called if no pre-processors have been defined for the space. The hook may be overridden in sub-classes.
   */
  public void onPreProcessed(T event) {
  }

  @Override
  public String getEtag() {
    if (response == null) {
      return null;
    }
    return response.getEtag();
  }

  /**
   * Returns the current response.
   *
   * @return the current response.
   */
  @SuppressWarnings("rawtypes")
  public XyzResponse getResponse() {
    return response;
  }

  /**
   * Sets the response to the given value.
   *
   * @param response the response.
   * @return the previously set value.
   */
  @SuppressWarnings("rawtypes")
  public XyzResponse setResponse(XyzResponse response) {
    final XyzResponse old = this.response;
    this.response = response;
    return old;
  }

  /**
   * Returns the response feature collection, if the response is a feature collection.
   *
   * @return the response feature collection, if the response is a feature collection.
   * @deprecated please rather use {@link #getResponse()}
   */
  public FeatureCollection responseCollection() {
    if (response instanceof FeatureCollection) {
      return (FeatureCollection) response;
    }
    return null;
  }

  private static RpcClient getRpcClient(Connector refConnector) throws HttpException {
    try {
      return RpcClient.getInstanceFor(refConnector);
    }
    catch (IllegalStateException e) {
      throw new HttpException(BAD_GATEWAY, "Connector not ready.");
    }
  }

  abstract static class ReadQuery<T extends QueryEvent<?>, X extends FeatureTask<T, ?>> extends FeatureTask<T, X> {

    private ReadQuery(T event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    private ReadQuery(T event, RoutingContext context, ApiResponseType apiResponseTypeType) {
      this(event, context, apiResponseTypeType, true);
    }

    boolean hasPropertyQuery() {
      return getEvent().getPropertiesQuery() != null;
    }
  }

  public static class GeometryQuery extends ReadQuery<GetFeaturesByGeometryEvent, GeometryQuery> {

    public final String refSpaceId;
    private final String refFeatureId;
    public Space refSpace;
    private Connector refConnector;

    public GeometryQuery(GetFeaturesByGeometryEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      this(event, context, apiResponseTypeType, skipCache, null, null);
    }

    public GeometryQuery(GetFeaturesByGeometryEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache,
        String refSpaceId, String refFeatureId) {
      super(event, context, apiResponseTypeType, skipCache);
      this.refFeatureId = refFeatureId;
      this.refSpaceId = refSpaceId;
    }

    @Override
    public TaskPipeline<GeometryQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(this::resolveRefSpace)
          .then(this::resolveRefConnector)
          .then(FeatureAuthorization::authorize)
          .then(this::loadObject)
          .then(this::verifyResourceExists)
          .then(FeatureTaskHandler::validate)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::writeCache);
    }

    private void verifyResourceExists(GeometryQuery task, Callback<GeometryQuery> callback) {
      if (this.getEvent().getGeometry() == null && this.getEvent().getH3Index() == null) {
        callback.exception(new HttpException(NOT_FOUND, "The 'refFeatureId' : '" + refFeatureId + "' does not exist."));
      } else {
        callback.call(task);
      }
    }

    private void resolveRefConnector(final GeometryQuery gq, final Callback<GeometryQuery> c) {
      try {
        if (refSpace == null || refSpace.getStorage().getId().equals(space.getStorage().getId())) {
          refConnector = storage;
          c.call(gq);
          return;
        }
        Service.connectorConfigClient.get(getMarker(), refSpace.getStorage().getId(), (arStorage) -> {
          if (arStorage.failed()) {
            c.exception(new InvalidStorageException("Unable to load the definition for this storage."));
            return;
          }
          refConnector = arStorage.result();
          c.call(gq);
        });
      } catch (Exception e) {
        c.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the definition for this storage.", e));
      }
    }

    private void resolveRefSpace(final GeometryQuery gq, final Callback<GeometryQuery> c) {
      try {
        if (refSpaceId == null) {
          c.call(gq);
          return;
        }
        //Load the space definition.
        Service.spaceConfigClient.get(getMarker(), refSpaceId)
            .onFailure(t -> c.exception(new HttpException(BAD_REQUEST, "The resource ID '" + refSpaceId + "' does not exist!", t)))
            .onSuccess(space -> {
              refSpace = space;
              if (refSpace == null)
                c.exception(new HttpException(BAD_REQUEST, "The resource ID '" + refSpaceId + "' does not exist!"));
              else {
                gq.getEvent().setParams(gq.space.getStorage().getParams());
                c.call(gq);
              }
            });
      }
      catch (Exception e) {
        c.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition.", e));
      }
    }

    @SuppressWarnings("serial")
    private void loadObject(final GeometryQuery gq, final Callback<GeometryQuery> c) {
      if (gq.getEvent().getGeometry() != null || gq.getEvent().getH3Index() != null) {
        c.call(this);
        return;
      }

      final LoadFeaturesEvent event = new LoadFeaturesEvent()
          .withStreamId(getMarker().getName())
          .withSpace(refSpaceId)
          .withParams(this.refSpace.getStorage().getParams())
          .withIdsMap(new HashMap<String, String>() {{
            put(refFeatureId, null);
          }});

      try {
        getRpcClient(refConnector).execute(getMarker(), event, r -> processLoadEvent(c, event, r));
      }
      catch (Exception e) {
        logger.warn(gq.getMarker(), "Error trying to process LoadFeaturesEvent.", e);
        c.exception(e);
      }
    }

    void processLoadEvent(Callback<GeometryQuery> callback, LoadFeaturesEvent event, AsyncResult<XyzResponse> r) {
      if (r.failed()) {
        if (r.cause() instanceof Exception) {
          callback.exception((Exception) r.cause());
        } else {
          callback.exception(new Exception(r.cause()));
        }
        return;
      }

      try {
        final XyzResponse response = r.result();
        if (!(response instanceof FeatureCollection)) {
          callback.exception(Api.responseToHttpException(response));
          return;
        }
        final FeatureCollection collection = (FeatureCollection) response;
        final List<Feature> features = collection.getFeatures();

        if (features.size() == 1) {
          this.getEvent().setGeometry(features.get(0).getGeometry());
        }

        callback.call(this);
      } catch (Exception e) {
        callback.exception(e);
      }
    }
  }

  public static class BBoxQuery extends ReadQuery<GetFeaturesByBBoxEvent<?>, BBoxQuery> {

    public BBoxQuery(GetFeaturesByBBoxEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<BBoxQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::validate)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class TileQuery extends ReadQuery<GetFeaturesByTileEvent, TileQuery> {

    /**
     * A local copy of some transformation-relevant properties from the event object.
     * NOTE: The event object is not in memory in the response-phase anymore.
     *
     * @see Task#consumeEvent()
     */
    TransformationContext transformationContext;

    public TileQuery(GetFeaturesByTileEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
      transformationContext = new TransformationContext(event.getX(), event.getY(), event.getLevel(), event.getMargin());
    }

    @Override
    public TaskPipeline<TileQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::validate)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::transformResponse)
          .then(FeatureTaskHandler::writeCache);
    }

    static class TransformationContext {
      TransformationContext(int x, int y, int level, int margin) {
        this.x = x;
        this.y = y;
        this.level = level;
        this.margin = margin;
      }

      int x;
      int y;
      int level;
      int margin;
    }
  }

  public static class IdsQuery extends FeatureTask<GetFeaturesByIdEvent, IdsQuery> {

    public IdsQuery(GetFeaturesByIdEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    public TaskPipeline<IdsQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::convertResponse)
          .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class LoadFeaturesQuery extends FeatureTask<LoadFeaturesEvent, LoadFeaturesQuery> {

    public LoadFeaturesQuery(LoadFeaturesEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    public TaskPipeline<LoadFeaturesQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(this::postResolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::convertResponse)
          .then(FeatureTaskHandler::writeCache);
    }

    private void postResolveSpace(LoadFeaturesQuery task, Callback<LoadFeaturesQuery> callback) {
      task.getEvent().setEnableHistory(task.space.isEnableHistory());
      callback.call(task);
    }
  }

  public static class IterateQuery extends ReadQuery<IterateFeaturesEvent, IterateQuery> {

    public IterateQuery(IterateFeaturesEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<IterateQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::validate)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class IterateHistoryQuery extends ReadQuery<IterateHistoryEvent, IterateHistoryQuery> {
    public IterateHistoryQuery(IterateHistoryEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<IterateHistoryQuery> getPipeline() {
      return TaskPipeline.create(this)
              .then(FeatureTaskHandler::resolveSpace)
              .then(FeatureAuthorization::authorize)
              .then(FeatureTaskHandler::validate)
              .then(FeatureTaskHandler::readCache)
              .then(FeatureTaskHandler::invoke)
              .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class SearchQuery extends ReadQuery<SearchForFeaturesEvent<?>, SearchQuery> {

    public SearchQuery(SearchForFeaturesEvent<?> event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<SearchQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::validate)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::writeCache);
    }
  }

  //TODO: Remove that implementation finally
  @Deprecated
  public static class CountQuery extends ReadQuery<CountFeaturesEvent, CountQuery> {

    public CountQuery(CountFeaturesEvent event, RoutingContext context, ApiResponseType apiResponseTypeType) {
      super(event, context, apiResponseTypeType);
    }

    @Override
    public TaskPipeline<CountQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::invoke);
    }
  }

  public static class GetStatistics extends FeatureTask<GetStatisticsEvent, GetStatistics> {

    public GetStatistics(GetStatisticsEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<GetStatistics> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::readCache)
          .then(FeatureTaskHandler::invoke)
          .then(FeatureTaskHandler::convertResponse)
          .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class GetHistoryStatistics extends FeatureTask<GetHistoryStatisticsEvent, GetHistoryStatistics> {

    public GetHistoryStatistics(GetHistoryStatisticsEvent event, RoutingContext context, ApiResponseType apiResponseTypeType, boolean skipCache) {
      super(event, context, apiResponseTypeType, skipCache);
    }

    @Override
    public TaskPipeline<GetHistoryStatistics> getPipeline() {
      return TaskPipeline.create(this)
              .then(FeatureTaskHandler::resolveSpace)
              .then(FeatureAuthorization::authorize)
              .then(FeatureTaskHandler::readCache)
              .then(FeatureTaskHandler::validate)
              .then(FeatureTaskHandler::invoke)
              .then(FeatureTaskHandler::writeCache);
    }
  }

  public static class DeleteOperation extends FeatureTask<DeleteFeaturesByTagEvent, DeleteOperation> {

    public DeleteOperation(DeleteFeaturesByTagEvent event, RoutingContext context, ApiResponseType apiResponseTypeType) {
      super(event, context, apiResponseTypeType, true);
    }

    @Override
    public TaskPipeline<DeleteOperation> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureTaskHandler::checkPreconditions)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::invoke);
    }
  }

  public static class ModifySpaceQuery extends FeatureTask<ModifySpaceEvent, ModifySpaceQuery> {

    Operation manipulatedOp;
    com.here.xyz.models.hub.Space manipulatedSpaceDefinition;

    ModifySpaceQuery(ModifySpaceEvent event, RoutingContext context, ApiResponseType apiResponseTypeType) {
      super(event, context, apiResponseTypeType, true);
    }

    //That hook will be called only if there were pre-processors which have been called and it sets the manipulatedSpaceDefinition value
    @Override
    public void onPreProcessed(ModifySpaceEvent event) {
      manipulatedOp = event.getOperation();
      //FIXME: Don't take the incoming spaceDefinition as is. Instead only merge the non-admin top-level properties and the connector config of the according processor (Processors should NOT be able to manipulate connector registrations of other connections at the space)
      manipulatedSpaceDefinition = event.getSpaceDefinition();
    }

    @Override
    public TaskPipeline<ModifySpaceQuery> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureTaskHandler::invoke);
    }
  }

  public static class ConditionalOperation extends FeatureTask<ModifyFeaturesEvent, ConditionalOperation> {

    public ModifyFeatureOp modifyOp;
    public IfNotExists ifNotExists;
    public IfExists ifExists;
    public boolean transactional;
    public ConflictResolution conflictResolution;
    public List<Feature> unmodifiedFeatures;
    private final boolean requireResourceExists;
    public List<String> addTags;
    public List<String> removeTags;
    public String prefixId;
    private Map<Object, Integer> positionById;
    private LoadFeaturesEvent loadFeaturesEvent;
    public boolean hasNonModified;

    public ConditionalOperation(ModifyFeaturesEvent event, RoutingContext context, ApiResponseType apiResponseTypeType,
        ModifyFeatureOp modifyOp, boolean requireResourceExists, int requestBodySize) {
      super(event, context, apiResponseTypeType, true, requestBodySize);
      this.modifyOp = modifyOp;
      this.requireResourceExists = requireResourceExists;
    }

    public ConditionalOperation(ModifyFeaturesEvent event, RoutingContext context, ApiResponseType apiResponseTypeType,
        IfNotExists ifNotExists, IfExists ifExists, boolean transactional, ConflictResolution conflictResolution, boolean requireResourceExists) {
      this(event, context, apiResponseTypeType, null, requireResourceExists);
      this.ifNotExists = ifNotExists;
      this.ifExists = ifExists;
      this.transactional = transactional;
      this.conflictResolution = conflictResolution;
    }

    @Override
    public TaskPipeline<ConditionalOperation> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureTaskHandler::registerRequestMemory)
          .then(FeatureTaskHandler::throttle)
          .then(FeatureTaskHandler::injectSpaceParams)
          .then(FeatureTaskHandler::checkPreconditions)
          .then(FeatureTaskHandler::prepareModifyFeatureOp)
          .then(FeatureTaskHandler::preprocessConditionalOp)
          .then(this::loadObjects)
          .then(this::verifyResourceExists)
          .then(FeatureTaskHandler::updateTags)
          .then(FeatureTaskHandler::processConditionalOp)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::enforceUsageQuotas)
          .then(this::extractUnmodifiedFeatures)
          .then(this::cleanup)
          .then(FeatureTaskHandler::invoke);
    }

    @Override
    public void execute(C1<ConditionalOperation> onSuccess, final C2<ConditionalOperation, Throwable> onException) {
      C1<ConditionalOperation> wrappedSuccessHandler = task -> {
        requestCompleted(task);
        onSuccess.call(task);
      };
      C2<ConditionalOperation, Throwable> wrappedExceptionHandler = (task, ex) -> {
        requestCompleted(task);
        onException.call(task, ex);
      };
      super.execute(wrappedSuccessHandler, wrappedExceptionHandler);
    }

    private void requestCompleted(FeatureTask task) {
      if (task.storage != null)
        FeatureTaskHandler.deregisterRequestMemory(task.storage.id, task.requestBodySize);
    }

    @Override
    protected <X extends Task<?, X>> void cleanup(X task, Callback<X> callback) {
      super.cleanup(task, callback);
      modifyOp = null;
      callback.call(task);
    }

    private <X extends FeatureTask<?, X>> void extractUnmodifiedFeatures(X task, Callback<X> callback) {
      if (modifyOp != null && modifyOp.entries != null)
        unmodifiedFeatures = modifyOp.entries.stream().filter(e -> !e.isModified).map(fe -> fe.result).collect(Collectors.toList());
      callback.call(task);
    }

    private void verifyResourceExists(ConditionalOperation task, Callback<ConditionalOperation> callback) {
      if (task.requireResourceExists && task.modifyOp.entries.get(0).head == null) {
        callback.exception(new HttpException(NOT_FOUND, "The requested resource does not exist."));
      } else {
        callback.call(task);
      }
    }

    private void loadObjects(final ConditionalOperation s, final Callback<ConditionalOperation> c) {
      final LoadFeaturesEvent event = toLoadFeaturesEvent();
      if (event == null) {
        c.call(this);
        return;
      }
      FeatureTaskHandler.setAdditionalEventProps(s, s.storage, event);
      try {
        getRpcClient(s.storage).execute(getMarker(), event, r -> processLoadEvent(c, event, r));
      }
      catch (Exception e) {
        logger.warn(s.getMarker(), "Error trying to process LoadFeaturesEvent.", e);
        c.exception(e);
      }
    }

    LoadFeaturesEvent toLoadFeaturesEvent() {
      if (loadFeaturesEvent != null) {
        return loadFeaturesEvent;
      }

      if (modifyOp.entries.size() == 0) {
        return null;
      }

      final HashMap<String, String> idsMap = new HashMap<>();
      for (Entry<Feature> entry : modifyOp.entries) {
        if (entry.input.get("id") instanceof String) {
          idsMap.put((String) entry.input.get("id"), entry.inputUUID);
        }
      }
      if (idsMap.size() == 0) {
        return null;
      }

      final LoadFeaturesEvent event = new LoadFeaturesEvent()
          .withStreamId(getMarker().getName())
          .withSpace(space.getId())
          .withParams(space.getStorage().getParams())
          .withEnableHistory(space.isEnableHistory())
          .withIdsMap(idsMap);

      loadFeaturesEvent = event;
      return event;
    }

    void processLoadEvent(Callback<ConditionalOperation> callback, LoadFeaturesEvent event, AsyncResult<XyzResponse> r) {
      final Map<String, String> idsMap = event.getIdsMap();
      if (r.failed()) {
        if (r.cause() instanceof Exception) {
          callback.exception((Exception) r.cause());
        } else {
          callback.exception(new Exception(r.cause()));
        }
        return;
      }

      try {
        final XyzResponse response = r.result();
        if (!(response instanceof FeatureCollection)) {
          callback.exception(Api.responseToHttpException(response));
          return;
        }
        final FeatureCollection collection = (FeatureCollection) response;
        final List<Feature> features = collection.getFeatures();

        // For each input feature there could be 0, 1(head state) or 2 (head state and base state) features in the response
        if (features == null) {
          callback.call(this);
          return;
        }

        for (final Feature feature : features) {
          final String id = feature.getId();

          // The uuid the client has requested.
          final String requestedUuid = idsMap.get(id);

          int position = getPositionForId(feature.getId());
          if (position == -1) { // There is no object with this ID in the input states
            continue;
          }

          if (feature.getProperties() == null || feature.getProperties().getXyzNamespace() == null) {
            throw new IllegalStateException("Received a feature with missing space namespace properties for object '" + id + "'");
          }

          String uuid = feature.getProperties().getXyzNamespace().getUuid();

          // Set the head state( i.e. the latest version in the database )
          if (modifyOp.entries.get(position).head == null || uuid != null && !uuid.equals(requestedUuid)) {
            modifyOp.entries.get(position).head = feature;
          }

          // Set the base state( i.e. the original version that the user was editing )
          // Note: The base state must not be empty. If the connector doesn't support history and doesn't return the base state, use the
          // head state instead.
          if (modifyOp.entries.get(position).base == null || uuid != null && uuid.equals(requestedUuid)) {
            modifyOp.entries.get(position).base = feature;
          }
        }

        callback.call(this);
      } catch (Exception e) {
        callback.exception(e);
      }
    }

    int getPositionForId(Object id) {
      if (id == null) {
        return -1;
      }

      if (positionById == null) {
        positionById = new HashMap<>();
        for (int i = 0; i < modifyOp.entries.size(); i++) {
          final Map<String, Object> input = modifyOp.entries.get(i).input;
          if (input != null && input.get("id") instanceof String) {
            positionById.put(input.get("id"), i);
          }
        }
      }

      return positionById.get(id) == null ? -1 : positionById.get(id);
    }
  }

  /**
   * Minimal version of Conditional Operation used on admin events endpoint.
   * Contains some limitations because doesn't implement the full pipeline.
   * If you want to use the full conditional operation pipeline, you should request
   * through Features API.
   * Known limitations:
   * - Cannot perform validation of existing resources per operation type
   */
  public static class ModifyFeaturesTask extends FeatureTask<ModifyFeaturesEvent, ModifyFeaturesTask> {

    public ModifyFeaturesTask(ModifyFeaturesEvent event, RoutingContext context, ApiResponseType responseType, boolean skipCache) {
      super(event, context, responseType, skipCache);
    }

    @Override
    public TaskPipeline<ModifyFeaturesTask> getPipeline() {
      return TaskPipeline.create(this)
          .then(FeatureTaskHandler::resolveSpace)
          .then(FeatureTaskHandler::checkPreconditions)
          .then(FeatureTaskHandler::injectSpaceParams)
          .then(FeatureAuthorization::authorize)
          .then(FeatureTaskHandler::enforceUsageQuotas)
          .then(FeatureTaskHandler::invoke);
    }
  }
}
