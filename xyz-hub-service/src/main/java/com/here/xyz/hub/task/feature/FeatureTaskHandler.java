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

package com.here.xyz.hub.task.feature;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;
import static com.here.xyz.hub.rest.ApiResponseType.MVT;
import static com.here.xyz.hub.rest.ApiResponseType.MVT_FLATTENED;
import static com.here.xyz.hub.task.feature.AbstractFeatureTask.FeatureKey.BBOX;
import static com.here.xyz.hub.task.feature.AbstractFeatureTask.FeatureKey.ID;
import static com.here.xyz.hub.task.feature.AbstractFeatureTask.FeatureKey.PROPERTIES;
import static com.here.xyz.hub.task.feature.AbstractFeatureTask.FeatureKey.TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContentModifiedNotification;
import com.here.xyz.events.Event;
import com.here.xyz.events.TrustedParams;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.hub.AbstractHttpServerVerticle;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.Context;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ICallback;
import com.here.xyz.hub.task.ModifyFeatureOp;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.task.feature.TileQuery.TransformationContext;
import com.here.xyz.hub.task.ModifyFeatureOp.FeatureEntry;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.task.ModifyOp.ModifyOpError;
import com.here.xyz.hub.util.geo.MapBoxVectorTileBuilder;
import com.here.xyz.hub.util.geo.MapBoxVectorTileFlattenedBuilder;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.ModifiedEventResponse;
import com.here.xyz.responses.ModifiedPayloadResponse;
import com.here.xyz.responses.ModifiedResponseResponse;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.PropertiesStatistics.Searchable;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.Cookie;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class FeatureTaskHandler {

  private static final Logger logger = LogManager.getLogger();

  private static final ExpiringMap<String, Long> countCache = ExpiringMap.builder()
      .maxSize(1024)
      .variableExpiration()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .build();
  private static final byte JSON_VALUE = 1;
  private static final byte BINARY_VALUE = 2;
  private static SnsAsyncClient snsClient;
  private static final ConcurrentHashMap<String, Long> contentModificationTimers = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Long> contentModificationAdminTimers = new ConcurrentHashMap<>();
  private static final long CONTENT_MODIFICATION_INTERVAL = 1_000; //1s
  private static final long CONTENT_MODIFICATION_ADMIN_INTERVAL = 300_000; //5min

  /**
   * The latest versions of the space contents as it has been seen on this service node. The key is the space ID and the value is the
   * according "latest seen content version".
   * There is neither a guarantee that the version value is pointing to the actual latest version of the space's content nor there is a
   * guarantee that it's defined at all.
   * If the value exists for a space and it points to a value > 0, that is the version of the latest write to that space as it has been
   * performed by this service-node.
   */
  private static ConcurrentHashMap<String, Integer> latestSeenContentVersions = new ConcurrentHashMap<>();

  /**
   * Contains the amount of all in-flight requests for each storage ID.
   */
  private static ConcurrentHashMap<String, LongAdder> inflightRequestMemory = new ConcurrentHashMap<>();
  private static LongAdder globalInflightRequestMemory = new LongAdder();

  /**
   * Sends the event to the connector client and write the response as the responseCollection of the task.
   *
   * @param task the FeatureTask instance
   * @param callback the callback handler
   * @param <T> the type of the FeatureTask
   */
  public static <T extends AbstractFeatureTask> void invoke(T task, ICallback<T> callback) {
    /*
    In case there is already, nothing has to be done here (happens if the response was set by an earlier process in the task pipeline
    e.g. when having a cache hit)
     */
    if (task.getResponse() != null) {
      callback.success(task);
      return;
    }
    /**
     * NOTE: The event may only be consumed once. Once it was consumed it should only be referenced in the request-phase. Referencing it in the
     *     response-phase will keep the whole event-data in the memory and could cause many major GCs to because of large request-payloads.
     *
     * @see NakshaTask#consumeEvent()
     */
    Event event = task.consumeEvent();

    if (!task.storageConnector.active) {
      if (event instanceof ModifySpaceEvent && ((ModifySpaceEvent) event).getOperation() == ModifySpaceEvent.Operation.DELETE) {
        /*
        If the connector is inactive, allow space deletions. In this case only the space configuration gets deleted. The
        deactivated connector does not get invoked so the underlying dataset stays untouched.
        */
        task.setResponse(new SuccessResponse().withStatus("OK"));
        callback.success(task);
      }
      else {
        //Abort further processing - do not: notifyProcessors, notifyListeners, invoke connector
        callback.throwException(new HttpException(BAD_REQUEST, "Related connector is not active: " + task.storageConnector.id));
      }
      return;
    }

    String eventType = event.getClass().getSimpleName();

    //Pre-process the event by executing potentially registered processors
    notifyProcessors(task, eventType, event, preProcessingResult -> {
      if (preProcessingResult.failed() || preProcessingResult.result() instanceof ErrorResponse) {
        handleProcessorFailure(task.getMarker(), preProcessingResult, callback);
        return;
      }
      Event eventToExecute = extractPayloadFromResponse(
          (ModifiedPayloadResponse<? extends ModifiedPayloadResponse>) preProcessingResult.result(), Event.class, event);
      //Call the pre-processed hook if there is one
      if (eventToExecute != event && callProcessedHook(task, eventToExecute, callback)) {
        return;
      }
      EventResponseContext responseContext = new EventResponseContext(eventToExecute);

      Event<? extends Event> requestListenerPayload = null;
      if (task.space.hasRequestListeners()) {
        //Clone the request-event to be used for request-listener notifications (see below) if there are some to be performed
        requestListenerPayload = eventToExecute.copy();
      }

      //CMEKB-2779 Remove failed entries before calling storage client
      if (eventToExecute instanceof ModifyFeaturesEvent) {
        ((ModifyFeaturesEvent) eventToExecute).setFailed(null);
      }
      //Do the actual storage call
      try {
        setAdditionalEventProps(task, task.storageConnector, eventToExecute);
        final long storageRequestStart = Core.currentTimeMillis();
        responseContext.rpcContext = getRpcClient(task.storageConnector).execute(task.getMarker(), eventToExecute, storageResult -> {
          if (task.getState().isFinal()) return;
          addConnectorPerformanceInfo(task, Core.currentTimeMillis() - storageRequestStart, responseContext.rpcContext, "S");
          if (storageResult.failed()) {
            callback.throwException(storageResult.cause());
            return;
          }
          XyzResponse response = storageResult.result();
          responseContext.enrichResponse(task, response);

          //Do the post-processing here before sending back the response and notifying response-listeners
          notifyProcessors(task, eventType, response, postProcessingResult -> {
            if (task.getState().isFinal()) return;
            if (postProcessingResult.failed() || postProcessingResult.result() instanceof ErrorResponse) {
              handleProcessorFailure(task.getMarker(), postProcessingResult, callback);
              return;
            }
            XyzResponse responseToSend = extractPayloadFromResponse(
                (ModifiedPayloadResponse<? extends ModifiedPayloadResponse>) postProcessingResult.result(),
                XyzResponse.class, storageResult.result());
            task.setResponse(responseToSend);
            //Success! Call the callback to send the response to the client.
            callback.success(task);
            //Send the event's (post-processed) response to potentially registered response-listeners
            notifyListeners(task, eventType, responseToSend);
            if (ModifyFeaturesEvent.class.getSimpleName().equals(eventType)) {
              //Set the latest version as it has been seen on this node, after the modification
              if (responseToSend instanceof FeatureCollection && ((FeatureCollection) responseToSend).getVersion() != null)
                setLatestSeenContentVersion(task.space, ((FeatureCollection) responseToSend).getVersion());
              //Send an additional ContentModifiedNotification to all components which are interested
              scheduleContentModifiedNotification(task);
            }
          });
        });
        AbstractHttpServerVerticle.addStreamInfo(task.routingContext, "SReqSize", responseContext.rpcContext.getRequestSize());
        task.addCancellingHandler(unused -> responseContext.rpcContext.cancelRequest());
      }
      catch (IllegalStateException e) {
        cancelRPC(responseContext.rpcContext);
        logger.warn(task.getMarker(), e.getMessage(), e);
        callback.throwException(new HttpException(BAD_REQUEST, e.getMessage(), e));
        return;
      }
      catch (HttpException e) {
        cancelRPC(responseContext.rpcContext);
        logger.warn(task.getMarker(), e.getMessage(), e);
        callback.throwException(e);
        return;
      }
      catch (Exception e) {
        cancelRPC(responseContext.rpcContext);
        logger.error(task.getMarker(), "Unexpected error executing the storage event.", e);
        callback.throwException(new HttpException(INTERNAL_SERVER_ERROR, "Unexpected error executing the storage event.", e));
        return;
      }

      //Update the contentUpdatedAt timestamp to indicate that the data in this space was modified
      if (task instanceof ConditionalModifyFeaturesTask || task instanceof DeleteFeaturesByTagTask) {
        long now = Core.currentTimeMillis();
        if (now - task.space.contentUpdatedAt > Space.CONTENT_UPDATED_AT_INTERVAL_MILLIS) {
          task.space.contentUpdatedAt = Core.currentTimeMillis();
          task.space.volatility = task.space.getVolatility();
          Service.spaceConfigClient.store(task.getMarker(), task.space)
              .onSuccess(v -> logger.info(task.getMarker(), "Updated contentUpdatedAt for space {}", task.space.getId()))
              .onFailure(t -> logger.error(task.getMarker(), "Error while updating contentUpdatedAt for space {}", task.space.getId(), t));
        }
      }
      //Send event to potentially registered request-listeners
      if (requestListenerPayload != null)
        notifyListeners(task, eventType, requestListenerPayload);
    });
    if (event instanceof ModifySpaceEvent) sendSpaceModificationNotification(task.getMarker(), event);
  }

  private static RpcClient getRpcClient(Connector refConnector) throws HttpException {
    try {
      return RpcClient.getInstanceFor(refConnector);
    }
    catch (IllegalStateException e) {
      throw new HttpException(BAD_GATEWAY, "Connector not ready.");
    }
  }

  private static void cancelRPC(RpcContext rpcContext) {
    if (rpcContext != null) {
      rpcContext.cancelRequest();
    }
  }

  private static <T extends AbstractFeatureTask> void addConnectorPerformanceInfo(T task, long storageTime, RpcContext rpcContext, String eventPrefix) {
    AbstractHttpServerVerticle.addStreamInfo(task.routingContext, eventPrefix + "Time", storageTime);
    if (rpcContext != null)
      AbstractHttpServerVerticle.addStreamInfo(task.routingContext, eventPrefix + "ResSize", rpcContext.getResponseSize());
  }

  private static <T extends AbstractFeatureTask> void addProcessorPerformanceInfo(T task, long processorTime, RpcContext rpcContext, int processorNo) {
    addConnectorPerformanceInfo(task, processorTime, rpcContext, "P" + processorNo);
  }

  private static XyzResponse transformCacheValue(byte[] value) throws JsonProcessingException {
    byte type = value[0];
    byte[] byteValue = Buffer.buffer(value).getBytes(1, value.length);
    switch (type) {
      case JSON_VALUE: {
        return XyzSerializable.deserialize(new String(byteValue));
      }
      case BINARY_VALUE: {
        return BinaryResponse.fromByteArray(byteValue);
      }
    }
    return null;
  }

  private static byte[] transformCacheValue(XyzResponse value) {
    byte[] type = {value instanceof BinaryResponse ? BINARY_VALUE : JSON_VALUE};
    Buffer b = Buffer.buffer(type).appendBytes(value.toByteArray());
    return b.getBytes();
  }

  public static <T extends AbstractFeatureTask> void readCache(T task, ICallback<T> callback) {
    if (task.getCacheProfile().serviceTTL > 0) {
      String cacheKey = task.getCacheKey();

      //Check the cache
      final long cacheRequestStart = Core.currentTimeMillis();
      Service.cacheClient.get(cacheKey).onSuccess(cacheResult -> {
        if (cacheResult == null) {
          //Cache MISS: Just go on in the task pipeline
          AbstractHttpServerVerticle.addStreamInfo(task.routingContext, "CH",0);
          logger.info(task.getMarker(), "Cache MISS for cache key {}", cacheKey);
        }
        else {
          //Cache HIT: Set the response for the task to the result from the cache so invoke (in the task pipeline) won't have anything to do
          try {
            task.setResponse(transformCacheValue(cacheResult));
            task.setCacheHit(true);
            AbstractHttpServerVerticle.addStreamInfo(task.routingContext, "CH", 1);
            logger.info(task.getMarker(), "Cache HIT for cache key {}", cacheKey);
          }
          catch (JsonProcessingException e) {
            //Actually, this should never happen as we're controlling how the data is written to the cache, but you never know ;-)
            //Treating an error as a Cache MISS
            logger.info(task.getMarker(), "Cache MISS (as of JSON parse exception) for cache key {} {}", cacheKey, e);
          }
        }
        AbstractHttpServerVerticle.addStreamInfo(task.routingContext, "CTime", Core.currentTimeMillis() - cacheRequestStart);
        callback.success(task);
      });
    }
    else {
      task.getEvent().setPreferPrimaryDataSource(true);
      callback.success(task);
    }
  }

  public static <T extends AbstractFeatureTask> void writeCache(T task, ICallback<T> callback) {
    callback.success(task);
    //From here everything is done asynchronous
    final CacheProfile cacheProfile = task.getCacheProfile();
    //noinspection rawtypes
    XyzResponse response = task.getResponse();
    if (cacheProfile.serviceTTL > 0 && response != null && !task.isCacheHit()
        && !(response instanceof NotModifiedResponse) && !(response instanceof ErrorResponse)) {
      String cacheKey = task.getCacheKey();
      if (cacheKey == null) {
        String npe = "cacheKey is null. Couldn't write cache.";
        logger.error(task.getMarker(), npe);
        throw new NullPointerException(npe);
      }
      logger.debug(task.getMarker(), "Writing entry with cache key {} to cache", cacheKey);
      Service.cacheClient.set(cacheKey, transformCacheValue(response), cacheProfile.serviceTTL);
    }
  }

  /**
   * @param task the FeatureTask instance
   * @param event The pre-processed event
   * @param callback The callback to be called in case of exception
   * @param <T> the type of the FeatureTask
   * @return Whether to stop the execution as of an exception occurred
   */
  private static <T extends AbstractFeatureTask> boolean callProcessedHook(T task, Event event, ICallback<T> callback) {
    try {
      task.onPreProcessed(event);
      return false;
    } catch (Exception e) {
      callback.throwException(e);
      return true;
    }
  }

  private static <R extends Payload> R extractPayloadFromResponse(ModifiedPayloadResponse<? extends ModifiedPayloadResponse> response,
      Class<R> type,
      R originalPayload) {
    if (response == null) {
      return originalPayload;
    }
    if (response instanceof ModifiedEventResponse) {
      R e = type.cast(((ModifiedEventResponse) response).getEvent());
      return e != null ? e : originalPayload;
    } else if (response instanceof ModifiedResponseResponse) {
      R r = type.cast(((ModifiedResponseResponse) response).getResponse());
      return r != null ? r : originalPayload;
    } else {
      throw new RuntimeException("Unexpected error while extracting the payload from a processor response.");
    }
  }

  private static <T extends AbstractFeatureTask> void handleProcessorFailure(Marker marker, AsyncResult<XyzResponse> processingResult,
      ICallback<T> callback) {
    if (processingResult.failed())
      callback.throwException(processingResult.cause());
    else if (processingResult.result() instanceof ErrorResponse)
      callback.throwException(Api.responseToHttpException(processingResult.result()));
    else
      callback.throwException(new Exception("Unexpected exception during processor error handling."));
  }

  static <T extends AbstractFeatureTask> void setAdditionalEventProps(T task, Connector connector, Event event) {
    setAdditionalEventProps(new NotificationContext(task, false), connector, event);
  }

  static void setAdditionalEventProps(NotificationContext nc, Connector connector, Event event) {
    event.setMetadata(nc.jwt.metadata);
    if (connector.trusted) {
      setTrustedParams(nc, connector, event);
    }
  }

  static void setTrustedParams(NotificationContext nc, Connector connector, Event event) {
    if (event == null || nc == null) return;

    if (nc.jwt != null) {
      event.setTid(nc.jwt.tid);
      event.setAid(nc.jwt.aid);
      event.setJwt(nc.jwt.jwt);
    }

    if (connector != null && connector.forwardParamsConfig != null) {
      final ForwardParamsConfig fwd = connector.forwardParamsConfig;
      final TrustedParams trustedParams = new TrustedParams();

      if (fwd.cookies != null && nc.cookies != null) {
        fwd.cookies.forEach(name -> {
          if (nc.cookies.containsKey(name)) {
            trustedParams.putCookie(name, nc.cookies.get(name).getValue());
          }
        });
      }

      if (fwd.headers != null && nc.headers != null) {
        fwd.headers.forEach(name -> {
          if (nc.headers.contains(name)) {
            trustedParams.putHeader(name, nc.headers.get(name));
          }
        });
      }

      if (fwd.queryParams != null && nc.queryParams != null) {
        fwd.queryParams.forEach(name -> {
          if (nc.queryParams.contains(name)) {
            trustedParams.putQueryParam(name, nc.queryParams.get(name));
          }
        });
      }

      if (!trustedParams.isEmpty()) {
        event.setTrustedParams(trustedParams);
      }
    }
  }

  static void setLatestSeenContentVersion(Space space, int version) {
    if ((space.isEnableHistory() || space.isEnableGlobalVersioning()) && version > 0)
      latestSeenContentVersions.compute(space.getId(), (spaceId, currentVersion) -> Math.max(currentVersion != null ? currentVersion : 0,
          version));
  }

  /**
   * Schedules the sending of a {@link ContentModifiedNotification} to the modification SNS topic and all listeners registered for it.
   * If some notification was already scheduled in the last time interval (specified by {@link #CONTENT_MODIFICATION_INTERVAL}), no further
   * notification will be scheduled for the current task.
   * @param task The {@link AbstractFeatureTask} which triggers the notification
   * @param <T>
   */
  private static <T extends AbstractFeatureTask> void scheduleContentModifiedNotification(T task) {
    NotificationContext nc = new NotificationContext(task, false);
    scheduleContentModificationNotificationIfAbsent(nc, contentModificationTimers, CONTENT_MODIFICATION_INTERVAL, false);
    scheduleContentModificationNotificationIfAbsent(nc, contentModificationAdminTimers, CONTENT_MODIFICATION_ADMIN_INTERVAL, true);
  }

  private static void scheduleContentModificationNotificationIfAbsent(NotificationContext nc, ConcurrentHashMap<String, Long> timerMap,
      long interval, boolean adminNotification) {
    if (!timerMap.containsKey(nc.space.getId())) {
      //Schedule a new notification
      long timerId = Service.vertx.setTimer(interval, tId -> {
        timerMap.remove(nc.space.getId());
        ContentModifiedNotification cmn = new ContentModifiedNotification().withSpace(nc.space.getId());
        Integer spaceVersion = latestSeenContentVersions.get(nc.space.getId());
        if (spaceVersion != null) cmn.setSpaceVersion(spaceVersion);
        if (adminNotification) {
          //Send it to the modification SNS topic
          sendSpaceModificationNotification(nc.marker, cmn);
        }
        else {
          //Send the notification to all registered listeners
          notifyConnectors(nc, ConnectorType.LISTENER, ContentModifiedNotification.class.getSimpleName(), cmn, null);
        }
      });
      //Check whether some other thread also just scheduled a new timer
      if (timerMap.putIfAbsent(nc.space.getId(), timerId) != null)
        //Another thread scheduled a new timer in the meantime. Cancelling this one ...
        Service.vertx.cancelTimer(timerId);
    }
  }

  private static <T extends AbstractFeatureTask> void notifyListeners(T task, String eventType, Payload payload) {
    notifyConnectors(new NotificationContext(task, false), ConnectorType.LISTENER, eventType, payload, null);
  }

  private static <T extends AbstractFeatureTask> void notifyProcessors(T task, String eventType, Payload payload,
      Handler<AsyncResult<XyzResponse>> callback) {
    if (payload instanceof BinaryResponse) {
      //No post-processor support for binary responses, skipping post-processor notification
      callback.handle(Future.succeededFuture(new ModifiedResponseResponse().withResponse(payload)));
      return;
    }
    notifyConnectors(new NotificationContext(task, true), ConnectorType.PROCESSOR, eventType, payload, callback);
  }

  private static <T extends AbstractFeatureTask> void notifyConnectors(NotificationContext nc, ConnectorType connectorType, String eventType,
      Payload payload, Handler<AsyncResult<XyzResponse>> callback) {
    //Send the event to all registered & matching listeners / processors
    Map<String, List<ResolvableListenerConnectorRef>> connectorMap = nc.space.getEventTypeConnectorRefsMap(connectorType);
    if (connectorMap != null && !connectorMap.isEmpty()) {
      String phase = payload instanceof Event ? "request" : "response";
      String notificationEventType = eventType + "." + phase;

      if (connectorMap.containsKey(notificationEventType)) {
        List<ResolvableListenerConnectorRef> connectors = connectorMap.get(notificationEventType);
        if (connectorType == ConnectorType.LISTENER) {
          notifyListeners(nc, connectors, notificationEventType, payload);
          return;
        }
        else if (connectorType == ConnectorType.PROCESSOR) {
          notifyProcessors(nc, connectors, notificationEventType, payload, callback);
          return;
        }
        else {
          throw new RuntimeException("Unsupported connector type.");
        }
      }
    }
    if (callback != null) {
      callback.handle(Future.succeededFuture(null));
    }
  }

  private static class RpcContextHolder {
    RpcContext rpcContext;
  }

  private static Future<Space> resolveSpace(final AbstractFeatureTask<?, ?> task) {
    try {
      //FIXME: Can be removed once the Space events are handled by the SpaceTaskHandler (refactoring pending ...)
      if (task.space != null) //If the space is already given we don't need to retrieve it
        return Future.succeededFuture(task.space);

      //Load the space definition.
      return Space.resolveSpace(task.getMarker(), task.getEvent().getSpace())
          .compose(
              space -> {
                if (space != null) {
                  if (space.getExtension() != null && task.getEvent() instanceof ContextAwareEvent && SUPER.equals(((ContextAwareEvent<?>) task.getEvent()).getContext()))
                    return switchToSuperSpace(task, space);
                  task.space = space;
                  //Inject the extension-map
                  return space.resolveCompositeParams(task.getMarker()).compose(resolvedExtensions -> {
                    Map<String, Object> storageParams = new HashMap<>();
                    if (space.getStorage().getParams() != null)
                      storageParams.putAll(space.getStorage().getParams());
                    storageParams.putAll(resolvedExtensions);

                    task.getEvent().setParams(storageParams);
                    return Future.succeededFuture(space);
                  });
                }
                else
                  return Future.succeededFuture();
              },
              t -> {
                logger.warn(task.getMarker(), "Unable to load the space definition for space '{}' {}", task.getEvent().getSpace(), t);
                return Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition", t));
              }
          );
    }
    catch (Exception e) {
      return Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition.", e));
    }
  }

  private static <X extends AbstractFeatureTask> Future<Space> switchToSuperSpace(X task, Space space) {
    //Overwrite the event's space ID to be the ID of the extended (super) space ...
    task.getEvent().setSpace(space.getExtension().getSpaceId());
    //also overwrite the space context to be DEFAULT now ...
    ((ContextAwareEvent<?>) task.getEvent()).setContext(DEFAULT);
    //... and resolve the extended (super) space instead
    return resolveSpace(task);
  }

  private static <X extends AbstractFeatureTask> Future<Space> resolveExtendedSpaces(X task, Space extendingSpace) {
    if (extendingSpace == null)
      return Future.succeededFuture();
    return resolveExtendedSpace(task, extendingSpace.getExtension());
  }

  private static <X extends AbstractFeatureTask> Future<Space> resolveExtendedSpace(X task, Extension spaceExtension) {
    if (spaceExtension == null)
      return Future.succeededFuture();
    return Space.resolveSpace(task.getMarker(), spaceExtension.getSpaceId())
        .compose(
            extendedSpace -> {
              if (task.extendedSpaces == null)
                task.extendedSpaces = new ArrayList();
              task.extendedSpaces.add(extendedSpace);
              return resolveExtendedSpace(task, extendedSpace.getExtension()); //Go to next extension level
            },
            t -> Future.failedFuture(t)
        );
  }

  private static <X extends AbstractFeatureTask> Future<Connector> resolveStorageConnector(final X task) {
    if (task.space == null)
      return Future.failedFuture(new HttpException(NOT_FOUND, "The resource with this ID does not exist."));

    logger.debug(task.getMarker(), "Given space configuration is: {}", task.space);

    final String storageId = task.space.getConnectorId().getId();
    AbstractHttpServerVerticle.addStreamInfo(task.routingContext, "SID", storageId);
    return Space.resolveConnector(task.getMarker(), storageId)
        .compose(
            connector -> {
              task.storageConnector = connector;
              return Future.succeededFuture(connector);
            },
            t -> Future.failedFuture(new InvalidStorageException("Unable to load the definition for this storage."))
        );
  }

  private static <X extends AbstractFeatureTask> Future<Void> resolveListenersAndProcessors(final X task) {
    Promise<Void> p = Promise.promise();
    try {
      //Also resolve all listeners & processors
      CompletableFuture.allOf(
          resolveConnectors(task.getMarker(), task.space, ConnectorType.LISTENER),
          resolveConnectors(task.getMarker(), task.space, ConnectorType.PROCESSOR)
      ).thenRun(() -> {
        //All listener & processor refs have been resolved now
        p.complete();
      });
    }
    catch (Exception e) {
      logger.error(task.getMarker(), "The listeners for this space cannot be initialized", e);
      p.fail(new HttpException(INTERNAL_SERVER_ERROR, "The listeners for this space cannot be initialized"));
    }
    return p.future();
  }

  private static CompletableFuture<Void> resolveConnectors(Marker marker, final Space space, final ConnectorType connectorType) {
    if (space == null || connectorType == null) {
      return CompletableFuture.completedFuture(null);
    }

    final Map<String, List<Space.ListenerConnectorRef>> connectorRefs = space.getConnectorRefsMap(connectorType);

    if (connectorRefs == null || connectorRefs.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<String, List<Space.ListenerConnectorRef>> entry : connectorRefs.entrySet()) {
      if (entry.getValue() != null && !entry.getValue().isEmpty()) {
        ListIterator<Space.ListenerConnectorRef> i = entry.getValue().listIterator();
        while (i.hasNext()) {
          Space.ListenerConnectorRef cR = i.next();
          CompletableFuture<Void> f = new CompletableFuture<>();
          Space.resolveConnector(marker, entry.getKey(), arListener -> {
            final Connector c = arListener.result();
            ResolvableListenerConnectorRef rCR = new ResolvableListenerConnectorRef();
            rCR.setId(entry.getKey());
            rCR.setParams(cR.getParams());
            rCR.setOrder(cR.getOrder());
            rCR.setEventTypes(cR.getEventTypes());
            rCR.resolvedConnector = c;
            //If no event types have been defined in the connectorRef we use the defaultEventTypes from the resolved connector config
            if ((rCR.getEventTypes() == null || rCR.getEventTypes().isEmpty()) && c.defaultEventTypes != null && !c.defaultEventTypes
                .isEmpty()) {
              rCR.setEventTypes(new ArrayList<>(c.defaultEventTypes));
            }
            // replace ListenerConnectorRef with ResolvableListenerConnectorRef
            i.set(rCR);
            f.complete(null);
          });
          futures.add(f);
        }
      }
    }

    //When all listeners have been resolved we can complete the returned future.
    CompletableFuture
        .allOf(futures.toArray(new CompletableFuture[0]))
        .thenRun(() -> future.complete(null));

    return future;
  }

  static <X extends AbstractFeatureTask> void registerRequestMemory(final X task, final ICallback<X> callback) {
    try {
      registerRequestMemory(task.storageConnector.id, task.requestBodySize);
    }
    finally {
      callback.success(task);
    }
  }

  private static void registerRequestMemory(String storageId, int byteSize) {
    if (byteSize <= 0) return;
    LongAdder usedMemory = inflightRequestMemory.get(storageId);
    if (usedMemory == null)
      inflightRequestMemory.put(storageId, usedMemory = new LongAdder());

    usedMemory.add(byteSize);
    globalInflightRequestMemory.add(byteSize);
  }

  public static void deregisterRequestMemory(String storageId, int byteSize) {
    if (byteSize <= 0) return;

    inflightRequestMemory.get(storageId).add(-byteSize);
    globalInflightRequestMemory.add(-byteSize);
  }

  static <X extends AbstractFeatureTask> void throttle(final X task, final ICallback<X> callback) {
    /*
    Connector storage = task.storage;
    final long GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE = (long) Service.configuration.GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE_MB * 1024 * 1024;
    float usedMemoryPercent = Service.getUsedMemoryPercent() / 100f;
    try {
      //When ZGC is in use, only throttle requests if the service memory filled up over the specified service memory threshold
      if (Service.IS_USING_ZGC) {
        if (usedMemoryPercent > Service.configuration.SERVICE_MEMORY_HIGH_UTILIZATION_THRESHOLD) {
          AbstractHttpServerVerticle.addStreamInfo(task.context, "THR", "M"); //Reason for throttling is memory
          throw new HttpException(TOO_MANY_REQUESTS, "Too many requests for the service node.");
        }
      }
      //For other GCs, only throttle requests if the request memory filled up over the specified request memory threshold
      else if (globalInflightRequestMemory.sum() >
          GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE * Service.configuration.GLOBAL_INFLIGHT_REQUEST_MEMORY_HIGH_UTILIZATION_THRESHOLD) {
        LongAdder storageInflightRequestMemory = inflightRequestMemory.get(storage.id);
        long storageInflightRequestMemorySum = 0;
        if (storageInflightRequestMemory == null || (storageInflightRequestMemorySum = storageInflightRequestMemory.sum()) == 0) {
          callback.call(task); //Nothing to throttle for that storage
          return;
        }

        RpcClient rpcClient = getRpcClient(storage);
        if (storageInflightRequestMemorySum > rpcClient.getFunctionClient().getPriority() * GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE) {
          AbstractHttpServerVerticle.addStreamInfo(task.context, "THR", "M"); //Reason for throttling is memory
          throw new HttpException(TOO_MANY_REQUESTS, "Too many requests for the storage.");
        }
      }
    }
    catch (HttpException e) {
      logger.warn(task.getMarker(), e.getMessage(), e);
      callback.exception(e);
      return;
    }
    */
    callback.success(task);
  }

  public static void prepareModifyFeatureOp(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) {
    if (task.modifyOp != null) {
      callback.success(task);
      return;
    }

    try {
      task.modifyOp = new ModifyFeatureOp(getFeatureModifications(task), task.ifNotExists, task.ifExists, task.transactional, task.conflictResolution, task.space.isAllowFeatureCreationWithUUID());
      callback.success(task);
    } catch (HttpException e) {
      logger.warn(task.getMarker(), e.getMessage(), e);
      callback.throwException(e);
    } catch (Exception e) {
      logger.warn(task.getMarker(), e.getMessage(), e);
      callback.throwException(new HttpException(BAD_REQUEST, "Unable to process the request input."));
    }
  }

  private static List<Map<String, Object>> getFeatureModifications(ConditionalModifyFeaturesTask task) throws Exception {
    if (APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST.equals(task.routingContext.parsedHeaders().contentType().rawValue())) {
      return getObjectsAsList(task.routingContext);
    }

    List<Map<String, Object>> features = getObjectsAsList(task.routingContext);
    if (task.responseType == ApiResponseType.FEATURE) { //TODO: Replace that evil hack
      features.get(0).put("id", task.routingContext.pathParam(ApiParam.Path.FEATURE_ID));
    }

    Map<String, Object> featureCollection = Collections.singletonMap("features", features);
    return Collections.singletonList(Collections.singletonMap("featureData", featureCollection));
  }

  /**
   * Parses the body of the request as a FeatureCollection, Feature or a FeatureModificationList object and returns the features as a list.
   */
  private static List<Map<String, Object>> getObjectsAsList(final RoutingContext context) throws HttpException {
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
    finally {
      context.setBody(null);
      context.data().remove("requestParameters");
      context.data().remove("parsedParameters");
    }
  }

  private static List<Map<String, Object>> getJsonObjects(JsonObject json, RoutingContext context) throws HttpException {
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

  static void preprocessConditionalOp(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) throws Exception {
    try {
      task.getEvent().setEnableUUID(task.space.isEnableUUID());
      // Ensure that the ID is a string or null and check for duplicate IDs
      Map<String, Boolean> ids = new HashMap<>();
      for (Entry<Feature> entry : task.modifyOp.entries) {
        final Object objId = entry.input.get(ID);
        String id = (objId instanceof String || objId instanceof Number) ? String.valueOf(objId) : null;
        if (task.prefixId != null) { // Generate IDs here, if a prefixId is required. Add the prefix otherwise.
          id = task.prefixId + ((id == null) ? RandomStringUtils.randomAlphanumeric(16) : id);
        }
        entry.input.put(ID, id);

        if (id != null) {
          // Minimum length of id should be 1
          if (id.length() < 1) {
            logger.info(task.getMarker(), "Minimum length of object id should be 1.");
            callback.throwException(new HttpException(BAD_REQUEST, "Minimum length of object id should be 1."));
            return;
          }
          // Test for duplicate IDs
          if (ids.containsKey(id)) {
            logger.info(task.getMarker(), "Objects with the same ID {} are included in the request.", id);
            callback.throwException(new HttpException(BAD_REQUEST, "Objects with the same ID " + id + " is included in the request."));
            return;
          }
          ids.put(id, true);
        }

        entry.input.putIfAbsent(TYPE, "Feature");

        // bbox is a dynamically calculated property
        entry.input.remove(BBOX);

        // Add the XYZ namespace if it is not set yet.
        entry.input.putIfAbsent(PROPERTIES, new HashMap<String, Object>());
        @SuppressWarnings("unchecked") final Map<String, Object> properties = (Map<String, Object>) entry.input.get("properties");
        properties.putIfAbsent(XyzNamespace.XYZ_NAMESPACE, new HashMap<String, Object>());
      }
    } catch (Exception e) {
      logger.warn(task.getMarker(), e.getMessage(), e);
      callback.throwException(new HttpException(BAD_REQUEST, "Unable to process the request input."));
      return;
    }
    callback.success(task);
  }

  static void monitorFeatureRequest(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) {
    try {
      if (Service.configuration.MONITOR_FEATURES_WITH_UUID
          && task.space.isEnableHistory()
          && task.modifyOp.isWrite()
          && !task.modifyOp.entries.isEmpty()) {

        FeatureEntry entry = task.modifyOp.entries.get(0);

        // monitor only the inputs which the first feature contains uuid
        if (!Strings.isNullOrEmpty(entry.inputUUID)) {
          boolean containsInput = entry.input != null;
          boolean containsHead = entry.head != null;
          boolean containsBase = entry.base != null;

          String spaceId = task.space.getId();
          String owner = task.space.getOwner();
          String featureId = containsInput ? (String) entry.input.get("id") : null;
          String uuid = entry.inputUUID;

          String ifExists = entry.ifExists.name();
          String ifNotExists = entry.ifNotExists.name();

          logger.warn(task.getMarker(), "Monitoring WRITE feature on space: " + spaceId + "; owner: " + owner +
              "; featureId: " + featureId + "; containsInput: " + containsInput + "; containsHead: " + containsHead + "; containsBase: "
              + containsBase + "; uuid: " + uuid + "; ifExists: " + ifExists + "; ifNotExists: " + ifNotExists);
        }
      }
    }
    catch (Exception e) {
      logger.warn(task.getMarker(), "Unable to monitor feature request", e);
    }

    callback.success(task);
  }

  static void processConditionalOp(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) throws Exception {
    try {
      task.modifyOp.process();
      final List<Feature> insert = new ArrayList<>();
      final List<Feature> update = new ArrayList<>();
      final Map<String, String> delete = new HashMap<>();
      List<FeatureCollection.ModificationFailure> fails = new ArrayList<>();

      Iterator<FeatureEntry> it = task.modifyOp.entries.iterator();
      int i=-1;
      while( it.hasNext() ){
        FeatureEntry entry = it.next();
        i++;

        if(entry.exception != null){
          ModificationFailure failure = new ModificationFailure()
              .withMessage(entry.exception.getMessage())
              .withPosition((long) i);
          if (entry.input.get("id") instanceof String) {
            failure.setId((String) entry.input.get("id"));
          }
          fails.add(failure);
          continue;
        }

        if (!entry.isModified) {
          task.hasNonModified = true;
          /** Entry does not exist - remove it to prevent null references */
          if(entry.head == null && entry.base == null)
            it.remove();
          continue;
        }

        final Feature result = entry.result;

        // Insert or update
        if (result != null) {

          try {
            result.validateGeometry();
          } catch (InvalidGeometryException e) {
            logger.info(task.getMarker(), "Invalid geometry found in feature: {}", result, e);
            throw new HttpException(BAD_REQUEST, e.getMessage() + ". Feature: \n" + Json.encode(entry.input));
          }

          boolean isInsert = entry.head == null;
          processNamespace(task, entry, result.useProperties().getXyzNamespace(), isInsert, i);
          (isInsert ? insert : update).add(result);
        }

        // DELETE
        else if (entry.head != null) {
          delete.put(entry.head.getId(), entry.inputUUID);
        }
      }

      task.getEvent().setInsertFeatures(insert);
      task.getEvent().setUpdateFeatures(update);
      task.getEvent().setDeleteFeatures(delete);
      task.getEvent().setFailed(fails);

      // In case nothing was changed, set the response directly to skip calling the storage connector.
      if (insert.size() == 0 && update.size() == 0 && delete.size() == 0) {
        FeatureCollection fc = new FeatureCollection();
        if( task.hasNonModified ){
          task.modifyOp.entries.stream().filter(e -> !e.isModified).forEach(e -> {
            try {
              if(e.result != null)
                fc.getFeatures().add(e.result);
            } catch (JsonProcessingException ignored) {}
          });
        }
        if(fails.size() > 0)
          fc.setFailed(fails);
        task.setResponse(fc);
      }

      callback.success(task);
    } catch (ModifyOpError e) {
      logger.info(task.getMarker(), "ConditionalOperationError: {}", e.getMessage(), e);
      throw new HttpException(CONFLICT, e.getMessage());
    }
  }

  static void processNamespace(ConditionalModifyFeaturesTask task, FeatureEntry entry, XyzNamespace nsXyz, boolean isInsert, long inputPosition) {
    // Set the space ID
    boolean spaceIsOptional = Service.configuration.containsFeatureNamespaceOptionalField("space");
    nsXyz.setSpace(spaceIsOptional ? null : task.space.getId());

    // Normalize the tags
    XyzNamespace.normalizeTags(nsXyz.getTags());
    if (nsXyz.getTags() == null) {
      nsXyz.setTags(new ArrayList<>());
    }

    // Optionally set tags
    boolean tagsIsOptional = Service.configuration.containsFeatureNamespaceOptionalField("tags");
    if (tagsIsOptional && nsXyz.getTags().isEmpty()) {
      nsXyz.setTags(null);
    }

    // Timestamp fields
    long now = Core.currentTimeMillis();
    nsXyz.setCreatedAt(isInsert ? now : entry.head.useProperties().getXyzNamespace().getCreatedAt());
    nsXyz.setUpdatedAt(now);

    // UUID fields
    if (task.space.isEnableUUID()) {
      nsXyz.setUuid(UUID.randomUUID().toString());

      if (!isInsert) {
        nsXyz.setPuuid(entry.head.getProperties().getXyzNamespace().getUuid());
        // If the user was updating an older version, set it under the merge uuid
        if (!entry.base.equals(entry.head)) {
          nsXyz.setMuuid(entry.base.getProperties().getXyzNamespace().getUuid());
        }
      }
    }
  }

  static void updateTags(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) {
    if ((task.addTags == null || task.addTags.size() == 0) && (task.removeTags == null || task.removeTags.size() == 0)) {
      callback.success(task);
      return;
    }

    for (Entry<Feature> entry : task.modifyOp.entries) {
      // For existing objects: if the input does not contain the tags, copy them from the edited state.
      final Map<String, Object> nsXyz = new JsonObject(entry.input).getJsonObject("properties").getJsonObject(XyzNamespace.XYZ_NAMESPACE)
          .getMap();
      if (!(nsXyz.get("tags") instanceof List)) {
        ArrayList<String> inputTags = new ArrayList<>();
        if (entry.base != null && entry.base.getProperties().getXyzNamespace().getTags() != null) {
          inputTags.addAll(entry.base.getProperties().getXyzNamespace().getTags());
        }
        nsXyz.put("tags", inputTags);
      }
      final List<String> tags = (List<String>) nsXyz.get("tags");
      if (task.addTags != null) {
        task.addTags.forEach(tag -> {
          if (!tags.contains(tag)) {
            tags.add(tag);
          }
        });
      }
      if (task.removeTags != null) {
        task.removeTags.forEach(tags::remove);
      }
    }

    callback.success(task);
  }

  static <X extends AbstractFeatureTask<?, X>> void enforceUsageQuotas(X task, ICallback<X> callback) {
    final long maxFeaturesPerSpace = task.getJwt().limits != null ? task.getJwt().limits.maxFeaturesPerSpace : -1;
    if (maxFeaturesPerSpace <= 0) {
      callback.success(task);
      return;
    }

    Long cachedCount = countCache.get(task.space.getId());
    if (cachedCount != null) {
      checkFeaturesPerSpaceQuota(task, callback, maxFeaturesPerSpace, cachedCount);
      return;
    }

    getCountForSpace(task, countResult -> {
      if (countResult.failed()) {
        callback.throwException(new Exception(countResult.cause()));
        return;
      }
      // Check the quota
      Long count = countResult.result();
      long ttl = (maxFeaturesPerSpace - count > 100_000) ? 60 : 10;
      countCache.put(task.space.getId(), count, ttl, TimeUnit.SECONDS);
      checkFeaturesPerSpaceQuota(task, callback, maxFeaturesPerSpace, count);
    });
  }

  static <X extends AbstractFeatureTask> void injectSpaceParams(final X task, final ICallback<X> callback) {
    try {
      if(task.getEvent() instanceof ModifyFeaturesEvent) {
         ((ModifyFeaturesEvent) task.getEvent()).setMaxVersionCount(task.space.getMaxVersionCount());
        ((ModifyFeaturesEvent) task.getEvent()).setEnableGlobalVersioning(task.space.isEnableGlobalVersioning());
        ((ModifyFeaturesEvent) task.getEvent()).setEnableHistory(task.space.isEnableHistory());
      }
      callback.success(task);
    } catch (Exception e) {
      callback.throwException(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition.", e));
    }
  }

  private static <X extends AbstractFeatureTask<?, X>> void checkFeaturesPerSpaceQuota(X task, ICallback<X> callback,
      long maxFeaturesPerSpace, Long count) {
    try {
      ModifyFeaturesEvent modifyEvent = (ModifyFeaturesEvent) task.getEvent();
      if (modifyEvent != null) {
        final List<Feature> insertFeaturesList = modifyEvent.getInsertFeatures();
        final int insertFeaturesSize = insertFeaturesList == null ? 0 : insertFeaturesList.size();
        final Map<String, String> deleteFeaturesMap = modifyEvent.getDeleteFeatures();
        final int deleteFeaturesSize = deleteFeaturesMap == null ? 0 : deleteFeaturesMap.size();
        final int featuresDelta = insertFeaturesSize - deleteFeaturesSize;
        final String spaceId = modifyEvent.getSpaceId();
        if (featuresDelta > 0 && count + featuresDelta > maxFeaturesPerSpace) {
          callback.throwException(new HttpException(FORBIDDEN,
              "The maximum number of " + maxFeaturesPerSpace + " features for the resource \"" + spaceId + "\" was reached. " +
              "The resource contains " + count + " features and cannot store " + featuresDelta + " more features."));
          return;
        }
      }
      callback.success(task);
    } catch (Exception e) {
      callback.throwException(e);
    }
  }

  private static <X extends AbstractFeatureTask<?, X>>void getCountForSpace(X task, Handler<AsyncResult<Long>> handler) {
    final GetStatisticsEvent countEvent = new GetStatisticsEvent();
    countEvent.setSpaceId(task.getEvent().getSpace());
    countEvent.setParams(task.getEvent().getParams());

    try {
      getRpcClient(task.storageConnector)
          .execute(task.getMarker(), countEvent, (AsyncResult<XyzResponse> eventHandler) -> {
            if (eventHandler.failed()) {
              handler.handle(Future.failedFuture((eventHandler.cause())));
              return;
            }
            Long count;
            final XyzResponse response = eventHandler.result();
            if (response instanceof StatisticsResponse)
              count = ((StatisticsResponse) response).getCount().getValue();
            else {
              handler.handle(Future.failedFuture(Api.responseToHttpException(response)));
              return;
            }
            handler.handle(Future.succeededFuture(count));
          });
    }
    catch (Exception e) {
      handler.handle(Future.failedFuture((e)));
    }
  }

  static void transformResponse(TileQuery task, ICallback<TileQuery> callback) {
    if (task.responseType != MVT
        && task.responseType != MVT_FLATTENED
        || !(task.getResponse() instanceof FeatureCollection)
        //The mvt transformation is not executed, if the source feature collection is the same.
        || task.etagMatches()) {
      callback.success(task);
      return;
    }

    TransformationContext tc = task.transformationContext;
    try {
      byte[] mvt;
      if (MVT == task.responseType) {
        mvt = new MapBoxVectorTileBuilder()
            .build(WebMercatorTile.forWeb(tc.level, tc.x, tc.y), tc.margin, task.space.getId(),
                ((FeatureCollection) task.getResponse()).getFeatures());
      }
      else {
        mvt = new MapBoxVectorTileFlattenedBuilder()
            .build(WebMercatorTile.forWeb(tc.level, tc.x, tc.y), tc.margin, task.space.getId(),
                ((FeatureCollection) task.getResponse()).getFeatures());
      }
      task.setResponse(new BinaryResponse()
              .withMimeType(APPLICATION_VND_MAPBOX_VECTOR_TILE)
              .withBytes(mvt)
              .withEtag(task.getResponse().getEtag()));
      callback.success(task);
    }
    catch (Exception e) {
      logger.warn(task.getMarker(), "Exception while transforming the response.", e);
      callback.throwException(new HttpException(INTERNAL_SERVER_ERROR, "Error while transforming the response."));
      return;
    }
  }

  public static <X extends AbstractFeatureTask<?, X>> void validate(X task, ICallback<X> callback) {
    if (task instanceof ReadQuery && ((ReadQuery) task).hasPropertyQuery()
        && !task.storageConnector.capabilities.propertySearch) {
      callback.throwException(new HttpException(BAD_REQUEST, "Property search queries are not supported by storage connector "
          + "\"" + task.storageConnector.id + "\"."));
      return;
    }

    if (task.getEvent() instanceof GetFeaturesByBBoxEvent) {
      GetFeaturesByBBoxEvent event = (GetFeaturesByBBoxEvent) task.getEvent();
      String clusteringType = event.getClusteringType();
      if (clusteringType != null && (task.storageConnector.capabilities.clusteringTypes == null
          || !task.storageConnector.capabilities.clusteringTypes.contains(clusteringType))) {
        callback.throwException(new HttpException(BAD_REQUEST, "Clustering of type \"" + clusteringType + "\" is not"
            + "supported by storage connector \"" + task.storageConnector.id + "\"."));
      }
    }

    if (task.getEvent() instanceof IterateHistoryEvent) {
      if (!task.space.isEnableGlobalVersioning()) {
        callback.throwException(new HttpException(BAD_REQUEST, "This space ["+task.space.getId()+"] does not support version queries."));
      }
      int startVersion = ((IterateHistoryEvent) task.getEvent()).getStartVersion();
      int endVersion = ((IterateHistoryEvent) task.getEvent()).getEndVersion();
      if(startVersion != 0 && startVersion < 1)
        callback.throwException(new HttpException(BAD_REQUEST, "startVersion is out or range [1-n]."));
      if(startVersion != 0 && endVersion != 0 && endVersion < startVersion)
        callback.throwException(new HttpException(BAD_REQUEST, "endVersion has to be smaller than startVersion."));
    }

    if (task.getEvent() instanceof IterateFeaturesEvent) {
      if (!task.space.isEnableGlobalVersioning() && ((IterateFeaturesEvent) task.getEvent()).getV() != null) {
        callback.throwException(new HttpException(BAD_REQUEST, "This space ["+task.space.getId()+"] does not support version queries."));
      }
    }

    if (task.getEvent() instanceof GetHistoryStatisticsEvent) {
      if (!task.space.isEnableGlobalVersioning()) {
        callback.throwException(new HttpException(BAD_REQUEST, "This space [" + task.space.getId() + "] does not support history."));
      }
    }

    callback.success(task);
  }

  static <X extends AbstractFeatureTask<?, X>> void convertResponse(X task, ICallback<X> callback) throws JsonProcessingException {
    if (task instanceof GetStatisticsTask) {
      if (task.getResponse() instanceof StatisticsResponse) {
        //Ensure the StatisticsResponse is correctly set-up
        StatisticsResponse response = (StatisticsResponse) task.getResponse();
        defineGlobalSearchableField(response, task);
      }
    } else if (task instanceof GetFeaturesByIdTask) {
      //Ensure to return a FeatureCollection when there are multiple features in the response (could happen e.g. for a virtual-space)
      if (task.getResponse() instanceof FeatureCollection && ((FeatureCollection) task.getResponse()).getFeatures() != null
          && ((FeatureCollection) task.getResponse()).getFeatures().size() > 1) {
        task.responseType = ApiResponseType.FEATURE_COLLECTION;
      }
    }
    callback.success(task);
  }

  private static void defineGlobalSearchableField(StatisticsResponse response, AbstractFeatureTask task) {
    if (!task.storageConnector.capabilities.propertySearch) {
      response.getProperties().setSearchable(Searchable.NONE);
    }

    // updates the searchable flag for each property in case of ALL or NONE
    final Searchable searchable = response.getProperties().getSearchable();
    if (searchable != null && searchable != Searchable.PARTIAL) {
      if (response.getProperties().getValue() != null) {
        response.getProperties().getValue().forEach(c -> c.setSearchable(searchable == Searchable.ALL));
      }
    }
  }

  static <X extends AbstractFeatureTask<?, X>> void checkPreconditions(X task, ICallback<X> callback) throws HttpException {
    if (task.space.isReadOnly() && (task instanceof ConditionalModifyFeaturesTask || task instanceof DeleteFeaturesByTagTask)) {
      throw new HttpException(METHOD_NOT_ALLOWED,
          "The method is not allowed, because the resource \"" + task.space.getId() + "\" is marked as read-only. Update the resource definition to enable editing of features.");
    }
    if (task.space.isEnableGlobalVersioning() && task.getEvent() instanceof  ModifyFeaturesEvent && ((ModifyFeaturesEvent) task.getEvent()).getTransaction() == false) {
      throw new HttpException(METHOD_NOT_ALLOWED,
           "The method is not allowed, because the resource \"" + task.space.getId() + "\" has enabledGlobalVersioning. Due to that, stream writing is not allowed.");
    }
    callback.success(task);
  }

  private static void sendSpaceModificationNotification(Marker marker, Event event) {
    if (!(event instanceof ModifySpaceEvent || event instanceof ContentModifiedNotification))
      throw new IllegalArgumentException("Invalid event type was given to send as space modification notification.");
    String spaceId = event.getSpaceId();
    String eventType = event.getClass().getSimpleName();
    try {
      if (Service.configuration.MSE_NOTIFICATION_TOPIC != null) {
        PublishRequest req = PublishRequest.builder()
            .topicArn(Service.configuration.MSE_NOTIFICATION_TOPIC)
            .message(event.serialize())
            .messageGroupId(event.getSpaceId())
            .build();
        getSnsClient()
            .publish(req)
            .whenComplete((result, error) -> {
              if (error != null)
                logger.error(marker,"Error sending MSE notification of type " + eventType + " for space " + spaceId, error);
              else
                logger.info(marker, "MSE notification of type " + eventType + " for space " + spaceId + " was sent.");
            });
      }
    }
    catch (Exception e) {
      logger.error(marker,"Unable to send MSE notification of type " + eventType + " for space " + spaceId, e);
    }
  }

  static void verifyResourceExists(ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback) {
    if (task.requireResourceExists && task.modifyOp.entries.get(0).head == null) {
      callback.throwException(new HttpException(NOT_FOUND, "The requested resource does not exist."));
    }
    else {
      callback.success(task);
    }
  }

  static void loadObjects(final ConditionalModifyFeaturesTask task, final ICallback<ConditionalModifyFeaturesTask> callback) {
    final LoadFeaturesEvent event = toLoadFeaturesEvent(task);
    if (event == null) {
      callback.success(task);
      return;
    }
    FeatureTaskHandler.setAdditionalEventProps(task, task.storageConnector, event);
    try {
      final long storageRequestStart = Core.currentTimeMillis();
      EventResponseContext responseContext = new EventResponseContext(event);
      responseContext.rpcContext = getRpcClient(task.storageConnector).execute(task.getMarker(), event, r -> {
        if (task.getState().isFinal()) return;
        addConnectorPerformanceInfo(task, Core.currentTimeMillis() - storageRequestStart, responseContext.rpcContext, "LF");
        processLoadEvent(task, callback, r);
      });
    }
    catch (Exception e) {
      logger.warn(task.getMarker(), "Error trying to process LoadFeaturesEvent.", e);
      callback.throwException(e);
    }
  }

  private static LoadFeaturesEvent toLoadFeaturesEvent(final ConditionalModifyFeaturesTask task) {
    if (task.loadFeaturesEvent != null)
      return task.loadFeaturesEvent;

    if (task.modifyOp.entries.size() == 0)
      return null;

    final boolean useRevision = task.space.getRevisionsToKeep() > 0;
    final HashMap<String, String> idsMap = new HashMap<>();
    for (FeatureEntry entry : task.modifyOp.entries) {
      if (entry.input.get("id") instanceof String) {
        idsMap.put((String) entry.input.get("id"), useRevision ? String.valueOf(entry.inputRevision) : entry.inputUUID);
      }
    }
    if (idsMap.size() == 0) {
      return null;
    }

    final LoadFeaturesEvent event = new LoadFeaturesEvent()
        .ensureStreamId(task.getMarker().getName())
        .withSpace(task.space.getId())
        .withParams(task.getEvent().getParams())
        .withContext(task.getEvent().getContext())
        .withEnableGlobalVersioning(task.space.isEnableGlobalVersioning())
        .withEnableHistory(task.space.isEnableHistory())
        .withIdsMap(idsMap);

    task.loadFeaturesEvent = event;
    return event;
  }

  private static void processLoadEvent(final ConditionalModifyFeaturesTask task, ICallback<ConditionalModifyFeaturesTask> callback, AsyncResult<XyzResponse> r) {
    final Map<String, String> idsMap = task.loadFeaturesEvent.getIdsMap();
    if (r.failed()) {
      callback.throwException(r.cause());
      return;
    }

    try {
      final XyzResponse response = r.result();
      if (!(response instanceof FeatureCollection)) {
        callback.throwException(Api.responseToHttpException(response));
        return;
      }
      final FeatureCollection collection = (FeatureCollection) response;
      final List<Feature> features = collection.getFeatures();

      //For each input feature there could be 0, 1(head state) or 2 (head state and base state) features in the response
      if (features == null) {
        callback.success(task);
        return;
      }

      for (final Feature feature : features) {
        //The uuid the client has requested.
        final String requestedUuid = idsMap.get(feature.getId());

        int position = getPositionForId(task, feature.getId());
        if (position == -1) { // There is no object with this ID in the input states
          continue;
        }

        if (feature.getProperties() == null || feature.getProperties().getXyzNamespace() == null) {
          throw new IllegalStateException("Received a feature with missing space namespace properties for object '" + feature.getId() + "'");
        }

        String uuid = feature.getProperties().getXyzNamespace().getUuid();

        //Set the head state( i.e. the latest version in the database )
        if (task.modifyOp.entries.get(position).head == null || uuid != null && !uuid.equals(requestedUuid))
          task.modifyOp.entries.get(position).head = feature;

        //Set the base state( i.e. the original version that the user was editing )
        //Note: The base state must not be empty. If the connector doesn't support history and doesn't return the base state, use the
        //head state instead.
        if (task.modifyOp.entries.get(position).base == null || uuid != null && uuid.equals(requestedUuid))
          task.modifyOp.entries.get(position).base = feature;
      }

      callback.success(task);
    }
    catch (Exception e) {
      callback.throwException(e);
    }
  }

  private static int getPositionForId(final ConditionalModifyFeaturesTask task, String id) {
    if (id == null) {
      return -1;
    }

    if (task.positionById == null) {
      task.positionById = new HashMap<>();
      for (int i = 0; i < task.modifyOp.entries.size(); i++) {
        final Map<String, Object> input = task.modifyOp.entries.get(i).input;
        if (input != null && input.get("id") instanceof String) {
          task.positionById.put(input.get("id"), i);
        }
      }
    }

    return task.positionById.get(id) == null ? -1 : task.positionById.get(id);
  }

  static void extractUnmodifiedFeatures(final ConditionalModifyFeaturesTask task, final ICallback<ConditionalModifyFeaturesTask> callback) {
    if (task.modifyOp != null && task.modifyOp.entries != null)
      task.unmodifiedFeatures = task.modifyOp.entries.stream().filter(e -> !e.isModified).map(fe -> fe.result).collect(Collectors.toList());
    callback.success(task);
  }

  private static SnsAsyncClient getSnsClient() {
    if (snsClient == null)
      snsClient = SnsAsyncClient.builder().build();

    return snsClient;
  }

  public static long getGlobalInflightRequestMemory() {
    return globalInflightRequestMemory.sum();
  }

  public static class InvalidStorageException extends Exception {

    InvalidStorageException(String msg) {
      super(msg);
    }
  }

  /**
   * Extracts specific information out of the event which should survive in memory until the response phase.
   */
  private static class EventResponseContext {

    List<FeatureCollection.ModificationFailure> failedModifications;
    Class<? extends Event> eventType;
    RpcContext rpcContext;

    EventResponseContext(Event event) {
      eventType = event.getClass();
      if (event instanceof ModifyFeaturesEvent) {
        failedModifications = ((ModifyFeaturesEvent) event).getFailed();
      }
    }

    <T extends AbstractFeatureTask, R extends XyzResponse> void enrichResponse(T task, R response) {
      if (task instanceof ConditionalModifyFeaturesTask && response instanceof FeatureCollection && ((ConditionalModifyFeaturesTask) task).hasNonModified
          && ((ConditionalModifyFeaturesTask) task).unmodifiedFeatures != null) {
        try {
          ((FeatureCollection) response).getFeatures().addAll(((ConditionalModifyFeaturesTask) task).unmodifiedFeatures);
        }
        catch (JsonProcessingException ignored) {}
      }
      if (eventType.isAssignableFrom(ModifyFeaturesEvent.class) && failedModifications != null && !failedModifications.isEmpty()) {
        //Copy over the failed modifications information to the response
        List<FeatureCollection.ModificationFailure> failed = ((FeatureCollection) response).getFailed();
        if (failed == null) {
          ((FeatureCollection) response).setFailed(failedModifications);
        } else {
          failed.addAll(failedModifications);
        }
      }
    }
  }

  static class NotificationContext {
    private Marker marker;
    private Space space;
    private JWTPayload jwt;
    private AbstractFeatureTask task;
    private Map<String, Cookie> cookies;
    private MultiMap headers;
    private MultiMap queryParams;

    public NotificationContext(AbstractFeatureTask task, boolean keepTask) {
      marker = task.getMarker();
      space = task.space;
      jwt = task.getJwt();
      cookies = task.routingContext.request().cookieMap();
      headers = task.routingContext.request().headers();
      queryParams = task.routingContext.request().params();
      if (keepTask)
        this.task = task;
    }
  }

  private static boolean isRevisionValid(String revision) {
    try {
      return "*".equals(revision) || Integer.parseInt(revision) > 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
