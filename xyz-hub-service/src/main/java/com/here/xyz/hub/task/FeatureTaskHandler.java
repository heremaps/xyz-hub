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

import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_INFO;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.BBOX;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.ID;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.PROPERTIES;
import static com.here.xyz.hub.task.FeatureTask.FeatureKey.TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.EventNotification;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.connectors.models.BinaryResponse;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.connectors.models.Space.CacheProfile;
import com.here.xyz.hub.connectors.models.Space.ConnectorType;
import com.here.xyz.hub.connectors.models.Space.ResolvableListenerConnectorRef;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.FeatureTask.ConditionalOperation;
import com.here.xyz.hub.task.FeatureTask.DeleteOperation;
import com.here.xyz.hub.task.FeatureTask.ReadQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery.TransformationContext;
import com.here.xyz.hub.task.ModifyFeatureOp.FeatureEntry;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.task.ModifyOp.ModifyOpError;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.hub.util.geo.MapBoxVectorTileBuilder;
import com.here.xyz.hub.util.geo.MapBoxVectorTileFlattenedBuilder;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.CountResponse;
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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class FeatureTaskHandler {

  private static final Logger logger = LogManager.getLogger();

  private static final ExpiringMap<String, Long> countCache = ExpiringMap.builder()
      .maxSize(1024)
      .variableExpiration()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .build();
  private static final byte JSON_VALUE = 1;
  private static final byte BINARY_VALUE = 2;

  /**
   * Sends the event to the connector client and write the response as the responseCollection of the task.
   *
   * @param task the FeatureTask instance
   * @param callback the callback handler
   * @param <T> the type of the FeatureTask
   */
  public static <T extends FeatureTask> void invoke(T task, Callback<T> callback) {
    /**
     * NOTE: The event may only be consumed once. Once it was consumed it should only be referenced in the request-phase. Referencing it in the
     *     response-phase will keep the whole event-data in the memory and could cause many major GCs to because of large request-payloads.
     *
     * @see Task#consumeEvent()
     */
    Event event = task.consumeEvent();
    /*
    In case there is already, nothing has to be done here (happens if the response was set by an earlier process in the task pipeline
    e.g. when having a cache hit)
     */
    if (task.getResponse() != null) {
      callback.call(task);
      return;
    }

    if (!task.storage.active) {
      if (event instanceof ModifySpaceEvent && ((ModifySpaceEvent) event).getOperation() == ModifySpaceEvent.Operation.DELETE) {
        /*
        If connector is inactive we allow space deletions. In this case only the space configuration gets deleted. The
        deactivated connector does not get invoked so the dataset behind stays untouched.
        */
        task.setResponse(new SuccessResponse().withStatus("OK"));
        callback.call(task);
      }
      else {
        //Abort further processing - do not: notifyProcessors, notifyListeners, invoke connector
        callback.exception(new HttpException(BAD_REQUEST, "Related connector is not active: " + task.storage.id));
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
        setAdditionalEventProps(task, task.storage, eventToExecute);
        final long storageRequestStart = Service.currentTimeMillis();
        responseContext.rpcContext = getRpcClient(task.storage).execute(task.getMarker(), eventToExecute, storageResult -> {
          if (task.getState().isFinal()) return;
          addStoragePerformanceInfo(task, Service.currentTimeMillis() - storageRequestStart, responseContext.rpcContext);
          if (storageResult.failed()) {
            handleFailure(task.getMarker(), storageResult.cause(), callback);
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
            callback.call(task);
            //Send the event's (post-processed) response to potentially registered response-listeners
            notifyListeners(task, eventType, responseToSend);
          });
        });
        addStreamInfo(task, "SReqSize=" + responseContext.rpcContext.getRequestSize() + ";");
        task.addCancellingHandler(unused -> responseContext.rpcContext.cancelRequest());
      }
      catch (IllegalStateException e) {
        cancelRPC(responseContext.rpcContext);
        logger.error(task.getMarker(), e.getMessage(), e);
        callback.exception(new HttpException(BAD_REQUEST, e.getMessage(), e));
        return;
      }
      catch (HttpException e) {
        cancelRPC(responseContext.rpcContext);
        logger.error(task.getMarker(), e.getMessage(), e);
        callback.exception(e);
        return;
      }
      catch (Exception e) {
        cancelRPC(responseContext.rpcContext);
        logger.error(task.getMarker(), "Unexpected error executing the storage event.", e);
        callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unexpected error executing the storage event.", e));
        return;
      }

      //Update the contentUpdatedAt timestamp to indicate that the data in this space was modified
      if (task instanceof FeatureTask.ConditionalOperation || task instanceof FeatureTask.DeleteOperation) {
        long now = Service.currentTimeMillis();
        if (now - task.space.contentUpdatedAt > Space.CONTENT_UPDATED_AT_INTERVAL_MILLIS) {
          task.space.contentUpdatedAt = Service.currentTimeMillis();
          task.space.volatilityAtLastContentUpdate = task.space.getVolatility();
          Service.spaceConfigClient.store(task.getMarker(), task.space,
              (ar) -> logger.info(task.getMarker(), "Updated contentUpdatedAt for space {}", task.space.getId()));
        }
      }
      //Send event to potentially registered request-listeners
      if (requestListenerPayload != null)
        notifyListeners(task, eventType, requestListenerPayload);
    });
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

  private static <T extends FeatureTask> void addStoragePerformanceInfo(T task, long storageTime, RpcContext rpcContext) {
    String connectorPerformanceValues = "STime=" + storageTime + ";";
    if (rpcContext != null)
      connectorPerformanceValues += "SResSize=" + rpcContext.getResponseSize() + ";";
    addStreamInfo(task, connectorPerformanceValues);
  }

  private static XyzResponse transform(byte[] value) throws JsonProcessingException {
    byte type = value[0];
    byte[] byteValue = Buffer.buffer(value).getBytes(1, value.length);
    switch (type) {
      case JSON_VALUE: {
        return XyzSerializable.deserialize(new String(byteValue));
      }
      case BINARY_VALUE: {
        return new BinaryResponse().withBytes(byteValue);
      }
    }
    return null;
  }

  private static byte[] transform(XyzResponse value) {
    byte[] byteValue;
    byte[] type = new byte[1];
    if (value instanceof BinaryResponse) {
      byteValue = ((BinaryResponse) value).getBytes();
      type[0] = BINARY_VALUE;
    } else {
      byteValue = value.serialize().getBytes();
      type[0] = JSON_VALUE;
    }
    Buffer b = Buffer.buffer(type).appendBytes(byteValue);
    return b.getBytes();
  }

  public static <T extends FeatureTask> void readCache(T task, Callback<T> callback) {
    if (task.getCacheProfile().serviceTTL > 0) {
      String cacheKey = task.getCacheKey();

      //Check the cache
      Service.cacheClient.get(cacheKey, cacheResult -> {
        if (cacheResult == null) {
          //Cache MISS: Just go on in the task pipeline
          addStreamInfo(task, "CH=0;");
          logger.info(task.getMarker(), "Cache MISS for cache key {}", cacheKey);
        } else {
          //Cache HIT: Set the response for the task to the result from the cache so invoke (in the task pipeline) won't have anything to do
          try {
            task.setResponse(transform(cacheResult));
            task.setCacheHit(true);
            addStreamInfo(task, "CH=1;");
            logger.info(task.getMarker(), "Cache HIT for cache key {}", cacheKey);
          } catch (JsonProcessingException e) {
            //Actually, this should never happen as we're controlling how the data is written to the cache, but you never know ;-)
            //Treating an error as a Cache MISS
            logger.info(task.getMarker(), "Cache MISS (as of JSON parse exception) for cache key {} {}", cacheKey, e);
          }
        }
        callback.call(task);
      });
    } else {
      callback.call(task);
    }
  }

  public static <T extends FeatureTask> void writeCache(T task, Callback<T> callback) {
    callback.call(task);
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
      Service.cacheClient.set(cacheKey, transform(response), cacheProfile.serviceTTL);
    }
  }

  /**
   * @param task the FeatureTask instance
   * @param event The pre-processed event
   * @param callback The callback to be called in case of exception
   * @param <T> the type of the FeatureTask
   * @return Whether to stop the execution as of an exception occurred
   */
  private static <T extends FeatureTask> boolean callProcessedHook(T task, Event event, Callback<T> callback) {
    try {
      task.onPreProcessed(event);
      return false;
    } catch (Exception e) {
      callback.exception(e);
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

  private static <T extends FeatureTask> void handleFailure(Marker marker, Throwable cause, Callback<T> callback) {
    if (cause instanceof Exception) {
      callback.exception((Exception) cause);
      return;
    }
    logger.error(marker, "Unexpected error", cause);
    callback.exception(new Exception(cause));
  }

  private static <T extends FeatureTask> void handleProcessorFailure(Marker marker, AsyncResult<XyzResponse> processingResult,
      Callback<T> callback) {
    if (processingResult.failed()) {
      handleFailure(marker, processingResult.cause(), callback);
    } else if (processingResult.result() instanceof ErrorResponse) {
      callback.exception(Api.responseToHttpException(processingResult.result()));
    } else {
      callback.exception(new Exception("Unexpected exception during processor error handling."));
    }
  }

  static <T extends FeatureTask> void setAdditionalEventProps(T task, Connector connector, Event event) {
    event.setMetadata(task.getJwt().metadata);
    if (connector.trusted) {
      event.setTid(task.getJwt().tid);
      event.setAid(task.getJwt().aid);
      event.setJwt(task.getJwt().jwt);
    }
  }

  private static <T extends FeatureTask> void notifyListeners(T task, String eventType, Payload payload) {
    notifyConnectors(task, ConnectorType.LISTENER, eventType, payload, null);
  }

  private static <T extends FeatureTask> void notifyProcessors(T task, String eventType, Payload payload,
      Handler<AsyncResult<XyzResponse>> callback) {
    notifyConnectors(task, ConnectorType.PROCESSOR, eventType, payload, callback);
  }

  private static <T extends FeatureTask> void notifyConnectors(T task, ConnectorType connectorType, String eventType,
      Payload payload, Handler<AsyncResult<XyzResponse>> callback) {
    //Send the event to all registered & matching listeners / processors
    Map<String, List<ResolvableListenerConnectorRef>> connectorMap = task.space.getEventTypeConnectorRefsMap(connectorType);
    if (connectorMap != null && !connectorMap.isEmpty()) {
      String phase = payload instanceof Event ? "request" : "response";
      String notificationEventType = eventType + "." + phase;

      if (connectorMap.containsKey(notificationEventType)) {
        List<ResolvableListenerConnectorRef> connectors = connectorMap.get(notificationEventType);
        if (connectorType == ConnectorType.LISTENER) {
          notifyListeners(task, connectors, notificationEventType, payload);
          return;
        } else if (connectorType == ConnectorType.PROCESSOR) {
          notifyProcessors(task, connectors, notificationEventType, payload, callback);
          return;
        } else {
          throw new RuntimeException("Unsupported connector type.");
        }
      }
    }
    if (callback != null) {
      callback.handle(Future.succeededFuture(null));
    }
  }

  private static <T extends FeatureTask> void notifyListeners(T task, List<ResolvableListenerConnectorRef> listeners,
      String notificationEventType, Payload payload) {
    listeners.forEach(l -> {
      RpcClient client;
      try {
        client = getRpcClient(l.resolvedConnector);
      }
      catch (Exception e) {
        logger.warn(task.getMarker(), "Error when trying to get client for remote function (listener) {}.", l.getId(), e);
        return;
      }
      //Send the event (notify the listener)
      client.send(task.getMarker(), createNotification(task, payload, notificationEventType, l));
    });
  }

  private static <T extends FeatureTask> void notifyProcessors(T task, List<ResolvableListenerConnectorRef> processors,
      String notificationEventType, Payload payload, Handler<AsyncResult<XyzResponse>> callback) {

    //For the first call we're mocking a ModifiedPayloadResponse as if it was coming from a previous processor
    ModifiedPayloadResponse initialResponse = payload instanceof Event ?
        new ModifiedEventResponse().withEvent((Event<? extends Event>) payload) : new ModifiedResponseResponse().withResponse(payload);
    CompletableFuture<XyzResponse> initialFuture = CompletableFuture.completedFuture(initialResponse);
    final List<FeatureCollection.ModificationFailure> failed = new LinkedList<>();

    CompletableFuture<XyzResponse> processedResult = processors.stream().reduce(initialFuture, (prevFuture, processor) -> {
      CompletableFuture<XyzResponse> nextFuture = new CompletableFuture<>();

      prevFuture
          //In case of error fail all following stages directly as well
          .exceptionally(e -> {
            nextFuture.completeExceptionally(e);
            return null;
          })
          //Execute the processor with the result of the previous processor and inform following stages about the outcome
          .thenAccept(result -> {
            if (task.getState().isFinal()) return;
            if (result == null) {
              return; //Happens in case of exception. Then the exceptionally handler already took over to inform the following stages.
            }

            Payload payloadToSend;

            if (result instanceof ErrorResponse) {
              //Handle well-thrown connector error by bubbling it through all stages till the end
              nextFuture.complete(result);
              return;
            }
            else if (result instanceof ModifiedEventResponse) {
              payloadToSend = ((ModifiedEventResponse) result).getEvent();
              // CMEKB-2779 Store ModificationFailures outside of the event
              if (payloadToSend instanceof ModifyFeaturesEvent) {
                ModifyFeaturesEvent modifyFeaturesEvent = (ModifyFeaturesEvent) payloadToSend;
                if (modifyFeaturesEvent.getFailed() != null) {
                  failed.addAll(modifyFeaturesEvent.getFailed());
                  modifyFeaturesEvent.setFailed(null);
                }
              }
            }
            else {
              payloadToSend = ((ModifiedResponseResponse) result).getResponse();
            }

            //Execute the processor with the event / response payload (do pre-processing / post-processing)
            executeProcessor(task, processor, notificationEventType, payloadToSend)
                .exceptionally(ex -> {
                  nextFuture.completeExceptionally(ex);
                  return null;
                })
                .thenAccept(processed -> {
                  if (processed != null) {
                    nextFuture.complete(processed);
                  }
                });
          });

      //Return the future for the next processor
      return nextFuture;
    }, (result1, result2) -> result1 == null ? result2 : result1);

    //Finally report the result of the last stage
    processedResult
        .exceptionally(ex -> {
          callback.handle(Future.failedFuture(ex));
          return null;
        })
        .thenAccept(processed -> {
          if (processed != null) {
            //CMEKB-2779 Get ModificationFailures from last processor and set the collected list
            if (processed instanceof ModifiedEventResponse &&
                ((ModifiedEventResponse) processed).getEvent() instanceof ModifyFeaturesEvent) {
              ModifyFeaturesEvent event = (ModifyFeaturesEvent) ((ModifiedEventResponse) processed).getEvent();
              if (event.getFailed() != null) {
                failed.addAll(event.getFailed());
              }
              event.setFailed(failed.isEmpty() ? null : failed);
            }
            callback.handle(Future.succeededFuture(processed));
          }
        });
  }

  private static <T extends FeatureTask> CompletableFuture<XyzResponse> executeProcessor(T task,
      ResolvableListenerConnectorRef p, String notificationEventType, Payload payload) {
    CompletableFuture<XyzResponse> f = new CompletableFuture<>();

    RpcClient client;
    try {
      client = getRpcClient(p.resolvedConnector);
    }
    catch (Exception e) {
      f.completeExceptionally(new Exception("Error when trying to get client for remote function (processor) " + p.getId() + ".", e));
      return f;
    }
    //Execute the processor with the event / response payload (do pre-processing / post-processing)
    RpcContext rpcContext = client.execute(task.getMarker(), createNotification(task, payload, notificationEventType, p), ar -> {
      if (ar.failed()) {
        f.completeExceptionally(ar.cause());
      } else {
        f.complete(ar.result());
      }
    });
    task.addCancellingHandler(unused -> rpcContext.cancelRequest());
    return f;
  }

  private static <T extends FeatureTask> EventNotification createNotification(T task, Payload payload, String notificationEventType,
      ResolvableListenerConnectorRef l) {
    //Create the EventNotification-event
    EventNotification event = new EventNotification()
        .withParams(l.getParams())
        .withEventType(notificationEventType)
        .withEvent(payload)
        .withSpace(task.space.getId())
        .withStreamId(task.getMarker().getName());
    setAdditionalEventProps(task, l.resolvedConnector, event);
    return event;
  }

  /**
   * Resolves the space, its storage and its listeners.
   */
  static <X extends FeatureTask> void resolveSpace(final X task, final Callback<X> callback) {
    try {
      //FIXME: Can be removed once the Space events are handled by the SpaceTaskHandler (refactoring pending ...)
      if (task.space != null) { //If the space is already given we don't need to retrieve it
        onSpaceResolved(task, callback);
        return;
      }

      //Load the space definition.
      Service.spaceConfigClient.get(task.getMarker(), task.getEvent().getSpace(), arSpace -> {
        if (arSpace.failed()) {
          logger
              .info(task.getMarker(), "Unable to load the space definition for space '{}' {}", task.getEvent().getSpace(), arSpace.cause());
          callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition", arSpace.cause()));
          return;
        }
        task.space = arSpace.result();
        if (task.space != null) {
          task.getEvent().setParams(task.space.getStorage().getParams());
        }
        onSpaceResolved(task, callback);
      });
    } catch (Exception e) {
      callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definition.", e));
    }
  }

  private static <X extends FeatureTask> void onSpaceResolved(final X task, final Callback<X> callback) {
    if (task.space == null) {
      callback.exception(new HttpException(NOT_FOUND, "The resource with this ID does not exist."));
      return;
    }
    logger.debug(task.getMarker(), "Given space configuration is: {}", Json.encode(task.space));

    final String storageId = task.space.getStorage().getId();
    addStreamInfo(task, "SID=" + storageId + ";");
    Space.resolveConnector(task.getMarker(), storageId, (arStorage) -> {
      if (arStorage.failed()) {
        callback.exception(new InvalidStorageException("Unable to load the definition for this storage."));
        return;
      }

      task.storage = arStorage.result();

      try {
        //Also resolve all listeners & processors
        CompletableFuture.allOf(
            resolveConnectors(task.getMarker(), task.space, ConnectorType.LISTENER),
            resolveConnectors(task.getMarker(), task.space, ConnectorType.PROCESSOR)
        ).thenRun(() -> {
          //All listener & processor refs have been resolved now
          callback.call(task);
        });
      } catch (Exception e) {
        logger.error(task.getMarker(), "The listeners for this space cannot be initialized", e);
        callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "The listeners for this space cannot be initialized"));
      }
    });
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

  static void preprocessConditionalOp(ConditionalOperation task, Callback<ConditionalOperation> callback) throws Exception {
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
            callback.exception(new HttpException(BAD_REQUEST, "Minimum length of object id should be 1."));
            return;
          }
          // Test for duplicate IDs
          if (ids.containsKey(id)) {
            logger.info(task.getMarker(), "Objects with the same ID {} are included in the request.", id);
            callback.exception(new HttpException(BAD_REQUEST, "Objects with the same ID " + id + " is included in the request."));
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
      logger.error(task.getMarker(), e.getMessage(), e);
      callback.exception(new HttpException(BAD_REQUEST, "Unable to process the request input."));
      return;
    }
    callback.call(task);
  }

  static void processConditionalOp(ConditionalOperation task, Callback<ConditionalOperation> callback) throws Exception {
    try {
      task.modifyOp.process();
      final List<Feature> insert = new ArrayList<>();
      final List<Feature> update = new ArrayList<>();
      final Map<String, String> delete = new HashMap<>();
      List<FeatureCollection.ModificationFailure> fails = new ArrayList<>();

      long now = Service.currentTimeMillis();

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

          final XyzNamespace nsXyz = result.getProperties().getXyzNamespace();

          // Set the space ID
          nsXyz.setSpace(task.space.getId());

          // Normalize the tags
          final List<String> tags = nsXyz.getTags();
          if (tags != null) {
            XyzNamespace.normalizeTagsOfFeature(result);
          } else {
            nsXyz.setTags(new ArrayList<>());
          }

          nsXyz.withInputPosition((long) i);

          // INSERT
          if (entry.head == null) {
            // Timestamps
            nsXyz.setCreatedAt(now);
            nsXyz.setUpdatedAt(now);

            // UUID
            if (task.space.isEnableUUID()) {
              nsXyz.setUuid(java.util.UUID.randomUUID().toString());
            }
            insert.add(result);
          }
          // UPDATE
          else {
            // Timestamps
            nsXyz.setCreatedAt(entry.head.getProperties().getXyzNamespace().getCreatedAt());
            nsXyz.setUpdatedAt(now);

            // UUID
            if (task.space.isEnableUUID()) {
              nsXyz.setUuid(java.util.UUID.randomUUID().toString());
              nsXyz.setPuuid(entry.head.getProperties().getXyzNamespace().getUuid());
              // If the user was updating an older version, set it under the merge uuid
              if (!entry.base.equals(entry.head)) {
                nsXyz.setMuuid(entry.base.getProperties().getXyzNamespace().getUuid());
              }
            }
            update.add(result);
          }
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

      callback.call(task);
    } catch (ModifyOpError e) {
      logger.info(task.getMarker(), "ConditionalOperationError: {}", e.getMessage(), e);
      throw new HttpException(CONFLICT, e.getMessage());
    }
  }

  static void updateTags(FeatureTask.ConditionalOperation task, Callback<FeatureTask.ConditionalOperation> callback) {
    if ((task.addTags == null || task.addTags.size() == 0) && (task.removeTags == null || task.removeTags.size() == 0)) {
      callback.call(task);
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

    callback.call(task);
  }

  static <X extends FeatureTask<?, X>> void enforceUsageQuotas(X task, Callback<X> callback) {
    final long maxFeaturesPerSpace = task.getJwt().limits != null ? task.getJwt().limits.maxFeaturesPerSpace : -1;
    if (maxFeaturesPerSpace <= 0) {
      callback.call(task);
      return;
    }

    Long cachedCount = countCache.get(task.space.getId());
    if (cachedCount != null) {
      checkFeaturesPerSpaceQuota(task, callback, maxFeaturesPerSpace, cachedCount);
      return;
    }

    getCountForSpace(task, countResult -> {
      if (countResult.failed()) {
        callback.exception(new Exception(countResult.cause()));
        return;
      }
      // Check the quota
      Long count = countResult.result();
      long ttl = (maxFeaturesPerSpace - count > 100_000) ? 60 : 10;
      countCache.put(task.space.getId(), count, ttl, TimeUnit.SECONDS);
      checkFeaturesPerSpaceQuota(task, callback, maxFeaturesPerSpace, count);
    });
  }

  private static <X extends FeatureTask<?, X>> void checkFeaturesPerSpaceQuota(X task, Callback<X> callback,
      long maxFeaturesPerSpace, Long count) {
    try {
      ModifyFeaturesEvent modifyEvent = (ModifyFeaturesEvent) task.getEvent();
      if (modifyEvent != null) {
        final List<Feature> insertFeaturesList = modifyEvent.getInsertFeatures();
        final int insertFeaturesSize = insertFeaturesList == null ? 0 : insertFeaturesList.size();
        final Map<String, String> deleteFeaturesMap = modifyEvent.getDeleteFeatures();
        final int deleteFeaturesSize = deleteFeaturesMap == null ? 0 : deleteFeaturesMap.size();
        final int featuresDelta = insertFeaturesSize - deleteFeaturesSize;
        final String spaceId = modifyEvent.getSpace();
        if (featuresDelta > 0 && count + featuresDelta > maxFeaturesPerSpace) {
          callback.exception(new HttpException(FORBIDDEN,
              "The maximum number of " + maxFeaturesPerSpace + " features for the resource \"" + spaceId + "\" was reached. " +
              "The resource contains " + count + " features and cannot store " + featuresDelta + " more features."));
          return;
        }
      }
      callback.call(task);
    } catch (Exception e) {
      callback.exception(e);
    }
  }

  private static <X extends FeatureTask<?, X>>void getCountForSpace(X task, Handler<AsyncResult<Long>> handler) {
    final CountFeaturesEvent countEvent = new CountFeaturesEvent();
    countEvent.setSpace(task.getEvent().getSpace());
    countEvent.setParams(task.getEvent().getParams());

    try {
      getRpcClient(task.storage)
          .execute(task.getMarker(), countEvent, (AsyncResult<XyzResponse> eventHandler) -> {
            if (eventHandler.failed()) {
              handler.handle(Future.failedFuture((eventHandler.cause())));
              return;
            }
            Long count;
            final XyzResponse response = eventHandler.result();
            if (response instanceof CountResponse) {
              count = ((CountResponse) response).getCount();
            } else if (response instanceof FeatureCollection) {
              count = ((FeatureCollection) response).getCount();
            } else {
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

  static void transformResponse(TileQuery task, Callback<TileQuery> callback) {
    if ((ApiResponseType.MVT != task.responseType && ApiResponseType.MVT_FLATTENED != task.responseType)
        || !(task.getResponse() instanceof FeatureCollection)) {
      callback.call(task);
      return;
    }

    BinaryResponse binaryResponse = new BinaryResponse();
    binaryResponse.setEtag(task.getResponse().getEtag());
    TransformationContext tc = task.transformationContext;

    //The mvt transformation is not executed, if the source feature collection is the same.
    if (!task.etagMatches()) {
      try {
        byte[] mvt;
        if (ApiResponseType.MVT == task.responseType) {
          mvt = new MapBoxVectorTileBuilder()
              .build(WebMercatorTile.forWeb(tc.level, tc.x, tc.y), tc.margin, task.space.getId(),
                  ((FeatureCollection) task.getResponse()).getFeatures());
        } else {
          mvt = new MapBoxVectorTileFlattenedBuilder()
              .build(WebMercatorTile.forWeb(tc.level, tc.x, tc.y), tc.margin, task.space.getId(),
                  ((FeatureCollection) task.getResponse()).getFeatures());
        }
        binaryResponse.setBytes(mvt);
      } catch (Exception e) {
        logger.info(task.getMarker(), "Exception while transforming the response.", e);
        callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Error while transforming the response."));
        return;
      }
    }

    task.setResponse(binaryResponse);

    callback.call(task);
  }

  private static <T extends FeatureTask> void addStreamInfo(T task, String streamInfoValues) {
    if (task.context.response().headers().contains(STREAM_INFO))
      streamInfoValues = task.context.response().headers().get(STREAM_INFO) + streamInfoValues;

    task.context.response().putHeader(STREAM_INFO, streamInfoValues);
  }

  public static <X extends FeatureTask<?, X>> void validate(X task, Callback<X> callback) {
    if (task instanceof ReadQuery && ((ReadQuery) task).hasPropertyQuery()
        && !task.storage.capabilities.propertySearch) {
      callback.exception(new HttpException(BAD_REQUEST, "Property search queries are not supported by storage connector "
          + "\"" + task.storage.id + "\"."));
      return;
    }

    if (task.getEvent() instanceof GetFeaturesByBBoxEvent) {
      GetFeaturesByBBoxEvent event = (GetFeaturesByBBoxEvent) task.getEvent();
      String clusteringType = event.getClusteringType();
      if (clusteringType != null && (task.storage.capabilities.clusteringTypes == null
          || !task.storage.capabilities.clusteringTypes.contains(clusteringType))) {
        callback.exception(new HttpException(BAD_REQUEST, "Clustering of type \"" + clusteringType + "\" is not"
            + "supported by storage connector \"" + task.storage.id + "\"."));
      }
    }
    callback.call(task);
  }

  static <X extends FeatureTask<?, X>> void convertResponse(X task, Callback<X> callback) throws JsonProcessingException {
    if (task instanceof FeatureTask.GetStatistics) {
      if (task.getResponse() instanceof StatisticsResponse) {
        //Ensure the StatisticsResponse is correctly set-up
        StatisticsResponse response = (StatisticsResponse) task.getResponse();
        defineGlobalSearchableField(response, task);
      }
    } else if (task instanceof FeatureTask.IdsQuery) {
      //Ensure to return a FeatureCollection when there are multiple features in the response (could happen e.g. for a virtual-space)
      if (task.getResponse() instanceof FeatureCollection && ((FeatureCollection) task.getResponse()).getFeatures() != null
          && ((FeatureCollection) task.getResponse()).getFeatures().size() > 1) {
        task.responseType = ApiResponseType.FEATURE_COLLECTION;
      }
    }
    callback.call(task);
  }

  private static void defineGlobalSearchableField(StatisticsResponse response, FeatureTask task) {
    if (!task.storage.capabilities.propertySearch) {
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

  static <X extends FeatureTask<?, X>> void checkPreconditions(X task, Callback<X> callback) throws HttpException {
    if (task.space.isReadOnly() && (task instanceof ConditionalOperation || task instanceof DeleteOperation)) {
      throw new HttpException(METHOD_NOT_ALLOWED,
          "The method is not allowed, because the resource \"" + task.space.getId() + "\" is marked as read-only. Update the resource definition to enable editing of features.");
    }
    callback.call(task);
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

    <T extends FeatureTask, R extends XyzResponse> void enrichResponse(T task, R response) {
      if (task instanceof ConditionalOperation && response instanceof FeatureCollection && ((ConditionalOperation) task).hasNonModified) {
        ((ConditionalOperation) task).modifyOp.entries.stream().filter(e -> !e.isModified).forEach(e -> {
          try {
            ((FeatureCollection) response).getFeatures().add(e.result);
          } catch (JsonProcessingException ignored) {}
        });
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
}
