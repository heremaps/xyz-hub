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

package com.here.xyz.hub.task;

import static com.here.xyz.util.service.rest.TooManyRequestsException.ThrottlingReason.MEMORY;
import static com.here.xyz.util.service.rest.TooManyRequestsException.ThrottlingReason.STORAGE_QUEUE_FULL;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_REQUIRED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.connectors.models.Space.ConnectorType;
import com.here.xyz.hub.connectors.models.Space.ResolvableListenerConnectorRef;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.models.hub.Space.ListenerConnectorRef;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.rest.TooManyRequestsException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class FeatureHandler {
  private static final Logger logger = LogManager.getLogger();
  private static final ExpiringMap<String, Long> countCache = ExpiringMap.builder()
      .maxSize(1024)
      .variableExpiration()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .build();
  /**
   * Contains the number of all in-flight requests for each storage ID.
   */
  private static final ConcurrentHashMap<String, LongAdder> inflightRequestMemory = new ConcurrentHashMap<>();
  private static final LongAdder globalInflightRequestMemory = new LongAdder();

  public static Future<FeatureCollection> writeFeatures(Marker marker, Space space, Set<Modification> modifications, SpaceContext spaceContext,
      String author, boolean responseDataExpected) {
    try {
      throttle(space);

      WriteFeaturesEvent event = new WriteFeaturesEvent()
          .withStreamId(marker.getName())
          .withModifications(modifications)
          .withContext(spaceContext)
          .withAuthor(author)
          .withResponseDataExpected(responseDataExpected);

      //Enrich event with properties from the space
      injectSpaceParams(event, space);

      Promise<FeatureCollection> promise = Promise.promise();
      RpcContext rpcContext = getRpcClient(space.getResolvedStorageConnector())
          .execute(marker, event, ar -> {
            if (ar.failed())
              promise.fail(ar.cause());
            else if (ar.result() instanceof FeatureCollection featureCollection)
              promise.complete(featureCollection);
            else
              promise.fail(new RuntimeException("Received unexpected response from storage connector: " + ar.result().getClass().getSimpleName()));
          });
      return promise.future();

      //TODO: (For later) In FeatureWriter (also return unmodified features?)
      //.then(FeatureTaskHandler::extractUnmodifiedFeatures)

    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  static void throttle(Space space) throws HttpException {
    Connector storage = space.getResolvedStorageConnector();
    final long GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE = (long) Service.configuration.GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE_MB * 1024 * 1024;
    float usedMemoryPercent = Service.getUsedMemoryPercent() / 100f;

    //When ZGC is in use, only throttle requests if the service memory filled up over the specified service memory threshold
    if (Service.IS_USING_ZGC) {
      if (usedMemoryPercent > Service.configuration.SERVICE_MEMORY_HIGH_UTILIZATION_THRESHOLD)
        throw new TooManyRequestsException("Too many requests for the service node.", MEMORY);
    }
    //For other GCs, only throttle requests if the request memory filled up over the specified request memory threshold
    else if (globalInflightRequestMemory.sum() >
        GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE * Service.configuration.GLOBAL_INFLIGHT_REQUEST_MEMORY_HIGH_UTILIZATION_THRESHOLD) {
      LongAdder storageInflightRequestMemory = inflightRequestMemory.get(storage.id);
      long storageInflightRequestMemorySum = 0;
      if (storageInflightRequestMemory == null || (storageInflightRequestMemorySum = storageInflightRequestMemory.sum()) == 0)
        return; //Nothing to throttle for that storage

      RpcClient rpcClient = getRpcClient(storage);
      if (storageInflightRequestMemorySum > rpcClient.getFunctionClient().getPriority() * GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE)
        throw new TooManyRequestsException("Too many requests for the storage.", STORAGE_QUEUE_FULL);
    }
  }

  static void injectSpaceParams(Event event, Space space) {
    event.setSpace(space.getId());
    if (event instanceof ContextAwareEvent contextAwareEvent)
      contextAwareEvent.setVersionsToKeep(space.getVersionsToKeep());

    Map<String, Object> storageParams = new HashMap<>();
    if (space.getStorage().getParams() != null)
      storageParams.putAll(space.getStorage().getParams());

    if (space.getExtension() != null) {
      Map<String, Object> extendsMap = space.getExtension().toMap();
      //Check if the extended space itself is extending some other space (2-level extension)
      if (space.getExtension().resolvedSpace.getExtension() != null)
        extendsMap.put("extends", space.getExtension().resolvedSpace.getExtension().toMap());
      storageParams.putAll(Map.of("extends", extendsMap));
    }

    event.setParams(storageParams);
  }

  public static Future<Long> getCountForSpace(Marker marker, Space space, SpaceContext spaceContext, String requesterId,
      long maxFeaturesPerSpace) {
    Long cachedCount = countCache.get(space.getId());
    if (cachedCount != null)
      return Future.succeededFuture(cachedCount);

    GetStatisticsEvent countEvent = new GetStatisticsEvent()
        .withSpace(space.getId())
        .withContext(spaceContext);

    try {
      Promise<Long> promise = Promise.promise();
      getRpcClient(space.getResolvedStorageConnector())
          .execute(marker, countEvent, (AsyncResult<XyzResponse> eventHandler) -> {
            if (eventHandler.failed()) {
              promise.fail(eventHandler.cause());
              return;
            }
            long count;
            XyzResponse response = eventHandler.result();
            if (response instanceof StatisticsResponse)
              count = ((StatisticsResponse) response).getCount().getValue();
            else {
              promise.fail(Api.responseToHttpException(response));
              return;
            }
            long ttl = maxFeaturesPerSpace - count > 100_000 ? 30_000 : 500;
            countCache.put(space.getId(), count, ttl, MILLISECONDS);
            promise.complete(count);
          }, space, requesterId);
      return promise.future();
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public static void checkFeaturesPerSpaceQuota(String spaceId, long maxFeaturesPerSpace, long currentSpaceCount, boolean isDeleteOnly)
      throws HttpException {
    if (!isDeleteOnly && currentSpaceCount >= maxFeaturesPerSpace)
      throw new HttpException(FORBIDDEN,
          "The maximum number of " + maxFeaturesPerSpace + " features for the resource \"" + spaceId + "\" was reached. " +
              "The resource contains " + currentSpaceCount + " features and cannot store any more features.");
  }

  private static RpcClient getRpcClient(Connector refConnector) throws HttpException {
    try {
      return RpcClient.getInstanceFor(refConnector);
    }
    catch (IllegalStateException e) {
      throw new HttpException(BAD_GATEWAY, "Connector not ready.");
    }
  }

  public static void checkReadOnly(Space space) throws HttpException {
    if (space.isReadOnly())
      throw new DetailedHttpException("E318452", Map.of("resourceId", space.getId()));
  }

  public static void checkIsActive(Space space) throws HttpException {
    if (!space.isActive())
      throw new DetailedHttpException("E318451", Map.of("resourceId", space.getId()));
  }

  public static Future<Void> resolveExtendedSpaces(Marker marker, Space compositeSpace) {
    return resolveExtendedSpace(marker, compositeSpace, compositeSpace.getExtension(), new ArrayList<>(List.of(compositeSpace.getId())));
  }

  private static Future<Void> resolveExtendedSpace(Marker marker, Space compositeSpace, Extension extendedConfig, List<String> resolvedIds) {
    if (extendedConfig == null)
      return Future.succeededFuture();
    return Space.resolveSpace(marker, extendedConfig.getSpaceId())
        .compose(extendedSpace -> {
          if (extendedSpace == null)
            return Future.failedFuture(new DetailedHttpException("E318442", Map.of("resource", extendedConfig.getSpaceId())));

          //Check for cyclical extensions
          if (resolvedIds.contains(extendedSpace.getId())) {
            logger.error(marker, "Cyclical extension on composite space {}. Causing extended space: {}.",
                compositeSpace.getId(), extendedSpace.getId());
            return Future.failedFuture(new HttpException(BAD_REQUEST, "Cyclical reference when resolving extensions"));
          }

          resolvedIds.add(extendedSpace.getId());
          extendedConfig.resolvedSpace = extendedSpace;
          return resolveExtendedSpace(marker, compositeSpace, extendedSpace.getExtension(), resolvedIds);
        });
  }

  /**
   * Returns true if the extended space is the provided composite space itself
   * or if it is already part of the "extension-path" of the provided composite space.
   * @param mainCompositeSpace The composite space within which to check whether the extendedSpace is part of its extension-path
   * @param extendedSpace The extended space for which to check whether it is already part of the extension-path of the extendedSpace
   * @return Whether the extendedSpace is contained within the extension-path of mainCompositeSpace already
   */
  private static boolean inExtensionPath(Space mainCompositeSpace, Space extendedSpace) {
    return mainCompositeSpace.getId().equals(extendedSpace.getId())
        || mainCompositeSpace.getExtension() != null && mainCompositeSpace.getExtension().resolvedSpace != null
        && inExtensionPath((Space) mainCompositeSpace.getExtension().resolvedSpace, extendedSpace);
  }

  public static Future<Void> resolveListenersAndProcessors(Marker marker, Space space) {
    Promise<Void> p = Promise.promise();
    try {
      //Also resolve all listeners & processors
      CompletableFuture.allOf(
          resolveConnectors(marker, space, ConnectorType.LISTENER),
          resolveConnectors(marker, space, ConnectorType.PROCESSOR)
      ).thenRun(() -> {
        //All listener & processor refs have been resolved now
        p.complete();
      });
    }
    catch (Exception e) {
      logger.error(marker, "The listeners for this space cannot be initialized", e);
      p.fail(new HttpException(INTERNAL_SERVER_ERROR, "The listeners for this space cannot be initialized"));
    }
    return p.future();
  }

  static CompletableFuture<Void> resolveConnectors(Marker marker, final Space space, final ConnectorType connectorType) {
    if (space == null || connectorType == null) {
      return CompletableFuture.completedFuture(null);
    }

    final Map<String, List<ListenerConnectorRef>> connectorRefs = space.getConnectorRefsMap(connectorType);

    if (connectorRefs == null || connectorRefs.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<String, List<ListenerConnectorRef>> entry : connectorRefs.entrySet()) {
      if (entry.getValue() != null && !entry.getValue().isEmpty()) {
        ListIterator<ListenerConnectorRef> i = entry.getValue().listIterator();
        while (i.hasNext()) {
          ListenerConnectorRef cR = i.next();
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
}
