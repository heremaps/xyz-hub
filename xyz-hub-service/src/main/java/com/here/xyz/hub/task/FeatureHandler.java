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
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.TooManyRequestsException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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

  public static Future<FeatureCollection> writeFeatures(Space space, byte[] featureData, boolean partialUpdates,
      UpdateStrategy updateStrategy, SpaceContext spaceContext, String author) {
    try {
      throttle(space);
    }
    catch (HttpException e) {
      return Future.failedFuture(e);
    }

    WriteFeaturesEvent event = new WriteFeaturesEvent()
        .withFeatureData(featureData)
        .withUpdateStrategy(updateStrategy)
        .withPartialUpdates(partialUpdates)
        .withContext(spaceContext)
        .withAuthor(author);

    //Enrich event with properties from the space
    injectSpaceParams(event, space);



    //.then(FeatureTaskHandler::resolveSpace) [DONE]
    //TODO: resolve space [DONE]
    //TODO: resolve connector, add SID to stream Info [DONE]
    //TODO: Resolve listeners & processors
    //TODO: resolveExtendedSpaces
    //.then(FeatureTaskHandler::injectSpaceParams) [DONE]
    //.then(FeatureTaskHandler::checkPreconditions) [DONE]

    //TODO: authorize & enforceUsageQuotas
    //.then(Authorization::authorizeComposite) -> ~ Not sure if needed (does nothing currently) [DONE - delegate]
    //.then(FeatureAuthorization::authorize) [DONE]
    //.then(FeatureTaskHandler::enforceUsageQuotas) [DONE]
    //TODO: throttle [DONE]
    //.then(FeatureTaskHandler::throttle) [DONE]

    //TODO: Invoke storage connector (and others)
    //.then(FeatureTaskHandler::invoke)


    //TODO: In FeatureWriter (also return unmodified features?)
    //.then(FeatureTaskHandler::extractUnmodifiedFeatures)




    return null;


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
    if (event instanceof ContextAwareEvent contextAwareEvent)
      contextAwareEvent.setVersionsToKeep(space.getVersionsToKeep());
  }

  public static Future<Long> getCountForSpace(Marker marker, Space space, SpaceContext spaceContext, String requesterId) {
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
            countCache.put(space.getId(), count, 60, SECONDS);
            promise.complete(count);
          }, space, requesterId);
      return promise.future();
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public static void checkFeaturesPerSpaceQuota(String spaceId, long maxFeaturesPerSpace, long currentSpaceCount,
      boolean isDeleteOnly) throws HttpException {
    if (!isDeleteOnly && currentSpaceCount > maxFeaturesPerSpace)
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
      throw new HttpException(METHOD_NOT_ALLOWED,
          "The method is not allowed, because the resource \"" + space.getId() + "\" is marked as read-only. "
              + "Update the resource definition to enable editing of features.");
  }
}
