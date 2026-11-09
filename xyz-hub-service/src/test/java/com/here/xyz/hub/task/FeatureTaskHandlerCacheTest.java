/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.FeatureTask.GetStatistics;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.responses.StatisticsResponse;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeatureTaskHandlerCacheTest {

  private Config previousConfiguration;
  private CacheClient previousVolatileCacheClient;
  private CacheClient previousStaticCacheClient;
  private final List<Object> sentResponses = new ArrayList<>();
  private final List<Throwable> sentErrors = new ArrayList<>();

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    previousVolatileCacheClient = Service.volatileCacheClient;
    previousStaticCacheClient = Service.staticCacheClient;
    Service.configuration = new Config();
    Service.volatileCacheClient = mock(CacheClient.class);
    Service.staticCacheClient = mock(CacheClient.class);
    when(Service.volatileCacheClient.get(anyString())).thenReturn(Future.succeededFuture(null));
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
    Service.volatileCacheClient = previousVolatileCacheClient;
    Service.staticCacheClient = previousStaticCacheClient;
  }

  @Test
  void sendsOneResponseWhenTheSpaceBecomesCacheableDuringTheRequest() {
    //Updated 30 s ago: not cacheable when readCache runs, so it computes no cache key
    StatisticsResponse response = new StatisticsResponse();
    GetStatistics task = statisticsTask(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(30), response, true);

    assertDoesNotThrow(() -> task.execute(t -> sentResponses.add(t.getResponse()), (t, e) -> sentErrors.add(e)));

    assertEquals(List.of(response), sentResponses);
    assertTrue(sentErrors.isEmpty());
    verify(Service.volatileCacheClient, never()).get(anyString());
    verify(Service.volatileCacheClient, never()).set(anyString(), any(), anyLong());
  }

  @Test
  void writesTheCacheWithTheKeyComputedByReadCache() {
    //Updated 2 days ago: cacheable at readCache and at writeCache
    StatisticsResponse response = new StatisticsResponse();
    GetStatistics task = statisticsTask(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2), response, false);

    task.execute(t -> sentResponses.add(t.getResponse()), (t, e) -> sentErrors.add(e));

    assertEquals(1, sentResponses.size());
    assertSame(response, sentResponses.get(0));
    assertTrue(sentErrors.isEmpty());
    String cacheKey = task.getCacheKey();
    verify(Service.volatileCacheClient).get(cacheKey);
    verify(Service.volatileCacheClient).set(eq(cacheKey), any(), anyLong());
  }

  /**
   * Creates a statistics task with the real readCache and writeCache stages around a stand-in for invoke. The stand-in consumes the event
   * and sets the response as invoke does. With crossNoCacheMark, it also moves the space's last content update back by two minutes, as if
   * the 60 s no-cache interval ended while the connector was processing the request.
   */
  private static GetStatistics statisticsTask(long contentUpdatedAt, StatisticsResponse response, boolean crossNoCacheMark) {
    GetStatistics task = new GetStatistics(new GetStatisticsEvent(), routingContext(), ApiResponseType.STATISTICS_RESPONSE, false) {
      @Override
      public TaskPipeline<GetStatistics> createPipeline() {
        return TaskPipeline.<GetStatistics>create(this)
            .then(FeatureTaskHandler::readCache)
            .then((t, callback) -> {
              t.consumeEvent();
              t.setResponse(response);
              if (crossNoCacheMark)
                t.space.setContentUpdatedAt(t.space.getContentUpdatedAt() - TimeUnit.MINUTES.toMillis(2));
              callback.call(t);
            })
            .then(FeatureTaskHandler::writeCache);
      }
    };
    task.space = new Space();
    task.space.setId("test-space");
    task.space.setCreatedAt(contentUpdatedAt);
    task.storage = new Connector();
    task.storage.capabilities.enableAutoCache = true;
    return task;
  }

  private static RoutingContext routingContext() {
    Map<String, Object> data = new HashMap<>();
    JWTPayload jwt = new JWTPayload();
    jwt.aid = "test-aid";
    data.put("jwt", jwt);
    RoutingContext context = mock(RoutingContext.class);
    HttpServerRequest request = mock(HttpServerRequest.class);
    when(context.request()).thenReturn(request);
    when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(context.get(anyString())).thenAnswer(invocation -> data.get(invocation.<String>getArgument(0)));
    when(context.put(anyString(), any())).thenAnswer(invocation -> {
      data.put(invocation.getArgument(0), invocation.getArgument(1));
      return context;
    });
    return context;
  }
}
