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

import static com.here.xyz.hub.cache.QueryTimeoutCache.QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL;
import static com.here.xyz.hub.cache.QueryTimeoutCache.QUERY_TIMEOUT_CACHE_TTL;
import static com.here.xyz.util.service.BaseHttpServerVerticle.STREAM_INFO_CTX_KEY;
import static com.here.xyz.util.service.rest.Api.RESPONSE_PAYLOAD_TOO_LARGE;
import static com.here.xyz.util.service.rest.Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.cache.QueryTimeoutCache;
import com.here.xyz.hub.connectors.StatementTimeoutException;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs spatial queries through the real readCache and writeCache stages around a stand-in for invoke, which takes the 23 s of a
 * statement timeout on a test clock and reports the outcome to the query timeout cache in the same way as RpcClient and invoke do.
 */
class FeatureTaskHandlerQueryTimeoutTest {

  private static final String CONNECTOR_ID = "test-connector";
  private static final long QUERY_MILLIS = SECONDS.toMillis(23);
  /**
   * Exponential backoff without jitter: the client waits 1, 2, 4 … 256 s after each failed attempt and makes 10 attempts.
   */
  private static final int[] BACKOFF_SECONDS = {1, 2, 4, 8, 16, 32, 64, 128, 256};

  private final AtomicLong now = new AtomicLong(1_000_000_000L);
  private final FakeRedis redis = new FakeRedis();
  private Config previousConfiguration;
  private CacheClient previousVolatileCacheClient;
  private CacheClient previousStaticCacheClient;
  private QueryTimeoutCache previousQueryTimeoutCache;
  private Space space;
  private Connector storage;
  /**
   * Whether the stand-in for invoke reports the failures of the connector as RpcClient does, which pauses the cache after other timeouts.
   */
  private boolean reportConnectorFailures = true;

  @BeforeAll
  static void loadErrors() {
    try {
      ErrorManager.loadErrors("hub-errors.json");
    }
    catch (RuntimeException e) {
      //Another test class in the same JVM loaded them already
      if (!(e.getCause() instanceof IllegalStateException) || !e.getCause().getMessage().contains("already registered"))
        throw e;
    }
  }

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    previousVolatileCacheClient = Service.volatileCacheClient;
    previousStaticCacheClient = Service.staticCacheClient;
    previousQueryTimeoutCache = Service.queryTimeoutCache;
    Service.configuration = new Config();
    Service.configuration.QUERY_TIMEOUT_CACHE_ENABLED = true;
    Service.volatileCacheClient = mock(CacheClient.class);
    when(Service.volatileCacheClient.get(anyString())).thenReturn(Future.succeededFuture(null));
    //Responses of immutable versions go to the static cache
    Service.staticCacheClient = mock(CacheClient.class);
    when(Service.staticCacheClient.get(anyString())).thenReturn(Future.succeededFuture(null));
    Service.queryTimeoutCache = new QueryTimeoutCache(redis, now::get);

    //Updated 2 days ago: cacheable
    space = new Space();
    space.setId("test-space");
    space.setCreatedAt(System.currentTimeMillis() - DAYS.toMillis(2));
    storage = new Connector();
    storage.id = CONNECTOR_ID;
    storage.capabilities.enableAutoCache = true;
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
    Service.volatileCacheClient = previousVolatileCacheClient;
    Service.staticCacheClient = previousStaticCacheClient;
    Service.queryTimeoutCache = previousQueryTimeoutCache;
  }

  @Test
  void replaysAnExponentialBackoffWithShortCutsFromAttempt4On() {
    List<Reply> replies = retry(10, attempt -> new StatementTimeoutException());

    assertQueriesRan(replies, 1, 2, 3, 10);
    replies.subList(3, 9).forEach(FeatureTaskHandlerQueryTimeoutTest::assertShortCut);
    //Attempt 10 comes 4 + 8 + 16 + 32 + 64 + 128 + 256 = 508 s after the 3rd timeout, after the 300 s short-cut period
    Reply probe = replies.get(9);
    assertEquals("P", probe.streamInfo("NC"));
    assertEquals(SECONDS.toMillis(508), probe.streamInfo("NCAge"));
    assertInstanceOf(StatementTimeoutException.class, probe.error);
    //The probe timed out again, so the marker is armed again at once
    assertEquals(4, redis.markerCount());
    now.addAndGet(SECONDS.toMillis(1));
    assertShortCut(send(new StatementTimeoutException()));
  }

  @Test
  void aRunThatSucceedsLateGetsItsSuccessAtTheProbe() {
    //The query times out 3 times, afterwards it would succeed
    List<Reply> replies = retry(10, attempt -> attempt <= 3 ? new StatementTimeoutException() : null);

    assertQueriesRan(replies, 1, 2, 3, 10);
    //Attempts 4 to 9 get the 504 although the query would have succeeded: these are the false 504s the probe limits
    replies.subList(3, 9).forEach(FeatureTaskHandlerQueryTimeoutTest::assertShortCut);
    Reply probe = replies.get(9);
    assertEquals("P", probe.streamInfo("NC"));
    assertNull(probe.error);
    assertInstanceOf(FeatureCollection.class, probe.response);
    //The success removes the marker and is cached as usual
    assertEquals(0, redis.size());
    verify(Service.volatileCacheClient).set(anyString(), any(), anyLong());
    Reply next = send(new StatementTimeoutException());
    assertTrue(next.queryRan);
    assertNull(next.streamInfo("NC"));
  }

  @Test
  void timeoutsOtherThanStatementTimeoutsAreNeverCounted() {
    //Without the pause that such timeouts cause, so that only the counting is tested here
    reportConnectorFailures = false;
    List<IntFunction<Throwable>> otherTimeouts = List.of(
        //Connector Lambda timeout and "No time for retry left", see RpcClientTimeoutTest
        attempt -> new DetailedHttpException("E318540"),
        //The service's own request timeout
        attempt -> new HttpException(GATEWAY_TIMEOUT, "The connector did not respond in time."),
        //HTTP connector timeout
        attempt -> new DetailedHttpException("E318540", new TimeoutException()));

    for (IntFunction<Throwable> otherTimeout : otherTimeouts) {
      List<Reply> replies = retry(5, otherTimeout);
      assertQueriesRan(replies, 1, 2, 3, 4, 5);
      assertEquals(0, redis.size());
    }
    //They also end a series of statement timeouts
    send(new StatementTimeoutException());
    send(new StatementTimeoutException());
    send(new DetailedHttpException("E318540"));
    assertEquals(0, redis.size());
  }

  @Test
  void connectionErrorsAreNeverCountedAndEndASeries() {
    List<Reply> replies = retry(4, attempt -> new HttpException(BAD_GATEWAY, "Connector error."));
    assertQueriesRan(replies, 1, 2, 3, 4);
    assertEquals(0, redis.size());

    send(new StatementTimeoutException());
    send(new StatementTimeoutException());
    send(new HttpException(BAD_GATEWAY, "Connector error."));
    assertEquals(0, redis.size());
    //It takes 3 new statement timeouts
    replies = retry(4, attempt -> new StatementTimeoutException());
    assertQueriesRan(replies, 1, 2, 3);
  }

  @Test
  void anotherTimeoutOfTheConnectorPausesTheCacheOnThisNode() {
    retry(3, attempt -> new StatementTimeoutException());
    assertEquals(3, redis.markerCount());

    //Another query on the same connector, the connector does not respond in time
    assertTrue(send(spatialEvent(9.0), false, new DetailedHttpException("E318540")).queryRan);
    //The query with the marker runs, and its timeout is not counted
    Reply paused = send(new StatementTimeoutException());
    assertTrue(paused.queryRan);
    assertNull(paused.streamInfo("NC"));
    assertEquals(3, redis.markerCount());
  }

  @Test
  void skipCacheBypassesTheMarker() {
    retry(3, attempt -> new StatementTimeoutException());

    Reply skipped = send(spatialEvent(8.0), true, new StatementTimeoutException());
    assertTrue(skipped.queryRan);
    assertNull(skipped.streamInfo("NC"));
    //Requests with skipCache=true are not counted either
    for (int i = 0; i < 3; i++)
      send(spatialEvent(9.0), true, new StatementTimeoutException());
    assertTrue(send(spatialEvent(9.0), true, new StatementTimeoutException()).queryRan);
    assertEquals(1, redis.size());
  }

  @Test
  void aContentUpdateBypassesTheMarker() {
    retry(3, attempt -> new StatementTimeoutException());
    long contentUpdatedAt = space.getContentUpdatedAt();

    //Right after a write the space is not cacheable, so readCache does not look at any cache
    space.setContentUpdatedAt(System.currentTimeMillis());
    Reply afterWrite = send(new StatementTimeoutException());
    assertTrue(afterWrite.queryRan);
    assertNull(afterWrite.streamInfo("NC"));
    //Later, the new content update time is part of the cache key
    space.setContentUpdatedAt(contentUpdatedAt + 1);
    Reply later = send(new StatementTimeoutException());
    assertTrue(later.queryRan);
    assertNull(later.streamInfo("NC"));
  }

  @Test
  void aSuccessStartsTheCountAgain() {
    List<Reply> replies = retry(7, attempt -> attempt == 3 ? null : new StatementTimeoutException());

    assertQueriesRan(replies, 1, 2, 3, 4, 5, 6);
    assertShortCut(replies.get(6));
  }

  @Test
  void theShortCutReplyIsTheSameAsTheRealOne() {
    List<Reply> replies = retry(4, attempt -> new StatementTimeoutException());
    Throwable realStatementTimeout = replies.get(2).error;
    Throwable shortCut = replies.get(3).error;
    assertFalse(replies.get(3).queryRan);

    List<String> realReply = render(realStatementTimeout, 504, "E318540");
    assertEquals(realReply, render(shortCut, 504, "E318540"));
    //Clients cannot tell a statement timeout from a connector Lambda timeout either
    assertEquals(realReply, render(new DetailedHttpException("E318540"), 504, "E318540"));
  }

  @Test
  void nothingChangesWhenTheCacheIsSwitchedOff() {
    Service.configuration.QUERY_TIMEOUT_CACHE_ENABLED = false;

    List<Reply> replies = retry(10, attempt -> new StatementTimeoutException());
    send(spatialEvent(9.0), false, new DetailedHttpException("E318540"));

    assertQueriesRan(replies, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    replies.forEach(reply -> assertNull(reply.streamInfo("NC")));
    replies.forEach(reply -> assertInstanceOf(StatementTimeoutException.class, reply.error));
    assertEquals(0, redis.calls);
  }

  @Test
  void aTooLargeResponseOnAReadOnlyVersionIsShortCutAfterTheFirstOneForHours() {
    readOnly(5);

    Reply first = send(connectorTooLarge());
    assertTrue(first.queryRan);
    assertEquals(QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL, redis.markerTtl());
    assertEquals(HOURS.toSeconds(6), QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL);
    //A request every 9 s, and no probe after the 300 s of a writable space
    for (int i = 0; i < 40; i++) {
      now.addAndGet(SECONDS.toMillis(9));
      assertTooLargeShortCut(send(connectorTooLarge()), "E318541");
    }
    now.addAndGet(HOURS.toMillis(6) - SECONDS.toMillis(9 * 40 + 1));
    Reply last = send(connectorTooLarge());
    assertTooLargeShortCut(last, "E318541");
    assertEquals(HOURS.toMillis(6) - SECONDS.toMillis(1), last.streamInfo("NCAge"));
    //The marker expires 6 h after the 513, the next request runs the query again and arms the marker again
    now.addAndGet(SECONDS.toMillis(1));
    Reply afterTtl = send(connectorTooLarge());
    assertTrue(afterTtl.queryRan);
    assertNull(afterTtl.streamInfo("NC"));
    now.addAndGet(SECONDS.toMillis(9));
    assertTooLargeShortCut(send(connectorTooLarge()), "E318541");
  }

  @Test
  void theServicesOwnTooLargeResponseOnAReadOnlyVersionIsShortCutAfterTheFirstOne() {
    readOnly(5);

    assertTrue(send(responseTooLarge()).queryRan);
    now.addAndGet(SECONDS.toMillis(9));
    assertTooLargeShortCut(send(responseTooLarge()), null);
    assertEquals(QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL, redis.markerTtl());
  }

  @Test
  void aVersionAtOrBelowTheReadOnlyHeadVersionIsImmutable() {
    readOnly(5);

    for (long version : new long[] {5, 3, 0}) {
      assertTrue(send(spatialEvent(8.0).withRef(new Ref(version)), false, connectorTooLarge()).queryRan);
      assertTooLargeShortCut(send(spatialEvent(8.0).withRef(new Ref(version)), false, connectorTooLarge()), "E318541");
    }
    //A HEAD read is a read of the readOnlyHeadVersion, so it shares the marker of version 5
    assertTooLargeShortCut(send(connectorTooLarge()), "E318541");
  }

  @Test
  void otherVersionsAndSpacesKeepTheThreshold() {
    //Above the readOnlyHeadVersion
    readOnly(5);
    assertTooLargeNeedsTheThreshold(() -> spatialEvent(8.0).withRef(new Ref(6)));
    //A version of a writable space
    space.setReadOnly(false);
    space.setReadOnlyHeadVersion(-1);
    assertTooLargeNeedsTheThreshold(() -> spatialEvent(9.0).withRef(new Ref(3)));
    //A read-only space without a readOnlyHeadVersion
    space.setReadOnly(true);
    assertTooLargeNeedsTheThreshold(() -> spatialEvent(10.0));
    //A read-only space extending a space which is not bound to a version
    readOnly(5);
    space.setExtension(new Extension().withSpaceId("base-space"));
    assertTooLargeNeedsTheThreshold(() -> spatialEvent(11.0));
  }

  @Test
  void aReadOnlySpaceExtendingABoundVersionIsImmutable() {
    readOnly(5);
    space.setExtension(new Extension().withSpaceId("base-space").withVersion(2L));

    assertTrue(send(connectorTooLarge()).queryRan);
    assertTooLargeShortCut(send(connectorTooLarge()), "E318541");
  }

  @Test
  void aTooLargeResponseOnAWritableSpaceNeedsTheThresholdAndIsProbed() {
    List<Reply> replies = retry(10, attempt -> connectorTooLarge());

    assertQueriesRan(replies, 1, 2, 3, 10);
    replies.subList(3, 9).forEach(reply -> assertTooLargeShortCut(reply, "E318541"));
    Reply probe = replies.get(9);
    assertEquals("P", probe.streamInfo("NC"));
    assertEquals(513, probe.streamInfo("NCStatus"));
    assertEquals(SECONDS.toMillis(508), probe.streamInfo("NCAge"));
    assertEquals(4, redis.markerCount());
    assertEquals(QUERY_TIMEOUT_CACHE_TTL, redis.markerTtl());
    //The probe succeeds: the marker is gone
    now.addAndGet(SECONDS.toMillis(301));
    assertNull(send((Throwable) null).error);
    assertEquals(0, redis.size());

    //The same for the service's own 513
    replies = retry(4, attempt -> responseTooLarge());
    assertQueriesRan(replies, 1, 2, 3);
    assertTooLargeShortCut(replies.get(3), null);
  }

  @Test
  void statementTimeoutsOnAReadOnlyVersionKeepTheThreshold() {
    readOnly(5);

    List<Reply> replies = retry(10, attempt -> new StatementTimeoutException());

    assertQueriesRan(replies, 1, 2, 3, 10);
    replies.subList(3, 9).forEach(FeatureTaskHandlerQueryTimeoutTest::assertShortCut);
    replies.subList(3, 9).forEach(reply -> assertEquals(504, reply.streamInfo("NCStatus")));
    assertEquals(QUERY_TIMEOUT_CACHE_TTL, redis.markerTtl());
  }

  @Test
  void aTooLargeResponseAndAStatementTimeoutEndEachOthersSeries() {
    send(new StatementTimeoutException());
    send(new StatementTimeoutException());
    send(connectorTooLarge());
    assertEquals(1, redis.markerCount());
    List<Reply> replies = retry(4, attempt -> new StatementTimeoutException());
    assertQueriesRan(replies, 1, 2, 3);
    assertShortCut(replies.get(3));

    now.addAndGet(SECONDS.toMillis(301));
    send(connectorTooLarge());
    assertEquals(1, redis.markerCount());
    replies = retry(4, attempt -> connectorTooLarge());
    assertQueriesRan(replies, 1, 2);
    assertTooLargeShortCut(replies.get(3), "E318541");
  }

  @Test
  void bypassesOfTooLargeMarkers() {
    //Writable space: skipCache and content updates
    retry(3, attempt -> connectorTooLarge());
    assertTrue(send(spatialEvent(8.0), true, connectorTooLarge()).queryRan);
    long contentUpdatedAt = space.getContentUpdatedAt();
    space.setContentUpdatedAt(System.currentTimeMillis());
    assertTrue(send(connectorTooLarge()).queryRan);
    space.setContentUpdatedAt(contentUpdatedAt + 1);
    Reply afterUpdate = send(connectorTooLarge());
    assertTrue(afterUpdate.queryRan);
    assertNull(afterUpdate.streamInfo("NC"));

    //Read-only version: skipCache, a non-cacheable space and a write after the space became writable again
    readOnly(5);
    Reply first = send(spatialEvent(9.0), false, connectorTooLarge());
    assertTrue(first.queryRan);
    assertTrue(send(spatialEvent(9.0), true, connectorTooLarge()).queryRan);
    storage.capabilities.enableAutoCache = false;
    assertTrue(send(spatialEvent(9.0), false, connectorTooLarge()).queryRan);
    storage.capabilities.enableAutoCache = true;
    assertTooLargeShortCut(send(spatialEvent(9.0), false, connectorTooLarge()), "E318541");
    space.setReadOnly(false);
    space.setReadOnlyHeadVersion(-1);
    space.setContentUpdatedAt(System.currentTimeMillis());
    assertTrue(send(spatialEvent(9.0), false, connectorTooLarge()).queryRan);
  }

  @Test
  void anotherTimeoutOfTheConnectorPausesTooLargeMarkersToo() {
    readOnly(5);
    send(connectorTooLarge());

    assertTrue(send(spatialEvent(9.0), false, new DetailedHttpException("E318540")).queryRan);
    Reply paused = send(connectorTooLarge());
    assertTrue(paused.queryRan);
    assertNull(paused.streamInfo("NC"));
  }

  @Test
  void theTooLargeShortCutReplyIsTheSameAsTheRealOne() {
    readOnly(5);
    for (Throwable tooLarge : List.of(connectorTooLarge(), responseTooLarge())) {
      GetFeaturesByGeometryEvent event = spatialEvent(tooLarge instanceof DetailedHttpException ? 8.0 : 9.0);
      Reply real = send(event, false, tooLarge);
      Reply shortCut = send(event, false, tooLarge);
      assertTrue(real.queryRan);
      assertFalse(shortCut.queryRan);
      String bodyPart = tooLarge instanceof DetailedHttpException ? "E318541" : RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE;
      assertEquals(render(real.error, 513, bodyPart), render(shortCut.error, 513, bodyPart));
    }
  }

  /**
   * Makes the space read-only at the given version, as the space API does.
   */
  private void readOnly(long readOnlyHeadVersion) {
    space.setReadOnly(true);
    space.setReadOnlyHeadVersion(readOnlyHeadVersion);
  }

  private void assertTooLargeNeedsTheThreshold(Supplier<GetFeaturesByGeometryEvent> event) {
    List<Reply> replies = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      now.addAndGet(SECONDS.toMillis(9));
      replies.add(send(event.get(), false, connectorTooLarge()));
    }
    assertQueriesRan(replies, 1, 2, 3);
    assertTooLargeShortCut(replies.get(3), "E318541");
  }

  /**
   * The connector's PAYLOAD_TO_LARGE error, as RpcClient maps it.
   */
  private static Throwable connectorTooLarge() {
    return new DetailedHttpException("E318541");
  }

  /**
   * The service's own check of the response size in RpcClient.
   */
  private static Throwable responseTooLarge() {
    return new HttpException(RESPONSE_PAYLOAD_TOO_LARGE, RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
  }

  /**
   * @param errorCode The code of the expected error, null for the service's own 513
   */
  private static void assertTooLargeShortCut(Reply reply, String errorCode) {
    assertFalse(reply.queryRan);
    assertEquals("S", reply.streamInfo("NC"));
    assertEquals(513, reply.streamInfo("NCStatus"));
    HttpException tooLarge = assertInstanceOf(HttpException.class, reply.error);
    assertEquals(513, tooLarge.status.code());
    if (errorCode == null) {
      assertFalse(tooLarge instanceof DetailedHttpException);
      assertEquals(RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE, tooLarge.getMessage());
    }
    else
      assertEquals(errorCode, assertInstanceOf(DetailedHttpException.class, tooLarge).errorDefinition.getCode());
  }

  private static void assertShortCut(Reply reply) {
    assertFalse(reply.queryRan);
    assertEquals("S", reply.streamInfo("NC"));
    DetailedHttpException timeout = assertInstanceOf(DetailedHttpException.class, reply.error);
    assertEquals(504, timeout.status.code());
    assertEquals("E318540", timeout.errorDefinition.getCode());
  }

  private static void assertQueriesRan(List<Reply> replies, int... attempts) {
    List<Integer> ran = IntStream.rangeClosed(1, replies.size()).filter(attempt -> replies.get(attempt - 1).queryRan).boxed().toList();
    assertEquals(Arrays.stream(attempts).boxed().toList(), ran);
  }

  /**
   * Sends the same query with the waits of {@link #BACKOFF_SECONDS} between the attempts.
   *
   * @param queryOutcome The failure of the query for each attempt (1-based), null for a success
   */
  private List<Reply> retry(int attempts, IntFunction<Throwable> queryOutcome) {
    List<Reply> replies = new ArrayList<>();
    for (int attempt = 1; attempt <= attempts; attempt++) {
      if (attempt > 1)
        now.addAndGet(SECONDS.toMillis(BACKOFF_SECONDS[attempt - 2]));
      replies.add(send(queryOutcome.apply(attempt)));
    }
    return replies;
  }

  private Reply send(Throwable queryFailure) {
    return send(spatialEvent(8.0), false, queryFailure);
  }

  private Reply send(GetFeaturesByGeometryEvent event, boolean skipCache, Throwable queryFailure) {
    Map<String, Object> contextData = new HashMap<>();
    AtomicBoolean queryRan = new AtomicBoolean();
    XyzResponse queryResponse = new FeatureCollection();
    GeometryQuery task = new GeometryQuery(event, routingContext(contextData), ApiResponseType.FEATURE_COLLECTION, skipCache, null,
        null) {
      @Override
      public TaskPipeline<GeometryQuery> createPipeline() {
        return TaskPipeline.<GeometryQuery>create(this)
            .then(FeatureTaskHandler::checkImmutability)
            .then(FeatureTaskHandler::readCache)
            .then((t, callback) -> {
              t.consumeEvent();
              queryRan.set(true);
              now.addAndGet(QUERY_MILLIS);
              if (queryFailure == null) {
                FeatureTaskHandler.recordQueryTimeoutOutcome(t, Future.succeededFuture(queryResponse));
                t.setResponse(queryResponse);
                callback.call(t);
              }
              else {
                //RpcClient reports each failure of the connector first
                if (reportConnectorFailures)
                  Service.queryTimeoutCache.onConnectorFailure(CONNECTOR_ID, queryFailure);
                FeatureTaskHandler.recordQueryTimeoutOutcome(t, Future.failedFuture(queryFailure));
                callback.exception(queryFailure);
              }
            })
            .then(FeatureTaskHandler::writeCache);
      }
    };
    task.space = space;
    task.storage = storage;

    AtomicReference<XyzResponse> response = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    task.execute(t -> response.set(t.getResponse()), (t, e) -> error.set(e));
    if (queryFailure != null && queryRan.get())
      assertSame(queryFailure, error.get());
    return new Reply(queryRan.get(), response.get(), error.get(), (Map<String, Object>) contextData.get(STREAM_INFO_CTX_KEY));
  }

  private static GetFeaturesByGeometryEvent spatialEvent(double longitude) {
    return new GetFeaturesByGeometryEvent()
        .withGeometry(new Point().withCoordinates(new PointCoordinates(longitude, 50.0)))
        .withRadius(500);
  }

  /**
   * Renders an exception the same way as the service does for a failed task, and returns all calls on the HTTP response.
   */
  private static List<String> render(Throwable e, int status, String bodyPart) {
    RoutingContext context = routingContext(new HashMap<>());
    when(context.request().getHeader("Stream-Id")).thenReturn("test-stream");
    HttpServerResponse response = mock(HttpServerResponse.class, RETURNS_SELF);
    when(context.response()).thenReturn(response);

    new ErrorRenderer().render(context, e);

    List<String> calls = mockingDetails(response).getInvocations().stream()
        .map(invocation -> invocation.getMethod().getName() + Arrays.toString(invocation.getArguments()))
        .toList();
    assertTrue(calls.stream().anyMatch(call -> call.startsWith("setStatusCode[" + status + "]")));
    assertTrue(calls.stream().anyMatch(call -> call.startsWith("end[") && call.contains(bodyPart)));
    return calls;
  }

  private static RoutingContext routingContext(Map<String, Object> data) {
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

  private record Reply(boolean queryRan, XyzResponse response, Throwable error, Map<String, Object> streamInfo) {

    Object streamInfo(String key) {
      return streamInfo == null ? null : streamInfo.get(key);
    }
  }

  private static class ErrorRenderer extends Api {

    void render(RoutingContext context, Throwable e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Redis on the test clock.
   */
  private class FakeRedis implements CacheClient {

    private final Map<String, byte[]> values = new HashMap<>();
    private final Map<String, Long> expiries = new HashMap<>();
    private final Map<String, Long> ttls = new HashMap<>();
    private int calls;

    @Override
    public Future<byte[]> get(String key) {
      calls++;
      return Future.succeededFuture(liveValues().get(key));
    }

    @Override
    public void set(String key, byte[] value, long ttl) {
      calls++;
      values.put(key, value);
      expiries.put(key, now.get() + SECONDS.toMillis(ttl));
      ttls.put(key, ttl);
    }

    @Override
    public void remove(String key) {
      calls++;
      values.remove(key);
      expiries.remove(key);
    }

    @Override
    public void shutdown() {}

    int size() {
      return liveValues().size();
    }

    /**
     * @return The count of the only marker
     */
    int markerCount() {
      assertEquals(1, size());
      String[] parts = new String(values.values().iterator().next()).split(":");
      //A 513 marker starts with the kind of the 513
      return Integer.parseInt(parts[parts.length - 2]);
    }

    /**
     * @return The TTL in seconds the only marker was set with
     */
    long markerTtl() {
      assertEquals(1, size());
      return ttls.get(values.keySet().iterator().next());
    }

    private Map<String, byte[]> liveValues() {
      expiries.entrySet().removeIf(expiry -> {
        boolean expired = expiry.getValue() <= now.get();
        if (expired)
          values.remove(expiry.getKey());
        return expired;
      });
      return values;
    }
  }
}
