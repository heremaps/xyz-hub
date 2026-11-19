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

import static com.here.xyz.hub.task.IcebergReadTestSupport.BASE_ID;
import static com.here.xyz.hub.task.IcebergReadTestSupport.SPACE_CREATED_AT;
import static com.here.xyz.hub.task.IcebergReadTestSupport.SPACE_ID;
import static com.here.xyz.hub.task.IcebergReadTestSupport.composite;
import static com.here.xyz.hub.task.IcebergReadTestSupport.momTypeQuery;
import static com.here.xyz.hub.task.IcebergReadTestSupport.routingContext;
import static com.here.xyz.hub.task.IcebergReadTestSupport.space;
import static com.here.xyz.hub.task.IcebergShadowComparisonTest.fc;
import static com.here.xyz.hub.task.IcebergShadowComparisonTest.feature;
import static com.here.xyz.util.service.BaseHttpServerVerticle.STREAM_INFO_CTX_KEY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.DataReferenceConfigClient;
import com.here.xyz.hub.config.IcebergVersionCache;
import com.here.xyz.hub.config.IcebergVersionCacheTestSupport;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.FeatureTask.BBoxQuery;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.FeatureTask.IterateQuery;
import com.here.xyz.hub.task.FeatureTask.SearchQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import com.here.xyz.hub.task.IcebergShadowComparison.Outcome;
import com.here.xyz.hub.task.IcebergShadowComparison.Result;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import com.here.xyz.util.service.rest.Api;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs reads through the routing steps around a stand-in for invoke, which answers with a response of the space's own connector,
 * with a stub for the Iceberg connector.
 */
class IcebergReadRoutingTest {

  private static final long VERSION = 3;
  private static final String LOCATION = new IcebergLocation("ns", "table", "v" + VERSION).toString();
  private static Vertx createdVertx;

  private Config previousConfiguration;
  private Space space;
  private Connector icebergConnector;
  private Future<Connector> resolvedConnector;
  private Future<List<DataReference>> loadResult;
  private final AtomicInteger loads = new AtomicInteger();
  private final List<Event<?>> icebergEvents = new CopyOnWriteArrayList<>();
  private final AtomicBoolean rpcCancelled = new AtomicBoolean();
  private final FeatureCollection postgresResponse = fc(feature("a"), feature("b"));
  private FeatureCollection icebergResponse = fc(feature("b"), feature("a"));
  /**
   * The answer of the Iceberg connector stub, null for no answer.
   */
  private Supplier<Future<XyzResponse>> icebergAnswer = () -> Future.succeededFuture(icebergResponse);
  private final CompletableFuture<Shadow> shadow = new CompletableFuture<>();

  @BeforeAll
  static void startVertx() {
    if (Core.vertx == null)
      Core.vertx = createdVertx = Vertx.vertx();
  }

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

  @AfterAll
  static void stopVertx() {
    if (createdVertx != null) {
      createdVertx.close();
      Core.vertx = null;
    }
  }

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
    Service.configuration.ICEBERG_READS = "ON";
    space = space(SPACE_ID);
    icebergConnector = new Connector();
    icebergConnector.id = "iceberg";
    resolvedConnector = Future.succeededFuture(icebergConnector);
    loadResult = Future.succeededFuture(List.of(ref((int) VERSION, "v" + VERSION)));

    DataReferenceConfigClient references = mock(DataReferenceConfigClient.class);
    when(references.load(anyString(), isNull(), isNull(), isNull(), isNull(), isNull(), eq(IcebergVersionCache.TARGET_SYSTEM)))
        .thenAnswer(invocation -> {
          loads.incrementAndGet();
          return loadResult;
        });
    IcebergReadRouting.setVersionCache(IcebergVersionCacheTestSupport.create(references));
    IcebergReadRouting.setConnectorResolver(task -> resolvedConnector);
    IcebergReadRouting.setInvoker((task, connector, event, handler) -> {
      icebergEvents.add(event);
      Future<XyzResponse> answer = icebergAnswer.get();
      if (answer != null)
        answer.onComplete(handler);
      return new RpcContext(connector) {
        @Override
        public void cancelRequest() {
          rpcCancelled.set(true);
        }
      };
    });
    IcebergReadRouting.setShadowListener((result, comparison) -> shadow.complete(new Shadow(result, comparison)));
  }

  @AfterEach
  void tearDown() {
    IcebergReadRouting.resetTestSeams();
    Service.configuration = previousConfiguration;
  }

  @Test
  void offIsNotRouted() {
    Service.configuration.ICEBERG_READS = "OFF";
    assertNotRouted(send(bboxTask(bboxEvent())));
    Service.configuration.ICEBERG_READS = "unknown";
    assertNotRouted(send(bboxTask(bboxEvent())));
    //SHADOW is only a space value
    Service.configuration.ICEBERG_READS = "SHADOW";
    Reply reply = send(bboxTask(bboxEvent()));
    assertNotRouted(reply);
    assertThat(reply.streamInfo("ISH")).isNull();
    //The Iceberg versions are not looked up
    assertThat(loads).hasValue(0);
  }

  @Test
  void theSpaceValueOverridesTheGlobalSetting() throws Exception {
    Service.configuration.ICEBERG_READS = "OFF";
    space.setIcebergReads("ON");
    assertRouted(send(bboxTask(bboxEvent())));

    Service.configuration.ICEBERG_READS = "ON";
    space.setIcebergReads("OFF");
    icebergEvents.clear();
    assertNotRouted(send(bboxTask(bboxEvent())));
    assertThat(loads).hasValue(1);

    Service.configuration.ICEBERG_READS = "OFF";
    space.setIcebergReads("SHADOW");
    assertShadowed(send(bboxTask(bboxEvent())));
    assertThat(shadow.get(5, TimeUnit.SECONDS).result).isEqualTo("MATCH");
  }

  @Test
  void aCompositeUsesItsOwnValue() {
    Service.configuration.ICEBERG_READS = "OFF";
    loadResult = Future.succeededFuture(List.of(ref(7, "ext_t_b5_v7")));
    Space base = space(BASE_ID);
    base.setIcebergReads("ON");
    space = composite(5L);
    assertNotRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(base))));
    assertThat(loads).hasValue(0);

    Service.configuration.ICEBERG_READS = "ON";
    base.setIcebergReads("OFF");
    space.setIcebergReads("ON");
    assertRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(base))));

    base.setIcebergReads("ON");
    space.setIcebergReads("OFF");
    icebergEvents.clear();
    assertNotRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(base))));
  }

  @Test
  void onRoutesEachReadTypeOfAnExplicitVersion() {
    List<Function<Request, FeatureTask<?, ?>>> tasks = List.of(
        request -> bboxTask(request, bboxEvent()),
        request -> geometryTask(request, new GetFeaturesByGeometryEvent().withRadius(10)
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8.0, 50.0))).withRef(new Ref(VERSION))),
        request -> tileTask(request, tileEvent(ResponseType.GEO_JSON)),
        request -> iterateTask(request, iterateEvent(100, null, new Ref(VERSION))),
        request -> searchTask(request, searchEvent(momTypeQuery("a"))));
    for (Function<Request, FeatureTask<?, ?>> task : tasks) {
      icebergEvents.clear();
      Reply reply = send(task);
      assertRouted(reply);
      Event<?> icebergEvent = icebergEvents.get(0);
      assertThat(icebergEvent.getClass()).isEqualTo(reply.task.getEvent().getClass());
      assertThat(icebergEvent.getParams()).containsEntry(IcebergLocation.EVENT_PARAM,
          Map.of(IcebergLocation.EVENT_PARAM_LOCATION, LOCATION));
      //The original event is not changed
      assertThat(reply.task.getEvent().getParams() == null ? Map.of() : reply.task.getEvent().getParams())
          .doesNotContainKey(IcebergLocation.EVENT_PARAM);
    }
    assertThat(loads).hasValue(1);
  }

  @Test
  void theHeadOfAReadOnlySpaceIsRouted() {
    space.setReadOnly(true);
    space.setReadOnlyHeadVersion(VERSION);
    assertRouted(send(bboxTask(bboxEvent().withRef(new Ref(Ref.HEAD)))));
    assertThat(((GetFeaturesByBBoxEvent<?>) icebergEvents.get(0)).getRef().getVersion()).isEqualTo(VERSION);
  }

  @Test
  void theHeadOfAWritableSpaceIsNotRouted() {
    assertNotRouted(send(bboxTask(bboxEvent().withRef(new Ref(Ref.HEAD)))));
    assertThat(loads).hasValue(0);
  }

  @Test
  void aPinnedCompositeIsRoutedToTheTagOfItsBaseVersion() {
    space = composite(5L);
    loadResult = Future.succeededFuture(List.of(ref(7, "ext_t_b4_v7"), ref(7, "ext_t_b5_v7"), ref(7, "ext_t_b6_v7")));
    Reply reply = send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(space(BASE_ID))));
    assertRouted(reply);
    assertThat(icebergEvents.get(0).getParams()).containsEntry(IcebergLocation.EVENT_PARAM,
        Map.of(IcebergLocation.EVENT_PARAM_LOCATION, new IcebergLocation("ns", "table", "ext_t_b5_v7").toString()));
  }

  @Test
  void aPinnedCompositeWithoutTheTagOfItsBaseVersionIsNotRouted() {
    space = composite(5L);
    loadResult = Future.succeededFuture(List.of(ref(7, "ext_t_b4_v7")));
    assertNotRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(space(BASE_ID)))));
    assertThat(loads).hasValue(1);
  }

  @Test
  void anUnpinnedCompositeOnAReadOnlyBaseIsNotRouted() {
    space = composite(null);
    Space base = space(BASE_ID);
    base.setReadOnly(true);
    base.setReadOnlyHeadVersion(4);
    loadResult = Future.succeededFuture(List.of(ref(7, "ext_t_b4_v7")));
    assertNotRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(base))));
    assertThat(loads).hasValue(0);
  }

  @Test
  void aCompositeOfCompositesIsNotRouted() {
    space = composite(5L);
    loadResult = Future.succeededFuture(List.of(ref(7, "ext_t_b5_v7")));
    assertNotRouted(send(compositeTask(bboxEvent().withRef(new Ref(7)), List.of(space(BASE_ID), space("base-of-base")))));
    assertThat(loads).hasValue(0);
  }

  @Test
  void anIneligibleReadIsNotRouted() {
    assertNotRouted(send(bboxTask(bboxEvent().withClusteringType("hexbin"))));
    assertNotRouted(send(bboxTask(bboxEvent().withSelection(List.of("id")))));
    assertThat(loads).hasValue(0);
  }

  @Test
  void propertyQueriesAreRoutedForTheConnectorsFilterProperties() {
    PropertiesQuery kind = IcebergReadTestSupport.equalsQuery(List.of(List.<Object[]>of(new Object[]{"kind", "a"})));
    assertNotRouted(send(searchTask(new Request(), searchEvent(kind))));
    assertThat(loads).hasValue(0);
    icebergConnector.params = new HashMap<>(Map.of("filterColumns", Map.of("kind", "kind")));
    assertRouted(send(searchTask(new Request(), searchEvent(kind))));
    icebergEvents.clear();
    assertNotRouted(send(searchTask(new Request(), searchEvent(momTypeQuery("a")))));
  }

  @Test
  void aCacheHitIsNotRouted() {
    FeatureCollection cached = fc(feature("c"));
    Request request = new Request();
    request.cached = cached;
    Reply reply = send(bboxTask(request, bboxEvent()));
    assertThat(reply.invokeRan).isFalse();
    assertThat(reply.response).isSameAs(cached);
    assertThat(icebergEvents).isEmpty();
  }

  @Test
  void aVersionWithoutReferenceIsNotRouted() {
    loadResult = Future.succeededFuture(List.of(ref(2, "v2")));
    assertNotRouted(send(bboxTask(bboxEvent())));
  }

  @Test
  void aFailureToLoadTheVersionsFallsBack() {
    loadResult = Future.failedFuture(new RuntimeException("Database not reachable"));
    Reply reply = send(bboxTask(bboxEvent()));
    assertNotRouted(reply);
    assertThat(reply.streamInfo("IFB")).isNull();
  }

  @Test
  void aMissingOrInactiveConnectorIsNotRouted() {
    resolvedConnector = Future.failedFuture(new RuntimeException("Connector not found"));
    assertNotRouted(send(bboxTask(bboxEvent())));
    resolvedConnector = Future.succeededFuture(null);
    assertNotRouted(send(bboxTask(bboxEvent())));
    icebergConnector.active = false;
    resolvedConnector = Future.succeededFuture(icebergConnector);
    assertNotRouted(send(bboxTask(bboxEvent())));
  }

  @Test
  void anIcebergErrorFallsBack() {
    icebergAnswer = () -> Future.failedFuture(new HttpException(BAD_REQUEST, "Not implemented"));
    Reply reply = send(bboxTask(bboxEvent()));
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("IFB")).isEqualTo(1);
    assertThat(icebergEvents).hasSize(1);
  }

  /**
   * A read whose Iceberg response is too large (e.g. because of a feature of more than 100 MB) is answered from the space's own
   * connector, whether the Iceberg connector reports PAYLOAD_TO_LARGE or the Hub's own response size check fails.
   */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"connector", "hub"})
  void aTooLargeIcebergResponseFallsBack(String limit) {
    HttpException failure = "connector".equals(limit) ? new DetailedHttpException("E318541")
        : new HttpException(Api.RESPONSE_PAYLOAD_TOO_LARGE, Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
    assertThat(failure.status.code()).isEqualTo(513);
    icebergAnswer = () -> Future.failedFuture(failure);
    Reply reply = send(bboxTask(bboxEvent()));
    assertThat(reply.error).isNull();
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("IFB")).isEqualTo(1);
    assertThat(icebergEvents).hasSize(1);
  }

  @Test
  void anIcebergTimeoutFallsBackAndCancelsTheRead() {
    Service.configuration.ICEBERG_READS_TIMEOUT_MS = 50;
    icebergAnswer = () -> null;
    Reply reply = send(bboxTask(bboxEvent()));
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("IFB")).isEqualTo(1);
    assertThat(rpcCancelled).isTrue();
  }

  @Test
  void anIcebergTokenIsRoutedEvenIfOff() {
    Service.configuration.ICEBERG_READS = "OFF";
    assertRouted(send(iterateTask(new Request(), icebergPage())));
    assertThat(((IterateFeaturesEvent<?>) icebergEvents.get(0)).getNextPageToken()).isEqualTo("ice:42:0:2");
  }

  @Test
  void anIcebergTokenDoesNotFallBack() {
    HttpException failure = new HttpException(BAD_REQUEST, "Invalid token");
    icebergAnswer = () -> Future.failedFuture(failure);
    Reply reply = send(iterateTask(new Request(), icebergPage()));
    assertThat(reply.invokeRan).isFalse();
    assertThat(reply.error).isSameAs(failure);
  }

  @Test
  void anIcebergTokenWithoutReferenceIsRejected() {
    loadResult = Future.succeededFuture(List.of());
    Reply reply = send(iterateTask(new Request(), icebergPage()));
    assertThat(reply.invokeRan).isFalse();
    assertThat(reply.error).isInstanceOf(HttpException.class);
    assertThat(((HttpException) reply.error).status).isEqualTo(BAD_REQUEST);
    assertThat(reply.error).hasMessageContaining("no longer valid");
    assertThat(icebergEvents).isEmpty();
  }

  @Test
  void anIcebergTokenOnTheHeadOfAWritableSpaceIsRejected() {
    Reply reply = send(iterateTask(new Request(), iterateEvent(100, "ice:42:0:2", new Ref(Ref.HEAD))));
    assertThat(reply.invokeRan).isFalse();
    assertThat(((HttpException) reply.error).status).isEqualTo(BAD_REQUEST);
    assertThat(icebergEvents).isEmpty();
  }

  @Test
  void aTokenOfTheSpacesConnectorIsNotRouted() {
    assertNotRouted(send(iterateTask(new Request(), iterateEvent(100, "abc", new Ref(VERSION)))));
    assertThat(loads).hasValue(0);
  }

  @Test
  void shadowServesThePrimaryAndReportsAMatch() throws Exception {
    space.setIcebergReads("SHADOW");
    Reply reply = send(bboxTask(bboxEvent()));
    assertShadowed(reply);
    Shadow result = shadow.get(5, TimeUnit.SECONDS);
    assertThat(result.result).isEqualTo("MATCH");
    assertThat(result.comparison.primaryCount()).isEqualTo(2);
  }

  @Test
  void shadowReportsAMismatch() throws Exception {
    space.setIcebergReads("SHADOW");
    icebergResponse = fc(feature("a"), "{\"id\":\"b\",\"type\":\"Feature\",\"properties\":{\"name\":\"other\"}}", feature("c"));
    assertShadowed(send(bboxTask(bboxEvent())));
    Shadow result = shadow.get(5, TimeUnit.SECONDS);
    assertThat(result.result).isEqualTo("MISMATCH");
    assertThat(result.comparison.differing()).containsExactly("b");
    assertThat(result.comparison.extra()).containsExactly("c");
  }

  @Test
  void shadowReportsIncomparableResponses() throws Exception {
    space.setIcebergReads("SHADOW");
    assertShadowed(send(iterateTask(new Request(), iterateEvent(2, null, new Ref(VERSION)))));
    Shadow result = shadow.get(5, TimeUnit.SECONDS);
    assertThat(result.result).isEqualTo("INCOMPARABLE");
    assertThat(result.comparison.outcome()).isEqualTo(Outcome.INCOMPARABLE);
  }

  @Test
  void shadowReportsIcebergErrors() throws Exception {
    space.setIcebergReads("SHADOW");
    icebergAnswer = () -> Future.failedFuture(new RuntimeException("Iceberg failure"));
    assertShadowed(send(bboxTask(bboxEvent())));
    assertThat(shadow.get(5, TimeUnit.SECONDS).result).isEqualTo("ERROR");
  }

  @Test
  void shadowSkipsResponsesLargerThanTheMaximum() throws Exception {
    space.setIcebergReads("SHADOW");
    Service.configuration.ICEBERG_READS_SHADOW_MAX_BYTES = 10;
    assertShadowed(send(bboxTask(bboxEvent())));
    Shadow result = shadow.get(5, TimeUnit.SECONDS);
    assertThat(result.result).isEqualTo("SKIPPED");
    assertThat(result.comparison.reason()).isEqualTo("too large");
  }

  @Test
  void shadowSkipsTheComparisonWhenTheQueueIsFull() throws Exception {
    space.setIcebergReads("SHADOW");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch running = new CountDownLatch(2);
    CompletableFuture<String> skipped = new CompletableFuture<>();
    List<String> results = new CopyOnWriteArrayList<>();
    IcebergReadRouting.setShadowListener((result, comparison) -> {
      results.add(result);
      if ("SKIPPED".equals(result)) {
        skipped.complete(result);
        return;
      }
      running.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      //Both comparison threads are blocked, 16 comparisons wait in the queue, the next one is skipped
      for (int i = 0; i < 2; i++) {
        icebergEvents.clear();
        assertShadowed(send(bboxTask(bboxEvent())));
      }
      assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();
      for (int i = 0; i < 16; i++) {
        icebergEvents.clear();
        assertShadowed(send(bboxTask(bboxEvent())));
      }
      assertThat(skipped).isNotDone();
      icebergEvents.clear();
      assertShadowed(send(bboxTask(bboxEvent())));
      assertThat(skipped.get(5, TimeUnit.SECONDS)).isEqualTo("SKIPPED");
    }
    finally {
      release.countDown();
    }
  }

  @Test
  void shadowDropsTooLargeResponsesBeforeTheComparisonQueue() throws Exception {
    space.setIcebergReads("SHADOW");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch running = new CountDownLatch(2);
    CompletableFuture<Result> tooLarge = new CompletableFuture<>();
    IcebergReadRouting.setShadowListener((result, comparison) -> {
      if ("SKIPPED".equals(result)) {
        if (comparison != null)
          tooLarge.complete(comparison);
        return;
      }
      running.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      //Both comparison threads are blocked and the queue is full
      for (int i = 0; i < 2; i++) {
        icebergEvents.clear();
        assertShadowed(send(bboxTask(bboxEvent())));
      }
      assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();
      for (int i = 0; i < 16; i++) {
        icebergEvents.clear();
        assertShadowed(send(bboxTask(bboxEvent())));
      }
      //A too large pair is skipped without a comparison thread
      Service.configuration.ICEBERG_READS_SHADOW_MAX_BYTES = 10;
      icebergEvents.clear();
      assertShadowed(send(bboxTask(bboxEvent())));
      assertThat(tooLarge.get(5, TimeUnit.SECONDS).reason()).isEqualTo(IcebergShadowComparison.TOO_LARGE);
    }
    finally {
      release.countDown();
    }
  }

  @Test
  void aMissingConnectorIsRememberedForAShortTime() {
    AtomicInteger lookups = new AtomicInteger();
    IcebergReadRouting.setConnectorResolver(task -> {
      lookups.incrementAndGet();
      return resolvedConnector;
    });
    resolvedConnector = Future.failedFuture(new RuntimeException("Connector not found"));
    assertNotRouted(send(bboxTask(bboxEvent())));
    assertNotRouted(send(bboxTask(bboxEvent())));
    Reply page = send(iterateTask(new Request(), icebergPage()));
    assertThat(((HttpException) page.error).status).isEqualTo(BAD_REQUEST);
    space.setIcebergReads("SHADOW");
    assertNotRouted(send(bboxTask(bboxEvent())));
    assertThat(lookups).hasValue(1);
    assertThat(loads).hasValue(0);

    //Another connector ID is looked up
    space.setIcebergReads(null);
    Service.configuration.ICEBERG_READS_CONNECTOR_ID = "other-iceberg";
    resolvedConnector = Future.succeededFuture(icebergConnector);
    assertRouted(send(bboxTask(bboxEvent())));
    assertThat(lookups).hasValue(2);
  }

  @Test
  void shadowOnlyRunsForTheSample() {
    space.setIcebergReads("SHADOW");
    Service.configuration.ICEBERG_READS_SHADOW_SAMPLE = 0;
    Reply reply = send(bboxTask(bboxEvent()));
    assertNotRouted(reply);
    assertThat(reply.streamInfo("ISH")).isNull();
  }

  @Test
  void shadowDoesNotWaitForTheVersionLookup() throws Exception {
    space.setIcebergReads("SHADOW");
    Promise<List<DataReference>> pendingLoad = Promise.promise();
    loadResult = pendingLoad.future();
    Reply reply = send(bboxTask(bboxEvent()));
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("ISH")).isEqualTo(1);
    assertThat(icebergEvents).isEmpty();

    pendingLoad.complete(List.of(ref((int) VERSION, "v" + VERSION)));
    assertThat(shadow.get(5, TimeUnit.SECONDS).result).isEqualTo("MATCH");
    assertThat(icebergEvents).hasSize(1);
    //The shadow read asks the Iceberg connector for a response no larger than the comparison can take
    assertThat(icebergEvents.get(0).getParams()).containsEntry(IcebergLocation.EVENT_PARAM,
        Map.of(IcebergLocation.EVENT_PARAM_LOCATION, LOCATION, IcebergLocation.EVENT_PARAM_MAX_RESPONSE_BYTES,
            (long) Service.configuration.ICEBERG_READS_SHADOW_MAX_BYTES));
  }

  @Test
  void shadowWithoutAnIcebergVersionFoundLaterIsAbandoned() throws Exception {
    space.setIcebergReads("SHADOW");
    Promise<List<DataReference>> pendingLoad = Promise.promise();
    loadResult = pendingLoad.future();
    assertThat(send(bboxTask(bboxEvent())).invokeRan).isTrue();
    pendingLoad.complete(List.of());
    assertThat(icebergEvents).isEmpty();
    assertThat(shadow).isNotDone();
  }

  @Test
  void shadowComparesAnIcebergReadWhichFinishesAfterThePrimary() throws Exception {
    space.setIcebergReads("SHADOW");
    Promise<XyzResponse> pendingIceberg = Promise.promise();
    icebergAnswer = pendingIceberg::future;
    assertShadowed(send(bboxTask(bboxEvent())));
    assertThat(shadow).isNotDone();
    pendingIceberg.complete(icebergResponse);
    assertThat(shadow.get(5, TimeUnit.SECONDS).result).isEqualTo("MATCH");
  }

  @Test
  void shadowReportsAnIcebergTimeout() throws Exception {
    space.setIcebergReads("SHADOW");
    Service.configuration.ICEBERG_READS_TIMEOUT_MS = 50;
    icebergAnswer = () -> null;
    assertShadowed(send(bboxTask(bboxEvent())));
    assertThat(shadow.get(5, TimeUnit.SECONDS).result).isEqualTo("ERROR");
    assertThat(rpcCancelled).isTrue();
  }

  @Test
  void aReadOfAnInactiveConnectorIsLeftToInvoke() {
    Request request = new Request();
    BBoxQuery task = bboxTask(request, bboxEvent());
    task.storage = new Connector();
    task.storage.active = false;
    assertNotRouted(send(task));

    IterateQuery page = iterateTask(new Request(), icebergPage());
    page.storage = task.storage;
    assertNotRouted(send(page));
    assertThat(loads).hasValue(0);
  }

  @Test
  void anIcebergTokenWithoutTheIcebergConnectorIsRejected() {
    Service.configuration.ICEBERG_READS = "OFF";
    resolvedConnector = Future.failedFuture(new RuntimeException("Connector not found"));
    Reply reply = send(iterateTask(new Request(), icebergPage()));
    assertThat(reply.invokeRan).isFalse();
    assertThat(((HttpException) reply.error).status).isEqualTo(BAD_REQUEST);
    assertThat(icebergEvents).isEmpty();
  }

  private void assertRouted(Reply reply) {
    assertThat(reply.error).isNull();
    assertThat(reply.invokeRan).isFalse();
    assertThat(reply.response).isSameAs(icebergResponse);
    assertThat(reply.streamInfo("SID")).isEqualTo("iceberg");
    assertThat(reply.streamInfo("ITime")).isNotNull();
    assertThat(reply.streamInfo("IFB")).isNull();
    assertThat(icebergEvents).isNotEmpty();
  }

  private void assertNotRouted(Reply reply) {
    assertThat(reply.error).isNull();
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("SID")).isNull();
    assertThat(icebergEvents).isEmpty();
  }

  private void assertShadowed(Reply reply) {
    assertThat(reply.error).isNull();
    assertThat(reply.invokeRan).isTrue();
    assertThat(reply.response).isSameAs(postgresResponse);
    assertThat(reply.streamInfo("ISH")).isEqualTo(1);
    assertThat(reply.streamInfo("SID")).isNull();
    assertThat(icebergEvents).hasSize(1);
  }

  private static DataReference ref(int endVersion, String tag) {
    return new DataReference()
        .withId(UUID.randomUUID())
        .withEntityId(SPACE_ID)
        .withStartVersion(0)
        .withEndVersion(endVersion)
        .withTargetSystem(IcebergVersionCache.TARGET_SYSTEM)
        .withLocation(new IcebergLocation("ns", "table", tag).toString())
        .withCreatedAt(SPACE_CREATED_AT + 1);
  }

  private static GetFeaturesByBBoxEvent<?> bboxEvent() {
    GetFeaturesByBBoxEvent<?> event = new GetFeaturesByBBoxEvent<>();
    event.setBbox(new BBox(8.0, 50.0, 8.1, 50.1));
    event.setRef(new Ref(VERSION));
    return event;
  }

  private static IterateFeaturesEvent<?> iterateEvent(long limit, String nextPageToken, Ref ref) {
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>();
    event.setLimit(limit);
    event.setNextPageToken(nextPageToken);
    event.setRef(ref);
    return event;
  }

  private static SearchForFeaturesEvent<?> searchEvent(PropertiesQuery propertiesQuery) {
    SearchForFeaturesEvent<?> event = new SearchForFeaturesEvent<>();
    event.setPropertiesQuery(propertiesQuery);
    event.setRef(new Ref(VERSION));
    return event;
  }

  private static GetFeaturesByTileEvent tileEvent(ResponseType responseType) {
    GetFeaturesByTileEvent event = new GetFeaturesByTileEvent().withLevel(12).withX(2138).withY(1388).withResponseType(responseType)
        .withRef(new Ref(VERSION));
    event.setBbox(new BBox(8.0, 50.0, 8.1, 50.1));
    return event;
  }

  private static IterateFeaturesEvent<?> icebergPage() {
    return iterateEvent(100, "ice:42:0:2", new Ref(VERSION));
  }

  /**
   * The data of one request.
   */
  private static final class Request {

    final Map<String, Object> contextData = new HashMap<>();
    final AtomicBoolean invokeRan = new AtomicBoolean();
    XyzResponse cached;
  }

  private record Reply(FeatureTask<?, ?> task, boolean invokeRan, XyzResponse response, Throwable error,
      Map<String, Object> streamInfo) {

    Object streamInfo(String key) {
      return streamInfo == null ? null : streamInfo.get(key);
    }
  }

  private record Shadow(String result, Result comparison) {}

  private Reply send(Function<Request, FeatureTask<?, ?>> taskFactory) {
    return send(taskFactory.apply(new Request()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Reply send(FeatureTask<?, ?> task) {
    Request data = requests.remove(task);
    CompletableFuture<Object> outcome = new CompletableFuture<>();
    ((FeatureTask) task).execute(t -> outcome.complete(((FeatureTask) t).getResponse()), (t, e) -> outcome.complete(e));
    Object result;
    try {
      result = outcome.get(5, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new AssertionError("The task did not complete", e);
    }
    return new Reply(task, data.invokeRan.get(), result instanceof XyzResponse response ? response : null,
        result instanceof Throwable error ? error : null, (Map<String, Object>) data.contextData.get(STREAM_INFO_CTX_KEY));
  }

  //The request data of each task, by task
  private final Map<FeatureTask<?, ?>, Request> requests = new HashMap<>();

  private BBoxQuery bboxTask(GetFeaturesByBBoxEvent<?> event) {
    return bboxTask(new Request(), event);
  }

  private BBoxQuery compositeTask(GetFeaturesByBBoxEvent<?> event, List<Space> extendedSpaces) {
    BBoxQuery task = bboxTask(event);
    task.extendedSpaces = new ArrayList<>(extendedSpaces);
    return task;
  }

  private <X extends FeatureTask<?, X>> X prepare(X task, Request request) {
    task.space = space;
    requests.put(task, request);
    return task;
  }

  private BBoxQuery bboxTask(Request request, GetFeaturesByBBoxEvent<?> event) {
    return prepare(new BBoxQuery(event, routingContext(request.contextData, SPACE_ID), ApiResponseType.FEATURE_COLLECTION, false) {
      @Override
      public TaskPipeline<BBoxQuery> createPipeline() {
        return IcebergReadRoutingTest.this.<BBoxQuery>pipeline(this, request);
      }
    }, request);
  }

  private GeometryQuery geometryTask(Request request, GetFeaturesByGeometryEvent event) {
    return prepare(new GeometryQuery(event, routingContext(request.contextData, SPACE_ID), ApiResponseType.FEATURE_COLLECTION, false,
        null, null) {
      @Override
      public TaskPipeline<GeometryQuery> createPipeline() {
        return IcebergReadRoutingTest.this.<GeometryQuery>pipeline(this, request);
      }
    }, request);
  }

  private TileQuery tileTask(Request request, GetFeaturesByTileEvent event) {
    ApiResponseType responseType = event.getResponseType() == ResponseType.MVT ? ApiResponseType.MVT : ApiResponseType.FEATURE_COLLECTION;
    return prepare(new TileQuery(event, routingContext(request.contextData, SPACE_ID), responseType, false) {
      @Override
      public TaskPipeline<TileQuery> createPipeline() {
        return IcebergReadRoutingTest.this.<TileQuery>pipeline(this, request);
      }
    }, request);
  }

  private IterateQuery iterateTask(Request request, IterateFeaturesEvent<?> event) {
    return prepare(new IterateQuery(event, routingContext(request.contextData, SPACE_ID), ApiResponseType.FEATURE_COLLECTION, false) {
      @Override
      public TaskPipeline<IterateQuery> createPipeline() {
        return IcebergReadRoutingTest.this.<IterateQuery>pipeline(this, request);
      }
    }, request);
  }

  private SearchQuery searchTask(Request request, SearchForFeaturesEvent<?> event) {
    return prepare(new SearchQuery(event, routingContext(request.contextData, SPACE_ID), ApiResponseType.FEATURE_COLLECTION, false) {
      @Override
      public TaskPipeline<SearchQuery> createPipeline() {
        return IcebergReadRoutingTest.this.<SearchQuery>pipeline(this, request);
      }
    }, request);
  }

  /**
   * checkImmutability, a cache stand-in, the routing, a stand-in for invoke and the shadow step.
   */
  private <X extends FeatureTask<?, X>> TaskPipeline<X> pipeline(X task, Request request) {
    return TaskPipeline.create(task)
        .then(FeatureTaskHandler::checkImmutability)
        .then((t, callback) -> {
          if (request.cached != null)
            t.setResponse(request.cached);
          callback.call(t);
        })
        .then(IcebergReadRouting::route)
        .then((t, callback) -> {
          if (t.getResponse() != null) {
            callback.call(t);
            return;
          }
          t.consumeEvent();
          request.invokeRan.set(true);
          t.setResponse(postgresResponse);
          callback.call(t);
        })
        .then(IcebergReadRouting::recordPrimary);
  }
}
