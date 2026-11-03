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

package com.here.xyz.hub.connectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RemoteFunctionPreparationTest {

  private static final long LONG_TIMEOUT_MS = 60_000;

  private static Config previousConfiguration;
  private static Vertx vertx;

  @BeforeAll
  static void setUpAll() {
    previousConfiguration = Service.configuration;
    //RpcClient reads the configuration when its class is initialized
    Service.configuration = new Config();
    vertx = Vertx.vertx();
  }

  @AfterAll
  static void tearDownAll() {
    vertx.close();
    Service.configuration = previousConfiguration;
  }

  @Test
  void selectsTheActiveAwsLambdaConnectorsOnly() {
    Connector lambda = connector(new AWSLambda());
    Connector inactiveLambda = connector(new AWSLambda());
    inactiveLambda.active = false;
    //The last client was destroyed, so it has no connector anymore
    List<RpcClient> clients = List.of(client(connector(new Embedded())), client(lambda), client(inactiveLambda),
        client(connector(new Http())), client(null));

    assertEquals(List.of(lambda), RemoteFunctionPreparation.select(clients));
  }

  @Test
  void completesWhenAllConnectorsAnswered() {
    List<Connector> connectors = List.of(connector(new AWSLambda()), connector(new AWSLambda()), connector(new AWSLambda()));
    Map<String, Handler<AsyncResult<XyzResponse>>> sent = new ConcurrentHashMap<>();

    Future<Void> preparation = RemoteFunctionPreparation.prepare(connectors, (connector, handler) -> sent.put(connector.id, handler),
        LONG_TIMEOUT_MS, vertx);
    //All events are sent before any of the connectors answered
    assertEquals(connectors.stream().map(connector -> connector.id).collect(Collectors.toSet()), sent.keySet());
    assertFalse(preparation.isComplete());

    connectors.forEach(connector -> sent.get(connector.id).handle(Future.succeededFuture(new HealthStatus())));
    assertTrue(preparation.succeeded(), "The preparation did not complete with the last answer");
  }

  @Test
  void completesAtTheTimeoutWhenAConnectorDoesNotAnswer() throws Exception {
    Connector answering = connector(new AWSLambda());
    Connector silent = connector(new AWSLambda());
    Map<String, Handler<AsyncResult<XyzResponse>>> sent = new ConcurrentHashMap<>();
    long timeoutMs = 500;

    long start = System.nanoTime();
    Future<Void> preparation = RemoteFunctionPreparation.prepare(List.of(answering, silent),
        (connector, handler) -> sent.put(connector.id, handler), timeoutMs, vertx);
    sent.get(answering.id).handle(Future.succeededFuture(new HealthStatus()));
    assertFalse(preparation.isComplete());

    preparation.toCompletionStage().toCompletableFuture().get(10, SECONDS);
    assertTrue(preparation.succeeded());
    long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMs >= timeoutMs, "Completed after " + elapsedMs + " ms, before the timeout");

    //A late answer is ignored
    sent.get(silent.id).handle(Future.succeededFuture(new HealthStatus()));
    assertTrue(preparation.succeeded());
  }

  @Test
  void completesWhenConnectorsFail() {
    Connector failing = connector(new AWSLambda());
    Connector throwing = connector(new AWSLambda());
    Connector answering = connector(new AWSLambda());

    Future<Void> preparation = RemoteFunctionPreparation.prepare(List.of(failing, throwing, answering), (connector, handler) -> {
      if (connector == throwing)
        throw new IllegalStateException("No RpcClient is ready for the given connector with ID " + connector.id);
      handler.handle(connector == failing ? Future.failedFuture(new HttpException(BAD_GATEWAY, "Connector error."))
          : Future.succeededFuture(new HealthStatus()));
    }, LONG_TIMEOUT_MS, vertx);

    assertTrue(preparation.succeeded(), "The preparation did not complete with the last answer");
  }

  private static Connector connector(RemoteFunctionConfig remoteFunction) {
    Connector connector = new Connector();
    connector.id = RemoteFunctionPreparationTest.class.getSimpleName() + "_" + UUID.randomUUID();
    connector.setRemoteFunction(remoteFunction);
    return connector;
  }

  private static RpcClient client(Connector connector) {
    RpcClient client = mock(RpcClient.class);
    when(client.getConnector()).thenReturn(connector);
    return client;
  }
}
