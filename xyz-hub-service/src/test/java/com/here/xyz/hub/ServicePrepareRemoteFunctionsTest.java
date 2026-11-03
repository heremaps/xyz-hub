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

package com.here.xyz.hub;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.Marker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Checks the startup step that prepares the remote functions, with {@link Config#PREPARE_REMOTE_FUNCTIONS_ON_STARTUP} off and on.
 */
class ServicePrepareRemoteFunctionsTest {

  private static Config previousConfiguration;
  private static Vertx previousVertx;
  private static Vertx vertx;

  @BeforeAll
  static void setUpAll() {
    previousConfiguration = Service.configuration;
    previousVertx = Core.vertx;
    if (Core.vertx == null)
      Core.vertx = vertx = Vertx.vertx();
  }

  @AfterAll
  static void tearDownAll() {
    Core.vertx = previousVertx;
    if (vertx != null)
      vertx.close();
    Service.configuration = previousConfiguration;
  }

  @BeforeEach
  void setUp() {
    //RpcClient reads the configuration when its class is initialized
    Service.configuration = new Config();
  }

  @Test
  void startupProceedsWithoutPreparationWhenTheFlagIsOff() {
    Service.configuration.PREPARE_REMOTE_FUNCTIONS_ON_STARTUP = false;
    JsonObject config = new JsonObject();

    try (MockedStatic<RpcClient> rpcClients = mockStatic(RpcClient.class)) {
      Future<JsonObject> step = Service.prepareRemoteFunctions(config);
      assertTrue(step.succeeded());
      assertSame(config, step.result());
      rpcClients.verifyNoInteractions();
    }
  }

  @Test
  void startupProceedsAfterThePreparationWhenTheFlagIsOn() throws Exception {
    Service.configuration.PREPARE_REMOTE_FUNCTIONS_ON_STARTUP = true;
    Service.configuration.PREPARE_REMOTE_FUNCTIONS_TIMEOUT_MS = 200;
    Connector connector = new Connector();
    connector.id = ServicePrepareRemoteFunctionsTest.class.getSimpleName() + "_" + UUID.randomUUID();
    connector.setRemoteFunction(new AWSLambda());
    //The client never answers, so the startup proceeds once the timeout passed
    RpcClient client = mock(RpcClient.class);
    when(client.getConnector()).thenReturn(connector);
    JsonObject config = new JsonObject();

    try (MockedStatic<RpcClient> rpcClients = mockStatic(RpcClient.class)) {
      rpcClients.when(RpcClient::getAllInstances).thenReturn(List.of(client));
      rpcClients.when(() -> RpcClient.getInstanceFor(connector)).thenReturn(client);

      JsonObject result = Service.prepareRemoteFunctions(config).toCompletionStage().toCompletableFuture().get(10, SECONDS);
      assertSame(config, result);
    }

    //One health check with priority and a stream ID that marks it as a preparation
    ArgumentCaptor<Event> event = ArgumentCaptor.forClass(Event.class);
    verify(client).execute(any(Marker.class), event.capture(), eq(true), any());
    assertInstanceOf(HealthCheckEvent.class, event.getValue());
    assertTrue(event.getValue().getStreamId().startsWith("PREP-"), "Unexpected stream ID " + event.getValue().getStreamId());
  }
}
