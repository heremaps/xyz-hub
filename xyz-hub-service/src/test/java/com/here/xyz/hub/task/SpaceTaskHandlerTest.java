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

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.models.hub.FeatureModificationList.IfExists.ERROR;
import static com.here.xyz.models.hub.FeatureModificationList.IfExists.PATCH;
import static com.here.xyz.models.hub.FeatureModificationList.IfNotExists.CREATE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpaceTaskHandlerTest {

  private static final String LAZY_TABLE_CREATION = "lazyTableCreation";
  private static final String PSQL_CONNECTOR_CLASS_NAME = "com.here.xyz.psql.PSQLXyzConnector";
  private static final String NL_CONNECTOR_CLASS_NAME = "com.here.xyz.psql.NLConnector";

  private Config previousConfiguration;
  private ConnectorConfigClient previousConnectorConfigClient;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    previousConnectorConfigClient = Service.connectorConfigClient;
    Service.configuration = new Config();
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
    Service.connectorConfigClient = previousConnectorConfigClient;
  }

  @Test
  void keepsLegacyResolutionWhenUniqueTableNamesAreDisabled() {
    Service.configuration.ENABLE_UNIQUE_PHYSICAL_TABLE_NAMES = false;
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "my-space"))), CREATE, ERROR, true, false);
    Space result = new Space();
    result.setId("my-space");
    result.setStorage(new ConnectorRef().withId("psql"));
    modifyOp.entries.get(0).result = result;
    ConditionalOperation task = new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false);

    SpaceTaskHandler.assignTableName(task, successfulCallback());

    assertNull(modifyOp.entries.get(0).result.getStorage().getParams());
  }

  @Test
  void preservesThePersistedTableNameOnUpdate() {
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "my-space"))), CREATE, PATCH, true, false);
    Space head = spaceWithParams(Map.of(TABLE_NAME, "persisted_table"));
    Space result = spaceWithParams(Map.of(TABLE_NAME, "replacement_table", "otherParam", "value"));
    modifyOp.entries.get(0).head = head;
    modifyOp.entries.get(0).result = result;
    ConditionalOperation task = new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false);

    SpaceTaskHandler.assignTableName(task, successfulCallback());

    assertEquals("persisted_table", result.getStorage().getParams().get(TABLE_NAME));
    assertEquals("value", result.getStorage().getParams().get("otherParam"));
  }

  @Test
  void doesNotAllowAddingATableNameToALegacySpaceOnUpdate() {
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "legacy-space"))), CREATE, PATCH, true, false);
    Space head = spaceWithParams(null);
    Space result = spaceWithParams(Map.of(TABLE_NAME, "replacement_table", "otherParam", "value"));
    modifyOp.entries.get(0).head = head;
    modifyOp.entries.get(0).result = result;

    SpaceTaskHandler.assignTableName(
        new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false), successfulCallback());

    assertFalse(result.getStorage().getParams().containsKey(TABLE_NAME));
    assertEquals("value", result.getStorage().getParams().get("otherParam"));
  }

  @Test
  void setsTheLazyTableFlagForAGeneratedTableNameOnALazyConnector() {
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of(LAZY_TABLE_CREATION, true)));
    Space result = createSpace(null);

    assertTrue(Space.isLazyTable(result.getStorage().getParams()));
    assertNotNull(result.getStorage().getParams().get(TABLE_NAME));
  }

  @Test
  void setsNoLazyTableFlagWithoutTheConnectorParam() {
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of()));
    Space result = createSpace(null);
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of(LAZY_TABLE_CREATION, false)));
    Space resultWithFalse = createSpace(null);

    assertFalse(result.getStorage().getParams().containsKey(LAZY_TABLE));
    assertNotNull(result.getStorage().getParams().get(TABLE_NAME));
    assertFalse(resultWithFalse.getStorage().getParams().containsKey(LAZY_TABLE));
  }

  @Test
  void setsNoLazyTableFlagForTheNewTableLayout() {
    Map<String, Object> lazyParams = Map.of(LAZY_TABLE_CREATION, true);
    Map<String, Object> newLayoutParams = Map.of(LAZY_TABLE_CREATION, true, "tableLayout", "NEW_LAYOUT");
    Map<String, Object> classNameParams = Map.of(LAZY_TABLE_CREATION, true, "className", NL_CONNECTOR_CLASS_NAME);

    for (Connector connector : List.of(
        embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, newLayoutParams),
        embeddedConnector(NL_CONNECTOR_CLASS_NAME, lazyParams),
        lambdaConnector(NL_CONNECTOR_CLASS_NAME, lazyParams),
        httpConnector(classNameParams))) {
      useConnector(connector);
      Space result = createSpace(null);

      assertFalse(result.getStorage().getParams().containsKey(LAZY_TABLE), "connector " + connector.id);
      assertNotNull(result.getStorage().getParams().get(TABLE_NAME));
    }
  }

  @Test
  void theClassNameOfTheRemoteFunctionTakesPrecedenceOverTheParam() {
    Map<String, Object> params = Map.of(LAZY_TABLE_CREATION, true, "className", NL_CONNECTOR_CLASS_NAME);

    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, params));
    assertTrue(Space.isLazyTable(createSpace(null).getStorage().getParams()));
    useConnector(lambdaConnector(PSQL_CONNECTOR_CLASS_NAME, params));
    assertTrue(Space.isLazyTable(createSpace(null).getStorage().getParams()));
  }

  @Test
  void effectiveClassNameFollowsThePrecedenceOfTheConnectorParams() {
    Map<String, Object> params = Map.of("className", "com.example.FromParams");

    assertEquals(PSQL_CONNECTOR_CLASS_NAME, embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, params).getEffectiveClassName());
    assertEquals(PSQL_CONNECTOR_CLASS_NAME, lambdaConnector(PSQL_CONNECTOR_CLASS_NAME, params).getEffectiveClassName());
    assertNull(lambdaConnector(null, params).getEffectiveClassName());
    assertEquals("com.example.FromParams", httpConnector(params).getEffectiveClassName());
    assertNull(httpConnector(Map.of()).getEffectiveClassName());
    Connector withoutParams = httpConnector(Map.of());
    withoutParams.params = null;
    assertNull(withoutParams.getEffectiveClassName());
  }

  @Test
  void setsNoLazyTableFlagForAClientSuppliedTableName() {
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of(LAZY_TABLE_CREATION, true)));
    Space result = createSpace(Map.of(TABLE_NAME, "client_table", LAZY_TABLE, true));

    assertEquals(Map.of(TABLE_NAME, "client_table"), result.getStorage().getParams());
  }

  @Test
  void removesAClientSuppliedLazyTableFlag() {
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of()));
    Space result = createSpace(Map.of(LAZY_TABLE, true, "otherParam", "value"));

    assertFalse(result.getStorage().getParams().containsKey(LAZY_TABLE));
    assertEquals("value", result.getStorage().getParams().get("otherParam"));
  }

  @Test
  void removesAClientSuppliedLazyTableFlagAlsoWithUniqueTableNamesDisabled() {
    Service.configuration.ENABLE_UNIQUE_PHYSICAL_TABLE_NAMES = false;
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of(LAZY_TABLE_CREATION, true)));
    Space result = createSpace(Map.of(LAZY_TABLE, true));
    Space resultWithOtherParam = createSpace(Map.of(LAZY_TABLE, true, "otherParam", "value"));

    //Without a generated table name a space is never lazy
    assertNull(result.getStorage().getParams());
    assertEquals(Map.of("otherParam", "value"), resultWithOtherParam.getStorage().getParams());
  }

  @Test
  void rejectsAReservedTableName() {
    useConnector(embeddedConnector(PSQL_CONNECTOR_CLASS_NAME, Map.of(LAZY_TABLE_CREATION, true)));
    ConditionalOperation task = createTask(Map.of(TABLE_NAME, LazySpaceTables.RESERVED_PREFIX + "v1"));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> SpaceTaskHandler.assignTableName(task, successfulCallback()));
    assertTrue(e.getMessage().contains("reserved prefix"));
  }

  @Test
  void setsNoLazyTableFlagAndContinuesIfTheConnectorCannotBeLoaded() {
    ConnectorConfigClient client = mock(ConnectorConfigClient.class);
    doAnswer(invocation -> {
      Handler<AsyncResult<Connector>> handler = invocation.getArgument(2);
      handler.handle(Future.failedFuture(new RuntimeException("Connector not found")));
      return null;
    }).when(client).get(any(), anyString(), any(Handler.class));
    Service.connectorConfigClient = client;

    //The authorization after this step answers for a storage the user may not use, so the pipeline must reach it
    Space result = createSpace(null);

    assertFalse(result.getStorage().getParams().containsKey(LAZY_TABLE));
    assertNotNull(result.getStorage().getParams().get(TABLE_NAME));
  }

  @Test
  void keepsTheLazyTableFlagOfALegacySpaceOnUpdates() {
    Space head = spaceWithParams(Map.of(LAZY_TABLE, true));
    for (Space result : List.of(spaceWithParams(null), new Space(), spaceWithParams(Map.of(LAZY_TABLE, false)))) {
      ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "my-space"))), CREATE, PATCH, true, false);
      modifyOp.entries.get(0).head = head;
      modifyOp.entries.get(0).result = result;

      SpaceTaskHandler.assignTableName(
          new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false), successfulCallback());

      assertEquals(Map.of(LAZY_TABLE, true), result.getStorage().getParams());
    }
  }

  @Test
  void logsClientErrorsDuringDependencyResolutionAsWarning() {
    assertEquals(Level.WARN, SpaceTaskHandler.dependencyResolutionFailureLevel(new HttpException(NOT_FOUND, "Layer not found")));
    assertEquals(Level.WARN, SpaceTaskHandler.dependencyResolutionFailureLevel(new HttpException(BAD_REQUEST, "Invalid")));
    assertEquals(Level.ERROR, SpaceTaskHandler.dependencyResolutionFailureLevel(new HttpException(GATEWAY_TIMEOUT, "Timeout")));
    assertEquals(Level.ERROR, SpaceTaskHandler.dependencyResolutionFailureLevel(new RuntimeException("Unexpected")));
  }

  private static ConditionalOperation createTask(Map<String, Object> params) {
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "my-space"))), CREATE, ERROR, true, false);
    modifyOp.entries.get(0).result = spaceWithParams(params);
    return new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false);
  }

  private static Space createSpace(Map<String, Object> params) {
    ConditionalOperation task = createTask(params);
    AtomicBoolean called = new AtomicBoolean();
    SpaceTaskHandler.assignTableName(task, new Callback<>() {
      @Override
      public void exception(Throwable e) {
        throw new AssertionError(e);
      }

      @Override
      public void call(ConditionalOperation value) {
        called.set(true);
      }
    });
    assertTrue(called.get(), "The pipeline must continue");
    return task.modifyOp.entries.get(0).result;
  }

  private static void useConnector(Connector connector) {
    ConnectorConfigClient client = mock(ConnectorConfigClient.class);
    doAnswer(invocation -> {
      Handler<AsyncResult<Connector>> handler = invocation.getArgument(2);
      handler.handle(Future.succeededFuture(connector));
      return null;
    }).when(client).get(any(), eq("psql"), any(Handler.class));
    Service.connectorConfigClient = client;
  }

  private static Connector connector(String id, RemoteFunctionConfig remoteFunction, Map<String, Object> params) {
    Connector connector = new Connector();
    connector.id = id;
    connector.params = new HashMap<>(params);
    connector.setRemoteFunction(remoteFunction);
    return connector;
  }

  private static Connector embeddedConnector(String className, Map<String, Object> params) {
    Embedded embedded = new Embedded();
    embedded.id = "embedded";
    embedded.className = className;
    return connector("embedded-" + className, embedded, params);
  }

  private static Connector lambdaConnector(String className, Map<String, Object> params) {
    AWSLambda lambda = new AWSLambda();
    lambda.id = "lambda";
    lambda.className = className;
    return connector("lambda-" + className, lambda, params);
  }

  private static Connector httpConnector(Map<String, Object> params) {
    Http http = new Http();
    http.id = "http";
    try {
      http.url = URI.create("http://localhost/connector").toURL();
    }
    catch (MalformedURLException e) {
      throw new IllegalStateException(e);
    }
    return connector("http", http, params);
  }

  private static Space spaceWithParams(Map<String, Object> params) {
    Space space = new Space();
    space.setId("my-space");
    space.setStorage(new ConnectorRef().withId("psql").withParams(params));
    return space;
  }

  private static Callback<ConditionalOperation> successfulCallback() {
    return new Callback<>() {
      @Override
      public void exception(Throwable e) {
        throw new AssertionError(e);
      }

      @Override
      public void call(ConditionalOperation value) {
      }
    };
  }

  private static RoutingContext routingContext() {
    RoutingContext context = mock(RoutingContext.class);
    HttpServerRequest request = mock(HttpServerRequest.class);
    JWTPayload jwt = new JWTPayload();
    jwt.aid = "test-aid";
    when(context.request()).thenReturn(request);
    when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(context.get("jwt")).thenReturn(jwt);
    when(context.put(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(context);
    return context;
  }
}