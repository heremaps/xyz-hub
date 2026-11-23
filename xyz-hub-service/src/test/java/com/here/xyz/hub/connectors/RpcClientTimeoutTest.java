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

import static com.here.xyz.util.service.rest.Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks how the connector's timeout and "too large" responses are mapped, using the real response parsing of {@link RpcClient} on a
 * client whose only stubbed method is {@link RpcClient#getConnector()}. The protected size checks only run where a test asks for them.
 */
class RpcClientTimeoutTest {

  private static final Marker MARKER = MarkerManager.getMarker(RpcClientTimeoutTest.class.getSimpleName());
  private static Config previousConfiguration;
  private RpcClient rpcClient;

  @BeforeAll
  static void setUpAll() {
    try {
      ErrorManager.loadErrors("hub-errors.json");
    }
    catch (RuntimeException e) {
      //Another test class in the same JVM loaded them already
      if (!(e.getCause() instanceof IllegalStateException) || !e.getCause().getMessage().contains("already registered"))
        throw e;
    }
    previousConfiguration = Service.configuration;
    //RpcClient reads the configuration when its class is initialized
    Service.configuration = new Config();
  }

  @AfterAll
  static void tearDownAll() {
    Service.configuration = previousConfiguration;
  }

  @BeforeEach
  void setUp() {
    Connector connector = new Connector();
    connector.id = "test-connector";
    connector.setRemoteFunction(new Embedded());
    rpcClient = mock(RpcClient.class);
    when(rpcClient.getConnector()).thenReturn(connector);
  }

  @Test
  void theDetailsOfAPayloadTooLargeAreKeptButNotReplied() throws Exception {
    Map<String, Object> details = Map.of("limit", "maxResponseBytes", "maxBytes", 10);
    Throwable e = parse(new ErrorResponse().withError(XyzError.PAYLOAD_TO_LARGE).withErrorMessage("The response would be too large")
        .withErrorDetails(details).serialize());

    PayloadTooLargeException tooLarge = assertInstanceOf(PayloadTooLargeException.class, e);
    assertEquals(details, tooLarge.connectorDetails);
    //The client gets the same 513 as for any other connector
    assertEquals(513, tooLarge.status.code());
    assertEquals("E318541", tooLarge.errorDefinition.getCode());
    assertTrue(tooLarge.errorDetails == null && tooLarge.placeholders.isEmpty());
  }

  @Test
  void aStatementTimeoutGetsItsOwnType() throws Exception {
    Throwable e = parse(new ErrorResponse().withError(XyzError.TIMEOUT).withErrorMessage(StatementTimeoutException.CONNECTOR_MESSAGE)
        .serialize());

    assertInstanceOf(StatementTimeoutException.class, e);
    assertTimeoutReply(e);
  }

  @Test
  void noTimeLeftForARetryIsNotAStatementTimeout() throws Exception {
    Throwable e = parse(new ErrorResponse().withError(XyzError.TIMEOUT).withErrorMessage("No time for retry left for database query.")
        .serialize());

    assertFalse(e instanceof StatementTimeoutException);
    assertTimeoutReply(e);
  }

  @Test
  void aLambdaTimeoutIsNotAStatementTimeout() throws Exception {
    Throwable e = parse("{\"errorType\":\"Sandbox.Timedout\",\"errorMessage\":\"RequestId: 1 Error: Task timed out after 30.00 seconds\"}");

    assertFalse(e instanceof StatementTimeoutException);
    assertTimeoutReply(e);
  }

  @Test
  void theConnectorsPayloadTooLargeIsE318541() throws Exception {
    Throwable e = parse(new ErrorResponse().withError(XyzError.PAYLOAD_TO_LARGE)
        .withErrorMessage("Maximum response char limit of 10 reached").serialize());

    //The query timeout cache replies a remembered one as new DetailedHttpException("E318541")
    DetailedHttpException tooLarge = assertInstanceOf(DetailedHttpException.class, e);
    assertEquals(513, tooLarge.status.code());
    assertEquals("E318541", tooLarge.errorDefinition.getCode());
    assertNull(tooLarge.errorDetails);
  }

  @Test
  void theServicesOwnSizeLimitIsAPlain513() throws Exception {
    long previousLimit = Service.configuration.MAX_UNCOMPRESSED_RESPONSE_SIZE;
    Service.configuration.MAX_UNCOMPRESSED_RESPONSE_SIZE = 10;
    try {
      doCallRealMethod().when(rpcClient).checkUncompressedResponseSize(any(), any());
      doCallRealMethod().when(rpcClient).throwResponseSizeException(any());
      Throwable e = parse("{\"type\":\"FeatureCollection\",\"features\":[]}");

      //The query timeout cache replies a remembered one as a new HttpException with the same status and message
      assertSame(HttpException.class, e.getClass());
      HttpException tooLarge = (HttpException) e;
      assertEquals(513, tooLarge.status.code());
      assertEquals(RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE, tooLarge.getMessage());
      assertNull(tooLarge.errorDetails);
    }
    finally {
      Service.configuration.MAX_UNCOMPRESSED_RESPONSE_SIZE = previousLimit;
    }
  }

  private static void assertTimeoutReply(Throwable e) {
    DetailedHttpException timeout = assertInstanceOf(DetailedHttpException.class, e);
    assertEquals(504, timeout.status.code());
    assertEquals("E318540", timeout.errorDefinition.getCode());
  }

  private Throwable parse(String payload) throws Exception {
    Method parseResponse = RpcClient.class.getDeclaredMethod("parseResponse", Marker.class, byte[].class, boolean.class, Handler.class);
    parseResponse.setAccessible(true);
    AtomicReference<AsyncResult<XyzResponse>> result = new AtomicReference<>();
    parseResponse.invoke(rpcClient, MARKER, payload.getBytes(UTF_8), false, (Handler<AsyncResult<XyzResponse>>) result::set);
    assertNotNull(result.get());
    assertTrue(result.get().failed());
    return result.get().cause();
  }
}
