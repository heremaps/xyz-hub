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

import static com.here.xyz.util.service.rest.TooManyRequestsException.ThrottlingReason.QUOTA;
import static com.here.xyz.util.service.rest.TooManyRequestsException.ThrottlingReason.STORAGE_QUEUE_FULL;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RemoteFunctionClient.FunctionCall;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.models.hub.Connector.ConnectionSettings;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.rest.TooManyRequestsException;
import com.here.xyz.util.service.rest.TooManyRequestsException.ThrottlingReason;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Checks that the connection counters of {@link RemoteFunctionClient}, the per-requester counter behind the per-requester limit and the
 * used-connections counter of the client, return to zero once every call has finished, was rejected or was dropped.
 * The clients in this test have no ACU monitor, so the per-requester limit always applies.
 */
class RemoteFunctionClientConnectionCountTest {

  private static final Marker MARKER = MarkerManager.getMarker(RemoteFunctionClientConnectionCountTest.class.getSimpleName());
  //RemoteFunctionClient#getMaxConnections() never goes below 4
  private static final int MAX_CONNECTIONS = 4;
  private static final int HIGH_REQUESTER_LIMIT = 100;
  private static final byte[] EMPTY = new byte[0];

  private static Config previousConfiguration;
  private static Vertx previousVertx;
  private static Vertx vertx;

  private final List<TestClient> clients = new ArrayList<>();

  @BeforeAll
  static void setUpAll() {
    previousConfiguration = Service.configuration;
    Config config = new Config();
    config.HOST_NAME = "localhost";
    config.INSTANCE_COUNT = 1;
    config.GLOBAL_MAX_QUEUE_SIZE = 1; //MB
    config.REMOTE_FUNCTION_MAX_CONNECTIONS = 512;
    config.REMOTE_FUNCTION_CONNECTION_HIGH_UTILIZATION_THRESHOLD = 1;
    Service.configuration = config;

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

  @AfterEach
  void tearDown() {
    clients.forEach(RemoteFunctionClient::destroy);
    clients.clear();
  }

  @Test
  void countersReturnToZeroAfterQueueHandoffsAndQueueDrops() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();
    int queueBytes = queueByteSize(client);

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    Call droppedAsOldest = submit(client, requester, new byte[queueBytes]);
    Call queued1 = submit(client, requester, new byte[1]);
    Call queued2 = submit(client, requester, EMPTY);
    Call droppedAsTooLarge = submit(client, requester, new byte[queueBytes + 1]);
    assertEquals(2, client.getQueueSize());

    //Each of the first two responses hands its connection to a queued call
    running.forEach(client::respond);
    client.respond(queued1);
    client.respond(queued2);

    running.forEach(Call::assertSucceeded);
    queued1.assertSucceeded();
    queued2.assertSucceeded();
    droppedAsOldest.assertRejected(STORAGE_QUEUE_FULL);
    droppedAsTooLarge.assertRejected(STORAGE_QUEUE_FULL);
    assertCountersAreZero(client, requester);
  }

  @Test
  void callHandingItsConnectionToAQueuedCallReleasesItsRequesterConnection() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    List<Call> queued = submit(client, requester, 2);
    assertEquals(MAX_CONNECTIONS, client.getUsedConnections());
    assertEquals(MAX_CONNECTIONS + 2, requesterConnections(client, requester));

    //An error or timeout response hands the connection over the same way as a success
    client.respond(running.get(0), Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Timeout")));
    client.respond(running.get(1));
    assertEquals(MAX_CONNECTIONS, client.getUsedConnections());
    assertEquals(MAX_CONNECTIONS, requesterConnections(client, requester));

    client.respond(running.get(2));
    client.respond(running.get(3));
    queued.forEach(client::respond);
    assertCountersAreZero(client, requester);
  }

  @Test
  void callsDroppedFromAFullQueueReleaseTheirRequesterConnection() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();
    int queueBytes = queueByteSize(client);

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    Call droppedAsOldest = submit(client, requester, new byte[queueBytes]);
    Call queued = submit(client, requester, new byte[1]);
    Call droppedAsTooLarge = submit(client, requester, new byte[queueBytes + 1]);
    droppedAsOldest.assertRejected(STORAGE_QUEUE_FULL);
    droppedAsTooLarge.assertRejected(STORAGE_QUEUE_FULL);
    assertEquals(MAX_CONNECTIONS + 1, requesterConnections(client, requester));

    running.forEach(client::respond);
    client.respond(queued);
    assertCountersAreZero(client, requester);
  }

  @Test
  void callDroppedByAQueueResizeReleasesItsRequesterConnection() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    submit(client, requester, new byte[queueByteSize(client)]);
    assertEquals(1, client.getQueueSize());

    //A new client takes a share of the global queue memory, so the queue of the existing one shrinks and drops the call
    newClient(HIGH_REQUESTER_LIMIT);
    assertEquals(0, client.getQueueSize());
    assertEquals(MAX_CONNECTIONS, requesterConnections(client, requester));

    running.forEach(client::respond);
    assertCountersAreZero(client, requester);
  }

  @Test
  void queuedCallStartedByAFinishedPriorityCallIsCounted() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    Call queued = submit(client, requester, EMPTY);
    Call priority = submit(client, null, EMPTY, true);
    assertTrue(client.isInFlight(priority));
    assertEquals(MAX_CONNECTIONS, client.getUsedConnections());

    client.respond(priority);
    assertTrue(client.isInFlight(queued));
    assertEquals(MAX_CONNECTIONS + 1, client.getUsedConnections());

    running.forEach(client::respond);
    client.respond(queued);
    priority.assertSucceeded();
    assertCountersAreZero(client, requester);
  }

  @Test
  void cancelledQueuedCallIsCountedOnce() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();

    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    Call queued = submit(client, requester, EMPTY);
    queued.fc.cancel();
    //The queued call holds no connection, so cancelling it frees none
    assertEquals(MAX_CONNECTIONS, client.getUsedConnections());
    assertEquals(MAX_CONNECTIONS, requesterConnections(client, requester));

    //The cancelled call stays queued and still gets invoked
    client.respond(running.get(0));
    assertTrue(client.isInFlight(queued));
    client.respond(queued);
    running.subList(1, MAX_CONNECTIONS).forEach(client::respond);
    assertCountersAreZero(client, requester);
  }

  @Test
  void runningCallCancelledBeforeItsResponseArrivesIsCountedOnce() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();
    String otherRequester = newRequesterId();

    Call cancelled = submit(client, requester, EMPTY);
    Call other = submit(client, otherRequester, EMPTY);
    cancelled.fc.cancel();
    assertEquals(1, client.getUsedConnections());
    assertEquals(0, requesterConnections(client, requester));

    //The response of a cancelled call can still arrive, e.g. as an aborted Lambda invocation
    client.respond(cancelled, Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Aborted")));
    assertEquals(1, client.getUsedConnections());
    assertEquals(1, requesterConnections(client, otherRequester));

    client.respond(other);
    assertCountersAreZero(client, requester);
    assertCountersAreZero(client, otherRequester);
  }

  @Test
  void callCancelledAfterItsResponseArrivedIsCountedOnce() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();
    String otherRequester = newRequesterId();

    Call finished = submit(client, requester, EMPTY);
    Call other = submit(client, otherRequester, EMPTY);
    client.respond(finished);
    finished.fc.cancel();
    assertEquals(1, client.getUsedConnections());
    assertEquals(1, requesterConnections(client, otherRequester));

    client.respond(other);
    finished.assertSucceeded();
    assertCountersAreZero(client, requester);
    assertCountersAreZero(client, otherRequester);
  }

  @Test
  void callWhoseInvocationThrowsReleasesItsConnections() {
    TestClient client = newClient(HIGH_REQUESTER_LIMIT);
    String requester = newRequesterId();

    client.invocationFailure = new RejectedExecutionException("Test");
    assertThrows(RejectedExecutionException.class, () -> submit(client, requester, EMPTY));
    assertCountersAreZero(client, requester);

    client.invocationFailure = null;
    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    submit(client, requester, EMPTY);
    client.invocationFailure = new RejectedExecutionException("Test");
    //The queued call fails to start when it takes over the connection
    assertThrows(RejectedExecutionException.class, () -> client.respond(running.get(0)));
    assertEquals(MAX_CONNECTIONS - 1, client.getUsedConnections());
    assertEquals(MAX_CONNECTIONS - 1, requesterConnections(client, requester));

    client.invocationFailure = null;
    running.subList(1, MAX_CONNECTIONS).forEach(client::respond);
    assertCountersAreZero(client, requester);
  }

  @Test
  void requesterAboveTheLimitIsStillRejected() {
    TestClient client = newClient(2);
    String requester = newRequesterId();
    String otherRequester = newRequesterId();

    List<Call> accepted = submit(client, requester, 2);
    Call rejected = submit(client, requester, EMPTY);
    rejected.assertRejected(QUOTA);
    assertEquals(2, requesterConnections(client, requester));

    //Other requesters are not affected
    Call other = submit(client, otherRequester, EMPTY);
    assertTrue(client.isInFlight(other));

    //Once a call finished, the requester may send one more
    client.respond(accepted.get(0));
    Call acceptedLater = submit(client, requester, EMPTY);
    assertTrue(client.isInFlight(acceptedLater));
    submit(client, requester, EMPTY).assertRejected(QUOTA);

    client.respond(accepted.get(1));
    client.respond(acceptedLater);
    client.respond(other);
    assertCountersAreZero(client, requester);
    assertCountersAreZero(client, otherRequester);
  }

  @Test
  void cancelledRejectedCallFreesNothing() {
    TestClient client = newClient(1);
    String requester = newRequesterId();

    Call accepted = submit(client, requester, EMPTY);
    Call rejected = submit(client, requester, EMPTY);
    rejected.assertRejected(QUOTA);
    rejected.fc.cancel();
    assertEquals(1, client.getUsedConnections());
    assertEquals(1, requesterConnections(client, requester));

    client.respond(accepted);
    assertCountersAreZero(client, requester);
  }

  @Test
  void requesterWithNothingInFlightIsNotRejectedAfterQueueHandoffs() {
    TestClient client = newClient(MAX_CONNECTIONS);
    String requester = newRequesterId();
    String otherRequester = newRequesterId();

    //The requester's calls hand their connections to the queued calls of another requester
    List<Call> running = submit(client, requester, MAX_CONNECTIONS);
    List<Call> queued = submit(client, otherRequester, MAX_CONNECTIONS);
    running.forEach(client::respond);
    queued.forEach(client::respond);

    //Nothing is in flight anymore, so the requester may use its full limit again
    List<Call> again = submit(client, requester, MAX_CONNECTIONS);
    for (Call call : again) {
      assertTrue(call.results.isEmpty(), () -> "Call was rejected: " + call.results.get(0).cause());
      assertTrue(client.isInFlight(call));
    }
    again.forEach(client::respond);
    assertCountersAreZero(client, requester);
    assertCountersAreZero(client, otherRequester);
  }

  private TestClient newClient(int maxConnectionsPerRequester) {
    Connector connector = new Connector();
    connector.id = getClass().getSimpleName() + "_" + UUID.randomUUID();
    connector.connectionSettings = new ObjectMapper().convertValue(Map.of("maxConnections", MAX_CONNECTIONS,
        "minConnections", MAX_CONNECTIONS, "maxConnectionsPerRequester", maxConnectionsPerRequester), ConnectionSettings.class);
    TestClient client = new TestClient(connector);
    clients.add(client);
    //A new client only sizes the queues of the clients created before it, so size its own queue as well
    client.setConnectorConfig(connector);
    return client;
  }

  private static String newRequesterId() {
    return "requester-" + UUID.randomUUID();
  }

  private static int queueByteSize(TestClient client) {
    long queueBytes = client.getMaxQueueByteSize();
    assertTrue(queueBytes > 1 && queueBytes < Integer.MAX_VALUE, "Unexpected queue byte size " + queueBytes);
    return (int) queueBytes;
  }

  private static List<Call> submit(TestClient client, String requesterId, int count) {
    List<Call> calls = new ArrayList<>();
    for (int i = 0; i < count; i++)
      calls.add(submit(client, requesterId, EMPTY));
    return calls;
  }

  private static Call submit(TestClient client, String requesterId, byte[] payload) {
    return submit(client, requesterId, payload, false);
  }

  private static Call submit(TestClient client, String requesterId, byte[] payload, boolean hasPriority) {
    Call call = new Call();
    RpcContext context = new RpcContext(client.getConnectorConfig());
    context.setRequesterId(requesterId);
    call.fc = client.submit(MARKER, payload, false, hasPriority, call.results::add, context);
    return call;
  }

  private static int requesterConnections(TestClient client, String requesterId) {
    return client.getUsedConnectionsByRequester().entrySet().stream()
        .filter(entry -> entry.getKey().endsWith("-" + requesterId))
        .mapToInt(entry -> entry.getValue().get())
        .sum();
  }

  private static void assertCountersAreZero(TestClient client, String requesterId) {
    assertEquals(0, client.getQueueSize(), "queued calls");
    assertEquals(0, client.inFlight.size(), "calls in flight");
    assertEquals(0, client.getUsedConnections(), "used connections of the client");
    assertEquals(0, requesterConnections(client, requesterId), "connections of the requester");
  }

  private static class Call {
    private final List<AsyncResult<byte[]>> results = new ArrayList<>();
    private FunctionCall fc;

    private void assertSucceeded() {
      assertEquals(1, results.size(), "responses");
      assertTrue(results.get(0).succeeded(), () -> "Call failed: " + results.get(0).cause());
    }

    private void assertRejected(ThrottlingReason reason) {
      assertEquals(1, results.size(), "responses");
      assertTrue(results.get(0).failed(), "Call succeeded");
      TooManyRequestsException e = assertInstanceOf(TooManyRequestsException.class, results.get(0).cause());
      assertEquals(reason, e.reason);
    }
  }

  /**
   * Keeps every invoked call in flight until the test responds to it.
   */
  private static class TestClient extends RemoteFunctionClient {
    private final Map<FunctionCall, Handler<AsyncResult<byte[]>>> inFlight = new LinkedHashMap<>();
    private RuntimeException invocationFailure;

    TestClient(Connector connector) {
      super(connector);
    }

    @Override
    protected void invoke(FunctionCall fc, Handler<AsyncResult<byte[]>> callback) {
      if (invocationFailure != null)
        throw invocationFailure;
      inFlight.put(fc, callback);
    }

    private boolean isInFlight(Call call) {
      return inFlight.containsKey(call.fc);
    }

    private void respond(Call call) {
      respond(call, Future.succeededFuture(EMPTY));
    }

    private void respond(Call call, AsyncResult<byte[]> response) {
      Handler<AsyncResult<byte[]>> callback = inFlight.remove(call.fc);
      assertNotNull(callback, "The call is not in flight");
      callback.handle(response);
    }
  }
}
