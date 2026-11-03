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

import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.Core;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

/**
 * Sends one {@link HealthCheckEvent} to each active AWS Lambda connector when the service starts, before it serves requests.
 * The event carries the connector's own settings and goes to the Lambda function version this service resolves for its own build version.
 * So the connector version that will serve this node's requests initializes itself against its database first, e.g., it checks and, if
 * needed, installs its SQL scripts there.
 * The preparation never fails the startup. A connector that fails or does not answer in time is logged, and its first request does the
 * initialization instead.
 */
public class RemoteFunctionPreparation {

  private static final Logger logger = LogManager.getLogger();
  static final String STREAM_ID_PREFIX = "PREP-";

  private RemoteFunctionPreparation() {}

  /**
   * Prepares the connectors of all existing RPC clients, see {@link #select(Collection)}.
   *
   * @param vertx The Vert.x instance to run the timeout on
   * @return A future that succeeds once all connectors answered or {@link com.here.xyz.hub.Config#PREPARE_REMOTE_FUNCTIONS_TIMEOUT_MS}
   *  passed. It never fails.
   */
  public static Future<Void> prepareAll(Vertx vertx) {
    try {
      return prepare(select(RpcClient.getAllInstances()), RemoteFunctionPreparation::sendHealthCheck,
          Service.configuration.PREPARE_REMOTE_FUNCTIONS_TIMEOUT_MS, vertx);
    }
    catch (Exception e) {
      logger.warn("Unable to prepare the remote functions.", e);
      return Future.succeededFuture();
    }
  }

  /**
   * @param clients The RPC clients to select from
   * @return The active connectors whose remote function is an AWS Lambda function. Embedded and HTTP connectors are not selected.
   */
  static List<Connector> select(Collection<RpcClient> clients) {
    return clients.stream()
        .map(RpcClient::getConnector)
        .filter(connector -> connector != null && connector.active && connector.getRemoteFunction() instanceof AWSLambda)
        .toList();
  }

  /**
   * Sends one event to each of the given connectors at the same time.
   *
   * @param connectors The connectors to prepare
   * @param sender Sends the event to one connector and calls the handler with its answer
   * @param timeoutMs The maximum time to wait for all answers
   * @param vertx The Vert.x instance to run the timeout on
   * @return A future that succeeds once all connectors answered or the timeout passed. It never fails.
   */
  static Future<Void> prepare(List<Connector> connectors, BiConsumer<Connector, Handler<AsyncResult<XyzResponse>>> sender,
      long timeoutMs, Vertx vertx) {
    Preparation preparation = new Preparation(connectors, timeoutMs);
    if (connectors.isEmpty()) {
      preparation.finish();
      return preparation.promise.future();
    }

    long timerId = vertx.setTimer(timeoutMs, t -> preparation.finish());
    preparation.promise.future().onComplete(ar -> vertx.cancelTimer(timerId));
    for (Connector connector : connectors) {
      try {
        sender.accept(connector, ar -> preparation.onAnswer(connector, ar));
      }
      catch (Exception e) {
        preparation.onAnswer(connector, Future.failedFuture(e));
      }
    }
    return preparation.promise.future();
  }

  private static void sendHealthCheck(Connector connector, Handler<AsyncResult<XyzResponse>> handler) {
    HealthCheckEvent healthCheck = new HealthCheckEvent();
    //Just generate a stream ID here as the stream actually "begins" here
    String streamId = STREAM_ID_PREFIX + UUID.randomUUID();
    healthCheck.setStreamId(streamId);
    RpcClient.getInstanceFor(connector).execute(new Log4jMarker(streamId), healthCheck, true, handler);
  }

  private static class Preparation {

    private final Promise<Void> promise = Promise.promise();
    private final long start = Core.currentTimeMillis();
    private final long timeoutMs;
    private final Map<String, Connector> pending = new LinkedHashMap<>();
    private final List<String> failed = new ArrayList<>();
    private final int total;
    private boolean finished;

    private Preparation(List<Connector> connectors, long timeoutMs) {
      this.timeoutMs = timeoutMs;
      connectors.forEach(connector -> pending.put(connector.id, connector));
      total = pending.size();
    }

    private void onAnswer(Connector connector, AsyncResult<XyzResponse> answer) {
      boolean last;
      synchronized (this) {
        //An answer after the timeout or a second answer is ignored
        if (finished || pending.remove(connector.id) == null)
          return;
        if (answer.failed())
          failed.add(connector.id);
        last = pending.isEmpty();
      }

      if (answer.failed())
        logger.warn("Preparing connector {} failed after {} ms: {}", connector.id, Core.currentTimeMillis() - start, answer.cause());
      if (last)
        finish();
    }

    private void finish() {
      List<String> missing;
      synchronized (this) {
        if (finished)
          return;
        finished = true;
        missing = new ArrayList<>(pending.keySet());
      }

      missing.forEach(connectorId -> logger.warn("Connector {} did not answer the preparation within {} ms.", connectorId, timeoutMs));
      logger.info("Prepared {} of {} AWS Lambda connectors in {} ms. Failed: {}. No answer within {} ms: {}.",
          total - failed.size() - missing.size(), total, Core.currentTimeMillis() - start, failed, timeoutMs, missing);
      promise.complete();
    }
  }
}
