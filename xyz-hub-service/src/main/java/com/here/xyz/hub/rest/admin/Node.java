/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.rest.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.messages.brokers.RedisMessageBroker;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.util.health.Config;
import com.here.xyz.hub.util.health.schema.Response;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Node represents one running Service Node of the XYZ Hub Service.
 */
public class Node {

  private static final int HEALTH_TIMEOUT = 25;
  private static final Logger logger = LogManager.getLogger();

  private static int nodeCount = Service.configuration.INSTANCE_COUNT;
  private static final int NODE_COUNT_FETCH_PERIOD = 30_000; //ms
  private static final int CLUSTER_NODES_CHECKER_PERIOD = 120_000; //ms
  private static final int CLUSTER_NODES_PING_PERIOD = 600_000; //ms

  public static final Node OWN_INSTANCE = new Node(Service.HOST_ID, Service.getHostname(),
      Service.configuration != null ? Service.getPublicPort() : -1);
  private static final Set<Node> otherClusterNodes = new CopyOnWriteArraySet<>();
  private static final int DEFAULT_PORT = 80;
  private static final String UNKNOWN_ID = "UNKNOWN";
  public String id;
  public String ip;
  public int port;
  @JsonIgnore
  private URL url;

  private Node(@JsonProperty("id") String id, @JsonProperty("ip") String ip, @JsonProperty("port") int port) {
    this.id = id;
    this.ip = ip;
    this.port = port;
  }

  public static void initialize() {
    startNodeInfoBroadcast();
    initNodeCountFetcher();
    initNodeChecker();
  }

  private static void startNodeInfoBroadcast() {
    new NodeInfoNotification().broadcast();
    if (Core.vertx != null) Core.vertx.setPeriodic(CLUSTER_NODES_PING_PERIOD, timerId -> new NodeInfoNotification().broadcast());
  }

  private static void initNodeCountFetcher() {
    if (Core.vertx != null) {
      Core.vertx.setPeriodic(NODE_COUNT_FETCH_PERIOD, timerId -> RedisMessageBroker.getInstance().fetchSubscriberCount(r -> {
        if (r.succeeded()) {
          nodeCount = r.result();
          logger.debug("Service node-count: " + nodeCount);
        }
        else
          logger.warn("Checking service node-count failed.", r.cause());
      }));
    }
  }

  private static void initNodeChecker() {
    if (Service.vertx != null) {
      Service.vertx.setPeriodic(CLUSTER_NODES_CHECKER_PERIOD, timerId -> otherClusterNodes.forEach(otherNode -> otherNode.isHealthy(ar -> {
        if (ar.failed()) otherClusterNodes.remove(otherNode);
      })));
    }
  }

  public static Node forIpAndPort(String ip, int port) {
    return new Node(UNKNOWN_ID, ip, port);
  }

  public void isAlive(Handler<AsyncResult<Void>> callback) {
    callHealthCheck(true, callback);
  }

  public void isHealthy(Handler<AsyncResult<Void>> callback) {
    callHealthCheck(false, callback);
  }

  private void callHealthCheck(boolean onlyAliveCheck, Handler<AsyncResult<Void>> callback) {
    Service.webClient.get(port, ip, HealthApi.MAIN_HEALTCHECK_ENDPOINT)
        .timeout(TimeUnit.SECONDS.toMillis(HEALTH_TIMEOUT))
        .putHeader(Config.getHealthCheckHeaderName(), Config.getHealthCheckHeaderValue())
        .send(ar -> {
          if (ar.succeeded()) {
            HttpResponse<Buffer> response = ar.result();
            if (onlyAliveCheck || response.statusCode() == 200) {
              Response r = response.bodyAsJson(Response.class);
              if (id.equals(r.getNode()))
                callback.handle(Future.succeededFuture());
              else
                callback.handle(Future.failedFuture("Node with ID " + id + " and IP " + ip + " is not existing anymore. "
                    + "IP is now used by node with ID " + r.getNode()));
            }
            else {
              callback.handle(Future.failedFuture("Node with ID " + id + " and IP " + ip + " is not healthy."));
            }
          }
          else {
            callback.handle(Future.failedFuture("Node with ID " + id + " and IP " + ip + " is not reachable."));
          }
        });
  }

  @JsonIgnore
  public URL getUrl() {
    try {
      url = new URL("http", ip, port, "");
    }
    catch (MalformedURLException e) {
      logger.error("Unable to create the URL for the node with id " + id + ".", e);
    }
    return url;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Node node = (Node) o;
    return UNKNOWN_ID.equals(node.id) ? false : id.equals(node.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public static int count() {
    return Math.max(nodeCount, 1);
  }

  public static Set<Node> getClusterNodes() {
    Set<Node> clusterNodes = new HashSet<>(otherClusterNodes);
    clusterNodes.add(OWN_INSTANCE);
    return clusterNodes;
  }

  private static class NodeInfoNotification extends AdminMessage {

    public boolean isResponse = false;

    @Override
    protected void handle() {
      otherClusterNodes.add(source);
      if (!isResponse) {
        NodeInfoNotification response = new NodeInfoNotification();
        response.isResponse = true;
        //Send a notification back to the sender
        response.send(source);
      }
    }
  }
}
