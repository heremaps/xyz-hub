/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.health.HealthApi;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Node represents one running Service Node of the XYZ Hub Service.
 */
public class Node {

  private static final Logger logger = LogManager.getLogger();

  public static final Node OWN_INSTANCE = new Node(Service.HOST_ID, Service.getHostname(),
      Service.configuration != null ? Service.configuration.ADMIN_MESSAGE_PORT : -1);
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
    Service.webClient.get(getUrl().getPort() == -1 ? DEFAULT_PORT : getUrl().getPort(), url.getHost(), HealthApi.MAIN_HEALTCHECK_ENDPOINT)
        .timeout(TimeUnit.SECONDS.toMillis(5))
        .send(ar -> {
          if (ar.succeeded()) {
            HttpResponse<Buffer> response = ar.result();
            if (onlyAliveCheck || response.statusCode() == 200) {
              callback.handle(Future.succeededFuture());
            } else {
              callback.handle(Future.failedFuture("Node with ID " + id + " and IP " + ip + " is not healthy."));
            }
          } else {
            callback.handle(Future.failedFuture("Node with ID " + id + " and IP " + ip + " is not reachable."));
          }
        });
  }

  @JsonIgnore
  public URL getUrl() {
    try {
      url = new URL("http", ip, port, "");
    } catch (MalformedURLException e) {
      logger.error("Unable to create the URL for the local node.", e);
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
}
