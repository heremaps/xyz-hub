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

package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.health.GroupedHealthCheck;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteFunctionHealthAggregator extends GroupedHealthCheck {

  private static final String RFC_HC_CACHE_KEY = "RFC_HC_RESPONSE";
  private Map<String, RemoteFunctionHealthCheck> checksByConnectorId = new HashMap<>();

  public RemoteFunctionHealthAggregator() {
    setName("Connectors");
    setRole(Role.CUSTOM);
    setTarget(Target.LOCAL);
  }

  private void addRfcHc(Connector connector) {
    RemoteFunctionHealthCheck rfcHc = new RemoteFunctionHealthCheck(connector);
    checksByConnectorId.put(connector.id, rfcHc);
    add(rfcHc);
  }

  private void removeRfcHc(String connectorId) {
    checks.forEach(hc -> {
      RemoteFunctionHealthCheck rfcHc = (RemoteFunctionHealthCheck) hc;
      if (rfcHc.getConnectorId().equals(connectorId)) {
        checksByConnectorId.remove(connectorId);
        remove(rfcHc);
      }
    });
  }

  @Override
  public Status execute() {
    Status s;
    Response res;

    //Check whether some node created an RFC health-check response recently
    Response cachedResponse = fetchResponseFromCache();
    if (cachedResponse != null && cachedResponse.getStatus() != null
        && cachedResponse.getStatus().getTimestamp() != null
        && cachedResponse.getStatus().getTimestamp() > Service.currentTimeMillis() - checkInterval) {
      //Use the cached response as it's still fresh
      res = cachedResponse;
      s = cachedResponse.getStatus();
      //Inject the cached sub-responses to the sub-health-checks
      cachedResponse.getChecks().forEach(c -> {
        RemoteFunctionHealthCheck check = checksByConnectorId.get(c.getName());
        if (check != null)
          check.injectCachedResponse(c.getStatus(), c.getResponse());
      });
    }
    else {
      //There was no cached response or it was too old, so this node will create a new health-check response
      s = super.execute().withTimestamp(Service.currentTimeMillis());
      res = getResponse().withNode(Node.OWN_INSTANCE.id);
      res.setStatus(s);

      try {
        List<Connector> activeConnectors = RemoteFunctionClient.getInstances()
            .stream()
            .map(RemoteFunctionClient::getConnectorConfig)
            .collect(Collectors.toList());

        Set<String> activeConnectorIds = activeConnectors.stream().map(c -> c.id).collect(Collectors.toSet());
        Set<String> toDelete = new HashSet<>();
        Set<Connector> toAdd = new HashSet<>();

        checksByConnectorId.forEach((connectorId, rfcHc) -> {
          if (!activeConnectorIds.contains(connectorId)) {
            toDelete.add(connectorId);
          }
        });

        activeConnectors.forEach(connector -> {
          if (!checksByConnectorId.containsKey(connector.id)) {
            toAdd.add(connector);
          }
        });

        toDelete.forEach(this::removeRfcHc);
        toAdd.forEach(this::addRfcHc);
      }
      catch (Exception e) {
        res = res.withMessage("Error when trying to gather remote functions info: (" + e.getClass().getSimpleName() + ") " + e.getMessage());
        s.setResult(ERROR);
      }
      //Write the new health-check response to the cache
      RedisCacheClient.getInstance().set(RFC_HC_CACHE_KEY, res.toInternalResponseString().getBytes(),
          TimeUnit.MILLISECONDS.toSeconds(checkInterval));
    }

    //Gather further global statistics
    try {
      res.setAdditionalProperty("globalFunctionClientCount", RemoteFunctionClient.getGlobalFunctionClientCount());
      res.setAdditionalProperty("globalMaxQueueByteSize", RemoteFunctionClient.GLOBAL_MAX_QUEUE_BYTE_SIZE);
      res.setAdditionalProperty("globalQueueByteSize", RemoteFunctionClient.getGlobalUsedQueueMemory());
      res.setAdditionalProperty("globalArrivalRate", RemoteFunctionClient.getGlobalArrivalRate());
      res.setAdditionalProperty("globalThroughput", RemoteFunctionClient.getGlobalThroughput());
      res.setAdditionalProperty("globalMinConnections", RemoteFunctionClient.getGlobalMinConnections());
      res.setAdditionalProperty("globalMaxConnections", Service.configuration.REMOTE_FUNCTION_MAX_CONNECTIONS);
      res.setAdditionalProperty("globalUsedConnections", RemoteFunctionClient.getGlobalUsedConnections());
      res.setAdditionalProperty("globalUsedConnectionsPercentage", RemoteFunctionClient.getGlobalUsedConnectionsPercentage());
      setResponse(res);
      return s.withResult(getWorseResult(OK, s.getResult()));
    }
    catch (Exception e) {
      setResponse(res.withMessage("Error when trying to gather global remote functions info: " + e.getMessage()));
      return s.withResult(ERROR);
    }
  }

  private Response fetchResponseFromCache() {
    CompletableFuture<Response> f = new CompletableFuture<>();
    RedisCacheClient.getInstance().get(RFC_HC_CACHE_KEY, r -> {
      Response response = null;

      try {
        if (r != null)
          response = new JsonObject(Buffer.buffer(r)).mapTo(Response.class);
      }
      catch (Exception ignored) {}

      f.complete(response);
    });

    try {
      return f.get();
    }
    catch (ExecutionException | InterruptedException e) {
      return null;
    }
  }

  public RemoteFunctionHealthCheck getRfcHealthCheck(String connectorId) {
    return checksByConnectorId.get(connectorId);
  }
}
