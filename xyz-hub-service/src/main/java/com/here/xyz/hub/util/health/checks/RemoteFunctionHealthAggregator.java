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
import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.util.health.GroupedHealthCheck;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoteFunctionHealthAggregator extends GroupedHealthCheck {

  private Set<String> connectorIds = new HashSet<>();

  public RemoteFunctionHealthAggregator() {
    setName("Connectors");
    setRole(Role.CUSTOM);
    setTarget(Target.LOCAL);
  }

  private void addRfcHc(Connector connector) {
    connectorIds.add(connector.id);
    add(new RemoteFunctionHealthCheck(connector));
  }

  private void removeRfcHc(String connectorId) {
    checks.forEach(hc -> {
      RemoteFunctionHealthCheck rfcHc = (RemoteFunctionHealthCheck) hc;
      if (rfcHc.getConnectorId().equals(connectorId)) {
        connectorIds.remove(connectorId);
        remove(rfcHc);
      }
    });
  }

  @Override
  public Status execute() {
    Status s = super.execute();
    Response res = getResponse();

    try {
      List<Connector> activeConnectors = RemoteFunctionClient.getInstances()
          .stream()
          .map(RemoteFunctionClient::getConnectorConfig)
          .collect(Collectors.toList());

      Set<String> activeConnectorIds = activeConnectors.stream().map(c -> c.id).collect(Collectors.toSet());
      Set<String> toDelete = new HashSet<>();
      Set<Connector> toAdd = new HashSet<>();

      connectorIds.forEach(connectorId -> {
        if (!activeConnectorIds.contains(connectorId)) {
          toDelete.add(connectorId);
        }
      });

      activeConnectors.forEach(connector -> {
        if (!connectorIds.contains(connector.id)) {
          toAdd.add(connector);
        }
      });


      toDelete.forEach(this::removeRfcHc);
      toAdd.forEach(this::addRfcHc);
    }
    catch (Exception e) {
      res = res.withMessage("Error when trying to gather remote functions info: " + e.getMessage());
      s.setResult(ERROR);
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
}
