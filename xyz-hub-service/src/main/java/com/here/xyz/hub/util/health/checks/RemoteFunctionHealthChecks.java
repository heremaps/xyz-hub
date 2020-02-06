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

import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.util.HashMap;
import java.util.Map;

public class RemoteFunctionHealthChecks extends ExecutableCheck {

  private Map<String, Map<String, Object>> rfcData = new HashMap<>();

  public RemoteFunctionHealthChecks() {
    setName("Connectors");
    setRole(Role.CUSTOM);
    setTarget(Target.LOCAL);
  }

  @Override
  public Status execute() {
    Status s = new Status();
    Response r = new Response();

    try {
      populateRfcData();
      r.setAdditionalProperty("globalMaxQueueByteSize", RemoteFunctionClient.GLOBAL_MAX_QUEUE_BYTE_SIZE);
      r.setAdditionalProperty("globalQueueByteSize", RemoteFunctionClient.getGlobalUsedQueueMemory());
      r.setAdditionalProperty("globalArrivalRate", RemoteFunctionClient.getGlobalArrivalRate());
      r.setAdditionalProperty("globalThroughput", RemoteFunctionClient.getGlobalThroughput());
      r.setAdditionalProperty("globalMaxConnections", RemoteFunctionClient.getGlobalMaxConnections());
      r.setAdditionalProperty("globalUsedConnections", RemoteFunctionClient.getGlobalUsedConnections());
      r.setAdditionalProperty("connectors", rfcData);
      setResponse(r);
      return s.withResult(OK);
    } catch (Exception e) {
      setResponse(r.withMessage("Error when trying to gather remote functions info: " + e.getMessage()));
      return s.withResult(ERROR);
    }
  }

  private void populateRfcData() {
    RemoteFunctionClient.getStream().forEach(rfc -> {
      final Connector connectorConfig = rfc.getConnectorConfig();
      if (connectorConfig == null) {
        return;
      }
      final RemoteFunctionConfig remoteFunction = connectorConfig.remoteFunction;
      if (remoteFunction == null) {
        return;
      }
      final String connectorId = rfc.getConnectorConfig().id;
      Map<String, Object> d = rfcData.get(connectorId);
      if (d == null) {
        rfcData.put(connectorId, d = new HashMap<>());
        String type = remoteFunction.getClass().getSimpleName();
        d.put("type", type);
        if (remoteFunction instanceof AWSLambda) {
          d.put("lambdaARN", ((AWSLambda) remoteFunction).lambdaARN);
        }
      }
      d.put("maxQueueSize", rfc.getMaxQueueSize());
      d.put("queueSize", rfc.getQueueSize());
      d.put("maxQueueByteSize", rfc.getMaxQueueByteSize());
      d.put("queueByteSize", rfc.getQueueByteSize());
      d.put("minConnections", rfc.getMinConnectionsPerInstance());
      d.put("maxConnections", rfc.getMaxConnectionsPerInstance());
      d.put("usedConnections", rfc.getUsedConnections());
      d.put("rateOfService", rfc.getRateOfService());
      d.put("arrivalRate", rfc.getArrivalRate());
      d.put("throughput", rfc.getThroughput());
    });
  }
}
