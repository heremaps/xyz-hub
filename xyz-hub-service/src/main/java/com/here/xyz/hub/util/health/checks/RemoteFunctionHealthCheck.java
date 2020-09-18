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

package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

public class RemoteFunctionHealthCheck extends ExecutableCheck {

  private Map<String, Object> rfcData = new HashMap<>();
  private Connector connector;

  RemoteFunctionHealthCheck(Connector connector) {
    this.connector = connector;
    setName(connector.id);
    setRole(Role.CUSTOM);
    setTarget(connector.remoteFunction instanceof Embedded ? Target.LOCAL : Target.REMOTE);
  }

  @JsonIgnore
  public String getConnectorId() {
    return connector.id;
  }

  @Override
  public Status execute() throws InterruptedException {
    Status s = new Status();
    HealthCheckEvent healthCheck = new HealthCheckEvent();
    //Just generate a stream ID here as the stream actually "begins" here
    final String healthCheckStreamId = UUID.randomUUID().toString();
    healthCheck.setStreamId(healthCheckStreamId);
    try {
      RpcClient client = getClient();
      client.execute(new Log4jMarker(healthCheckStreamId), healthCheck, true, ar -> {
        if (ar.failed()) {
          setResponse(generateResponse().withMessage("Error in connector health-check: " + ar.cause().getMessage()));
          s.setResult(ERROR);
        }
        else {
          client.getFunctionClient().setLastHealthyTimestamp(Instant.now().getEpochSecond());
          setResponse(generateResponse());
          s.setResult(OK);
        }
        synchronized (s) {
          s.notify();
        }
      });

      while (s.getResult() == UNKNOWN) {
        synchronized (s) {
          s.wait();
        }
        Thread.sleep(100);
      }
    }
    catch (InterruptedException interruption) {
      setResponse(generateResponse());
      throw interruption;
    }
    catch (Exception e) {
      setResponse(generateResponse().withMessage("Error trying to execute health-check event: " + e.getMessage()));
      return s.withResult(ERROR);
    }

    return s;
  }

  private Response generateResponse() {
    Response r = new Response();
    try {
      RemoteFunctionClient rfc = getClient().getFunctionClient();

      rfcData.put("id", connector.remoteFunction.id);
      rfcData.put("type", connector.remoteFunction.getClass().getSimpleName());
      if (connector.remoteFunction instanceof AWSLambda) {
        rfcData.put("lambdaARN", ((AWSLambda) connector.remoteFunction).lambdaARN);
      }
      else if (connector.remoteFunction instanceof Http) {
        rfcData.put("url", ((Http) connector.remoteFunction).url);
      }
      rfcData.put("maxQueueSize", rfc.getMaxQueueSize());
      rfcData.put("queueSize", rfc.getQueueSize());
      rfcData.put("maxQueueByteSize", rfc.getMaxQueueByteSize());
      rfcData.put("queueByteSize", rfc.getQueueByteSize());
      rfcData.put("minConnections", rfc.getMinConnections());
      rfcData.put("maxConnections", rfc.getMaxConnections());
      rfcData.put("weightedMaxConnections", rfc.getWeightedMaxConnections());
      rfcData.put("usedConnections", rfc.getUsedConnections());
      rfcData.put("rateOfService", rfc.getRateOfService());
      rfcData.put("arrivalRate", rfc.getArrivalRate());
      rfcData.put("throughput", rfc.getThroughput());
      rfcData.put("priority", rfc.getPriority());
      rfcData.put("lastHealthyTimestamp", rfc.getLastHealthyTimestamp());

      return r.withAdditionalProperty("statistics", rfcData);
    }
    catch (Exception e) {
      return r.withMessage("Error when trying to gather remote function info: " + e.getMessage());
    }
  }

  private RpcClient getClient() {
    return RpcClient.getInstanceFor(connector);
  }

}
