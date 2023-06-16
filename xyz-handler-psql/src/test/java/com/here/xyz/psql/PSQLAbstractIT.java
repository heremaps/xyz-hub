/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.util.IOUtils;
import com.here.mapcreator.ext.naksha.PsqlPool;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.IoEventPipeline;
import com.here.xyz.Payload;
import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.tools.Helper;
import com.here.xyz.responses.SuccessResponse;
import com.jayway.jsonpath.JsonPath;
import javax.sql.DataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class PSQLAbstractIT extends Helper {

  protected static final Logger LOGGER = LogManager.getLogger();

  protected static Random RANDOM = new Random();
  protected static String TEST_SPACE_ID = "foo";

  protected static Map<String, Object> defaultTestConnectorParams = new HashMap<String, Object>() {
    {
      put("connectorId", "test-connector");
      put("propertySearch", true);
    }
  };

  protected static PsqlHandlerParams connectorParams;

  protected static DataSource dataSource() {
    assert connectorParams != null;
    return PsqlPool.get(connectorParams.getDbConfig()).dataSource;
  }

  protected static void initEnv(Map<String, Object> connectorParameters) throws Exception {
    LOGGER.info("Setup environment...");
    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    connectorParams = new PsqlHandlerParams(connectorParameters);

    final HealthCheckEvent event = new HealthCheckEvent();
    event.setMinResponseTime(100);
    //event.setConnectorParams(connectorParameters);

    invokeLambda(event.serialize());
    LOGGER.info("Setup environment Completed.");
  }

  protected static void invokeCreateTestSpace(Map<String, Object> connectorParameters, String spaceId) throws Exception {
    LOGGER.info("Creat Test space..");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    final Space space = new Space(RandomStringUtils.randomAlphabetic(12));
    space.setId(spaceId);
    final ModifySpaceEvent event = new ModifySpaceEvent();
    //event.setSpaceId(spaceId);
    event.setOperation(ModifySpaceEvent.Operation.CREATE);
    //event.setConnectorParams(connectorParameters);
    event.setSpaceDefinition(space);
    SuccessResponse response = JsonSerializable.deserialize(invokeLambda(event.serialize()));
    assertEquals("OK", response.getStatus());
  }

  protected static void invokeDeleteTestSpace(Map<String, Object> connectorParameters) throws Exception {
    LOGGER.info("Cleanup spaces..");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    final ModifySpaceEvent event = new ModifySpaceEvent();
    //event.setSpaceId(TEST_SPACE_ID);
    event.setOperation(ModifySpaceEvent.Operation.DELETE);
    //event.setConnectorParams(connectorParameters);
    String response = invokeLambda(event.serialize());
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());

    LOGGER.info("Cleanup space Completed.");
  }

  protected static void invokeDeleteTestSpaces(Map<String, Object> connectorParameters, List<String> spaces) throws Exception {
    LOGGER.info("Cleanup spaces...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;

    for (String space : spaces) {
      final ModifySpaceEvent event = new ModifySpaceEvent();
      //event.setSpaceId(space);
      event.setOperation(ModifySpaceEvent.Operation.DELETE);
      //event.setConnectorParams(connectorParameters);

      String response = invokeLambda(event.serialize());
      assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());
    }

    LOGGER.info("Cleanup spaces Completed.");
  }

  protected String invokeLambdaFromFile(String file) throws Exception {
    InputStream jsonStream = PSQLAbstractIT.class.getResourceAsStream(file);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    final IoEventPipeline pipeline = new IoEventPipeline();
    // TODO: We need to create a pre-configured connector for the test, because the connector is the PSQL storage for a specific db!
    pipeline.addEventHandler(new PsqlHandler(new Connector(RandomStringUtils.randomAlphabetic(12), RandomUtils.nextInt())));
    assert jsonStream != null;
    pipeline.sendEvent(jsonStream, os);
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    LOGGER.info("Response from lambda - {}", response);
    return response;
  }

  protected static String invokeLambda(String request) throws Exception {
    LOGGER.info("Request to lambda - {}", request);
    InputStream jsonStream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    final IoEventPipeline pipeline = new IoEventPipeline();
    // TODO: We need to create a pre-configured connector for the test, because the connector is the PSQL storage for a specific db!
    pipeline.addEventHandler(new PsqlHandler(new Connector(RandomStringUtils.randomAlphabetic(12), RandomUtils.nextInt())));
    pipeline.sendEvent(jsonStream, os);
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    LOGGER.info("Response from lambda - {}", response);
    return response;
  }
}
