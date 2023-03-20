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

import com.amazonaws.util.IOUtils;
import com.here.mapcreator.ext.naksha.PsqlPool;
import com.here.xyz.SimpleIoPipeline;
import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import com.here.xyz.AbstractIoPipeline;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.tools.Helper;
import com.here.xyz.responses.SuccessResponse;
import com.jayway.jsonpath.JsonPath;
import javax.sql.DataSource;
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

import static org.junit.Assert.assertEquals;

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

  protected static PsqlProcessorParams connectorParams;

  protected static DataSource dataSource() {
    assert connectorParams != null;
    return PsqlPool.get(connectorParams.getDbConfig()).dataSource;
  }

  protected static void initEnv(Map<String, Object> connectorParameters) throws Exception {
    LOGGER.info("Setup environment...");
    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    connectorParams = new PsqlProcessorParams(connectorParameters, "IntegrationTest");

    HealthCheckEvent event = new HealthCheckEvent()
        .withMinResponseTime(100)
        .withConnectorParams(connectorParameters);

    invokeLambda(event.serialize());
    LOGGER.info("Setup environment Completed.");
  }

  protected static void invokeCreateTestSpace(Map<String, Object> connectorParameters, String spaceId) throws Exception {
    LOGGER.info("Creat Test space..");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    ModifySpaceEvent mse = new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(ModifySpaceEvent.Operation.CREATE)
        .withConnectorParams(connectorParameters)
        .withSpaceDefinition(new Space()
            .withId(spaceId)
        );
    SuccessResponse response = XyzSerializable.deserialize(invokeLambda(mse.serialize()));
    assertEquals("OK", response.getStatus());
  }

  protected static void invokeDeleteTestSpace(Map<String, Object> connectorParameters) throws Exception {
    LOGGER.info("Cleanup spaces..");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    ModifySpaceEvent mse = new ModifySpaceEvent()
        .withSpace(TEST_SPACE_ID)
        .withOperation(ModifySpaceEvent.Operation.DELETE)
        .withConnectorParams(connectorParameters);

    String response = invokeLambda(mse.serialize());
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());

    LOGGER.info("Cleanup space Completed.");
  }

  protected static void invokeDeleteTestSpaces(Map<String, Object> connectorParameters, List<String> spaces) throws Exception {
    LOGGER.info("Cleanup spaces...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;

    for (String space : spaces) {
      ModifySpaceEvent mse = new ModifySpaceEvent()
          .withSpace(space)
          .withOperation(ModifySpaceEvent.Operation.DELETE)
          .withConnectorParams(connectorParameters);

      String response = invokeLambda(mse.serialize());
      assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());
    }

    LOGGER.info("Cleanup spaces Completed.");
  }

  protected String invokeLambdaFromFile(String file) throws Exception {
    InputStream jsonStream = PSQLAbstractIT.class.getResourceAsStream(file);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    final AbstractIoPipeline pipeline = new SimpleIoPipeline(new PsqlProcessor());
    assert jsonStream != null;
    pipeline.processEvent(jsonStream, os);
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    LOGGER.info("Response from lambda - {}", response);
    return response;
  }

  protected static String invokeLambda(String request) throws Exception {
    LOGGER.info("Request to lambda - {}", request);
    InputStream jsonStream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    final AbstractIoPipeline pipeline = new SimpleIoPipeline(new PsqlProcessor());
    pipeline.processEvent(jsonStream, os);
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    LOGGER.info("Response from lambda - {}", response);
    return response;
  }
}
