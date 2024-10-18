/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static com.here.xyz.psql.DatabaseHandler.ECPS_PHRASE;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_HOST;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_PASSWORD;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_USER;
import static io.restassured.path.json.JsonPath.with;
import static org.junit.Assert.assertEquals;

import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableMap;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.tools.Helper;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.service.aws.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;

public abstract class PSQLAbstractIT extends Helper {

  public final static String CONNECTOR_ID = "connectorId";
  public final static String PROPERTY_SEARCH = "propertySearch";
  public final static String AUTO_INDEXING = "autoIndexing";
  public final static String ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
  public final static String ON_DEMAND_IDX_LIMIT = "onDemandIdxLimit";
  protected static final Logger LOGGER = LogManager.getLogger();

  protected static PSQLXyzConnector LAMBDA;
  private static SimulatedContext TEST_CONTEXT = new SimulatedContext("xyz-psql-local",
      Collections.singletonMap(ECPS_PHRASE, "testing"));
  private static final String TEST_ECPS = XyzSerializable.serialize(ImmutableMap.of(
      PSQL_HOST, "localhost",
      PSQL_USER, "postgres",
      PSQL_PASSWORD, "password"
      //$encrypt({"PSQL_HOST":"${PSQL_HOST}","PSQL_REPLICA_HOST":"${PSQL_HOST}","PSQL_REPLICA_USER":"ro_user","PSQL_PORT":"${PSQL_PORT}","PSQL_DB":"${PSQL_DB}","PSQL_USER":"${PSQL_USER}","PSQL_PASSWORD":"${PSQL_PASSWORD}","PSQL_MAX_CONN":1024})
  ));
  protected static Random RANDOM = new Random();
  protected static String TEST_SPACE_ID = "foo";

  @BeforeClass
  public static void init() throws Exception {
    initEnv(null);
  }

  protected static Map<String, Object> defaultTestConnectorParams = new HashMap<String,Object>(){
    {
      put("connectorId","test-connector");
      put("propertySearch", true);
    }
  };

  protected static void initEnv(Map<String, Object>  connectorParameters) throws Exception {
    LOGGER.info("Setup environment...");

    LAMBDA = new PSQLXyzConnector();
    LAMBDA.reset();

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;

    HealthCheckEvent event = new HealthCheckEvent()
      .withMinResponseTime(100)
      .withConnectorParams(connectorParameters);

    invokeLambda(event);
    LOGGER.info("Setup environment Completed.");
  }

  protected static void invokeCreateTestSpace(String spaceId) throws Exception {
    invokeCreateTestSpace(Collections.emptyMap(), spaceId);
  }

  protected static void invokeCreateTestSpace(Map<String, Object>  connectorParameters, String spaceId) throws Exception {
    LOGGER.info("Create Test space ...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace(spaceId)
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withConnectorParams(connectorParameters)
            .withSpaceDefinition(new Space()
                    .withId(spaceId)
            );
    SuccessResponse response = XyzSerializable.deserialize(invokeLambda(mse));
    assertEquals("OK",response.getStatus());
  }

  protected static void invokeDeleteTestSpace() throws Exception {
    invokeDeleteTestSpace(Collections.emptyMap());
  }

  protected static void invokeDeleteTestSpace(Map<String, Object>  connectorParameters) throws Exception {
    LOGGER.info("Cleanup spaces ...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace(TEST_SPACE_ID)
            .withOperation(ModifySpaceEvent.Operation.DELETE)
            .withConnectorParams(connectorParameters);

    String response = invokeLambda(mse);
    assertEquals("Check response status", "OK", with(response).get("status"));

    LOGGER.info("Cleanup space Completed.");
  }

  protected static void invokeDeleteTestSpaces(Map<String, Object>  connectorParameters, List<String> spaces) throws Exception {
    LOGGER.info("Cleanup spaces ...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;

    for (String space : spaces) {
      ModifySpaceEvent mse = new ModifySpaceEvent()
              .withSpace(space)
              .withOperation(ModifySpaceEvent.Operation.DELETE)
              .withConnectorParams(connectorParameters);

      String response = invokeLambda(mse);
      assertEquals("Check response status", "OK", with(response).get("status"));
    }

    LOGGER.info("Cleanup spaces Completed.");
  }

  protected String invokeLambdaFromFile(String file) throws Exception {
    return invokeLambda(XyzSerializable.deserialize(PSQLAbstractIT.class.getResourceAsStream(file), Event.class));
  }

  protected static String invokeLambda(Event event) throws Exception {
    //TODO: Remove this injection of "connectorId" connector-param when the hash of ECPS is used as cache key for any connections in the PSQL connector
    Map<String, Object> connectorParams = event.getConnectorParams() != null ? new HashMap<>(event.getConnectorParams()) : new HashMap<>();
    connectorParams.put(CONNECTOR_ID, "test-connector");
    if (!connectorParams.containsKey("ecps"))
      connectorParams.put("ecps", ECPSTool.encrypt(TEST_CONTEXT.getEnv(ECPS_PHRASE), TEST_ECPS));
    event.setConnectorParams(connectorParams);
    return invokeLambda(event.toString());
  }

  private static String invokeLambda(String request) throws Exception {
    LOGGER.info("Request to lambda - {}", request);
    return invokeLambda(new ByteArrayInputStream(request.getBytes()));
  }

  private static String invokeLambda(InputStream jsonStream) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    LAMBDA.handleRequest(jsonStream, os, TEST_CONTEXT);
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    LOGGER.info("Response from lambda - {}", response);
    return response;
  }
}
