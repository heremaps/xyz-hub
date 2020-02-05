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

package com.here.xyz.psql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.MultiPolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.*;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.PropertiesStatistics;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class PSQLXyzConnectorIT {

  private static final Logger logger = LogManager.getLogger();

  private static PSQLXyzConnector lambda;

  @BeforeClass
  public static void setupEnv() throws Exception {
    logger.info("Setup environment...");

    lambda = new PSQLXyzConnector();
    lambda.setEmbedded(true);

    invokeLambdaFromFile("/events/HealthCheckEvent.json");

    logger.info("Setup Completed.");
  }

  @Before
  public void setup() throws Exception {
    logger.info("Setup...");

    // DELETE EXISTING FEATURES TO START FRESH
    invokeLambdaFromFile("/events/DeleteSpaceEvent.json");

    logger.info("Setup Completed.");
  }

  @After
  public void shutdown() throws Exception {
    logger.info("Shutdown...");
    invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    logger.info("Shutdown Completed.");
  }

  @Test
  public void testHealthCheck() throws Exception {
    String response = invokeLambdaFromFile("/events/HealthCheckEvent.json");
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());
  }

  @Test
  public void testHealthCheckWithConnectorParams() throws Exception {
    Map<String, Object> connectorParams = new HashMap<>();
    Map<String, Object> parametersToEncrypt = new HashMap<>();
    parametersToEncrypt.put(PSQLConfig.PSQL_HOST, "example.com");
    parametersToEncrypt.put(PSQLConfig.PSQL_PORT, "1234");
    parametersToEncrypt.put(PSQLConfig.PSQL_PASSWORD, "1234password");
    connectorParams.put("ecps", PSQLConfig.encryptCPS(new ObjectMapper().writeValueAsString(parametersToEncrypt), "testing"));

    HealthCheckEvent event = new HealthCheckEvent()
        .withMinResponseTime(100)
        .withConnectorParams(connectorParams);

    PSQLConfig config = new PSQLConfig(event, GSContext.newLocal());
    assertEquals(config.host(), "example.com");
    assertEquals(config.port(), 1234);
    assertEquals(config.password(), "1234password");
  }

  @Test
  public void testTableCreated() throws Exception {
    String response = invokeLambdaFromFile("/events/TestCreateTable.json");
    assertEquals("Check response status", JsonPath.read(response, "$.type").toString(), "FeatureCollection");
  }

  //@Test
  public void testBrokenEvent() throws Exception {
    final String response = invokeLambdaFromFile("/events/BrokenEvent.json");
    try {
      ErrorResponse error = XyzSerializable.deserialize(response);
      assertNotNull(error);
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testIterate() throws Exception {
    final String response = invokeLambdaFromFile("/events/IterateMySpace.json");
    final FeatureCollection features = XyzSerializable.deserialize(response);
    features.serialize(true);
  }

  /**
   * Test getFeaturesByGeometryEvent
   */
  @Test
  public void testGetFeaturesByGeometryQuery() throws Exception {
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();
    // =========== INSERT Point Grid 20x20 ==========
    for (double x = 7.; x < 7.19d; x += 0.01d) {
      for (double y = 50.; y < 50.19d; y += 0.01d) {
        Feature f = new Feature()
            .withGeometry(
                new Point().withCoordinates(new PointCoordinates((Math.round(x * 10000.0) / 10000.0), Math.round(y * 10000.0) / 10000.0)))
            .withProperties(new Properties().with("foo", Math.round(x * 10000.0) / 10000.0).with("foo2", 1).withXyzNamespace(xyzNamespace));
        featureList.add(f);
      }
    }
    // =========== INSERT Polygon  ==========
    PolygonCoordinates singlePoly = new PolygonCoordinates();
    LinearRingCoordinates rC = new LinearRingCoordinates();
    rC.add(new Position(7.01, 50.01));
    rC.add(new Position(7.03, 50.01));
    rC.add(new Position(7.03, 50.03));
    rC.add(new Position(7.01, 50.03));
    rC.add(new Position(7.01, 50.01));
    singlePoly.add(rC);

    Feature f = new Feature()
        .withGeometry(
            new Polygon().withCoordinates(singlePoly))
        .withProperties(new Properties().with("foo", 999.1).withXyzNamespace(xyzNamespace));
    featureList.add(f);

    collection.setFeatures(featureList);
    // =========== INSERT Polygon  inside hole ==========
    singlePoly = new PolygonCoordinates();
    rC = new LinearRingCoordinates();
    rC.add(new Position(7.06, 50.07));
    rC.add(new Position(7.08, 50.07));
    rC.add(new Position(7.08, 50.08));
    rC.add(new Position(7.06, 50.08));
    rC.add(new Position(7.06, 50.07));
    singlePoly.add(rC);

    f = new Feature()
        .withGeometry(
            new Polygon().withCoordinates(singlePoly))
        .withProperties(new Properties().with("foo", 999.2).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

// =========== INSERT Line ==========
    LineStringCoordinates lcCoords = new LineStringCoordinates();
    lcCoords.add(new Position(7.02, 50.02));
    lcCoords.add(new Position(7.18, 50.18));

    f = new Feature()
        .withGeometry(
            new LineString().withCoordinates(lcCoords))
        .withProperties(new Properties().with("foo", 999.3).withXyzNamespace(xyzNamespace));
    featureList.add(f);

    // =========== INSERT Line ==========
    lcCoords = new LineStringCoordinates();
    lcCoords.add(new Position(7.16, 50.01));
    lcCoords.add(new Position(7.19, 50.01));

    f = new Feature()
        .withGeometry(
            new LineString().withCoordinates(lcCoords))
        .withProperties(new Properties().with("foo", 999.4).withXyzNamespace(xyzNamespace));
    featureList.add(f);

    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());
    logger.info("Insert feature tested successfully");
// =========== QUERY WITH POLYGON ==========
    PolygonCoordinates polyCoords = new PolygonCoordinates();
    LinearRingCoordinates ringCords = new LinearRingCoordinates();
    ringCords.add(new Position(7, 50));
    ringCords.add(new Position(7, 50.1));
    ringCords.add(new Position(7.1, 50.1));
    ringCords.add(new Position(7.1, 50));
    ringCords.add(new Position(7, 50));
    polyCoords.add(ringCords);

    Geometry geo = new Polygon().withCoordinates(polyCoords);

    GetFeaturesByGeometryEvent geometryEvent = new GetFeaturesByGeometryEvent()
        .withSpace("foo")
        .withGeometry(geo);

    String queryResponse = invokeLambda(geometryEvent.serialize());
    FeatureCollection featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    assertEquals(124, featureCollection.getFeatures().size());
    logger.info("Area Query with POLYGON tested successfully");
    // =========== QUERY WITH POLYGON WITH HOLE ==========
    LinearRingCoordinates holeCords = new LinearRingCoordinates();
    holeCords.add(new Position(7.05, 50.05));
    holeCords.add(new Position(7.05, 50.09));
    holeCords.add(new Position(7.09, 50.09));
    holeCords.add(new Position(7.09, 50.05));
    holeCords.add(new Position(7.05, 50.05));
    polyCoords.add(holeCords);

    geo = new Polygon().withCoordinates(polyCoords);
    geometryEvent = new GetFeaturesByGeometryEvent()
        .withSpace("foo")
        .withGeometry(geo);

    queryResponse = invokeLambda(geometryEvent.serialize());
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    for (Feature feature : featureCollection.getFeatures()) {
      /* try to find polygon inside the hole  */
      if ((feature.getProperties().get("foo")).toString().contains(("999.2"))) {
        fail();
      }
    }
    assertEquals(114, featureCollection.getFeatures().size());
    logger.info("Area Query with POLYGON incl. hole tested successfully");
    // =========== QUERY WITH MULTIPOLYGON ==========
    PolygonCoordinates polyCoords2 = new PolygonCoordinates();
    LinearRingCoordinates ringCords2 = new LinearRingCoordinates();
    ringCords2.add(new Position(7.1, 50.1));
    ringCords2.add(new Position(7.2, 50.1));
    ringCords2.add(new Position(7.2, 50.2));
    ringCords2.add(new Position(7.1, 50.2));
    ringCords2.add(new Position(7.1, 50.1));
    polyCoords2.add(ringCords2);

    MultiPolygonCoordinates multiCords = new MultiPolygonCoordinates();
    multiCords.add(polyCoords);
    multiCords.add(polyCoords2);

    geo = new MultiPolygon().withCoordinates(multiCords);
    geometryEvent = new GetFeaturesByGeometryEvent()
        .withSpace("foo")
        .withGeometry(geo);

    queryResponse = invokeLambda(geometryEvent.serialize());
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    int cnt = 0;
    for (Feature feature : featureCollection.getFeatures()) {
      /* Try to find the both polygons */
      if ((feature.getProperties().get("foo")).toString().contains(("999."))) {
        cnt++;
      }
    }
    assertEquals(213, featureCollection.getFeatures().size());
    assertEquals(2, cnt);
    logger.info("Area Query with MULTIPOLYGON tested successfully");
    // =========== QUERY WITH MULTIPOLYGON + PROPERTIES_SEARCH ==========
    PropertiesQuery pq = new PropertiesQuery();
    PropertyQueryList pql = new PropertyQueryList();
    pql.add(new PropertyQuery().withKey("properties.foo").withOperation(QueryOperation.LESS_THAN_OR_EQUALS)
        .withValues(new ArrayList<>(Collections.singletonList(7.1))));
    pq.add(pql);

    geo = new MultiPolygon().withCoordinates(multiCords);
    geometryEvent = new GetFeaturesByGeometryEvent()
        .withSpace("foo")
        .withGeometry(geo)
        .withPropertiesQuery(pq);

    queryResponse = invokeLambda(geometryEvent.serialize());
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    assertEquals(121, featureCollection.getFeatures().size());
    logger.info("Area Query with MULTIPOLYGON + PROPERTIES_SEARCH tested successfully");
    // =========== QUERY WITH MULTIPOLYGON + SELECTION ==========
    geo = new MultiPolygon().withCoordinates(multiCords);
    geometryEvent = new GetFeaturesByGeometryEvent()
        .withSpace("foo")
        .withGeometry(geo)
        .withSelection(new ArrayList<>(Collections.singletonList("properties.foo2")));

    queryResponse = invokeLambda(geometryEvent.serialize());
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    assertEquals(213, featureCollection.getFeatures().size());

    Properties properties = featureCollection.getFeatures().get(0).getProperties();
    assertEquals(new Integer(1), properties.get("foo2"));
    assertNull(properties.get("foo"));
    logger.info("Area Query with MULTIPOLYGON + SELECTION tested successfully");
  }

  /**
   * Test all branches of the BBox query.
   */
  @Test
  public void testBBoxQuery() throws Exception {
    // =========== INSERT ==========
    final String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    final String insertResponse = invokeLambdaFromFile(insertJsonFile);
    final String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== QUERY BBOX ==========
    String queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [],\n"
        + "\t\"simplificationLevel\": -1,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": false\n"
        + "}";
    String queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    FeatureCollection featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    List<Feature> features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(3, features.size());

    // =========== QUERY BBOX - +TAGS ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
        + "\t\"simplificationLevel\": -1,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());

    // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
    queryEvent = "{\n"
        + "\"margin\": 20,\n"
        + "\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\"level\": 3,\n"
        + "\"bbox\": [-170, -170, 170, 170],\n"
        + "\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\"space\": \"foo\",\n"
        + "\"tags\": [[\"yellow\"]],\n"
        + "\"simplificationLevel\": -1,\n"
        + "\"limit\": 30000,\n"
        + "\"selection\": [\"id\",\"type\",\"geometry\",\"properties.name\"],\n"
        + "\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());
    assertEquals(1, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());
    assertEquals("Toyota", features.get(0).getProperties().get("name"));
    assertNotNull(features.get(0).getId());
    assertNotNull(features.get(0));
    assertNotNull(features.get(0).getGeometry());

    // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
    queryEvent = "{\n"
        + "\"margin\": 20,\n"
        + "\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\"level\": 3,\n"
        + "\"bbox\": [-170, -170, 170, 170],\n"
        + "\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\"space\": \"foo\",\n"
        + "\"tags\": [[\"yellow\"]],\n"
        + "\"simplificationLevel\": -1,\n"
        + "\"limit\": 30000,\n"
        + "\"selection\": [\"properties.@ns:com:here:xyz.tags\"],\n"
        + "\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());
    assertEquals(1, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());
    assertEquals(1, features.get(0).getProperties().getXyzNamespace().getTags().size());

    // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
    queryEvent = "{\n"
        + "\"margin\": 20,\n"
        + "\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\"level\": 3,\n"
        + "\"bbox\": [-170, -170, 170, 170],\n"
        + "\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\"space\": \"foo\",\n"
        + "\"tags\": [[\"yellow\"]],\n"
        + "\"simplificationLevel\": -1,\n"
        + "\"limit\": 30000,\n"
        + "\"selection\": [\"properties\"],\n"
        + "\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());
    assertEquals(2, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());

    // =========== QUERY BBOX - +SMALL; +TAGS ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [10, -5, 20, 5],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
        + "\t\"simplificationLevel\": -1,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());

    // =========== QUERY BBOX - +TAGS, +simplificationLevel ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
        + "\t\"simplificationLevel\": 2,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": false\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());

    // =========== QUERY BBOX - +TAGS, +clip ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
        + "\t\"simplificationLevel\": -1,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": true\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());

    // =========== QUERY BBOX - +TAGS, +simplificationLevel, +clip ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
        + "\t\"simplificationLevel\": 2,\n"
        + "\t\"limit\": 30000,\n"
        + "\t\"clip\": true\n"
        + "}";
    queryResponse = invokeLambda(queryEvent);
    assertNotNull(queryResponse);
    featureCollection = XyzSerializable.deserialize(queryResponse);
    assertNotNull(featureCollection);
    features = featureCollection.getFeatures();
    assertNotNull(features);
    assertEquals(1, features.size());

    // =========== DELETE SPACE ==========
    String deleteSpaceResponse = invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    assertDeleteSpaceResponse(deleteSpaceResponse);
    logger.info("Delete space tested successfully - " + deleteSpaceResponse);
  }


  @Test
  public void testCrudFeatureWithHash() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventWithHash.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
    String featuresList = XyzSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {
    });
    String updateRequest = "{\n" +
        "    \"type\": \"ModifyFeaturesEvent\",\n" +
        "    \"space\": \"foo\",\n" +
        "    \"enableUUID\": true,\n" +
        "    \"params\": {},\n" +
        "    \"updateFeatures\": " + featuresList + "\n" +
        "}";
    updateRequest = updateRequest.replaceAll("Tesla", "Honda");
    String updateResponse = invokeLambda(updateRequest);
    assertUpdate(updateRequest, updateResponse, true);
    assertUpdate(updateRequest, updateResponse, true);
    logger.info("Update feature tested successfully");
  }

  @Test
  public void testCrudFeatureWithTransaction() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
    String featuresList = XyzSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {
    });
    String updateRequest = "{\n" +
        "    \"type\": \"ModifyFeaturesEvent\",\n" +
        "    \"space\": \"foo\",\n" +
        "    \"enableUUID\": true,\n" +
        "    \"params\": {},\n" +
        "    \"updateFeatures\": " + featuresList + "\n" +
        "}";
    updateRequest = updateRequest.replaceAll("Tesla", "Honda");
    String updateResponse = invokeLambda(updateRequest);
    assertUpdate(updateRequest, updateResponse, true);
    logger.info("Update feature tested successfully");

    // =========== LoadFeaturesEvent ==========
    String loadFeaturesEvent = "/events/LoadFeaturesEvent.json";
    String loadResponse = invokeLambdaFromFile(loadFeaturesEvent);
    featureCollection = XyzSerializable.deserialize(loadResponse);
    assertNotNull(featureCollection.getFeatures());
    assertEquals(1, featureCollection.getFeatures().size());
    assertEquals("test", featureCollection.getFeatures().get(0).getId());

    // =========== DELETE ==========
    final String deleteId = featureCollection.getFeatures().get(0).getId();
    String deleteRequest = "{\n" +
        "    \"type\": \"ModifyFeaturesEvent\",\n" +
        "    \"space\": \"foo\",\n" +
        "    \"enableUUID\": true,\n" +
        "    \"transaction\": true,\n" +
        "    \"params\": {},\n" +
        "    \"deleteFeatures\": {\"" + deleteId + "\":null}\n" +
        "}";
    String deleteResponse = invokeLambda(deleteRequest);
  }

  @Test
  public void testGetStatisticsEvent() throws Exception {

    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== GetStatistics ==========
    invokeLambdaFromFile("/events/HealthCheckEvent.json");
    GetStatisticsEvent event = new GetStatisticsEvent();
    event.setSpace("foo");
    event.setStreamId(RandomStringUtils.randomAlphanumeric(10));
    String eventJson = event.serialize();
    String statisticsJson = invokeLambda(eventJson);
    StatisticsResponse response = XyzSerializable.deserialize(statisticsJson);

    assertNotNull(response);

    assertEquals(new Long(3), response.getCount().getValue());
    assertEquals(false, response.getCount().getEstimated());

    assertTrue(response.getByteSize().getValue() > 0);
    assertEquals(true, response.getByteSize().getEstimated());

    assertNotNull(response.getBbox());
    assertEquals(false, response.getBbox().getEstimated());

    assertEquals("name", response.getProperties().getValue().get(0).getKey());
    assertEquals("string", response.getProperties().getValue().get(0).getDatatype());
    assertEquals(3, response.getProperties().getValue().get(0).getCount());
    assertEquals(false, response.getProperties().getEstimated());
    assertEquals(PropertiesStatistics.Searchable.ALL, response.getProperties().getSearchable());

    assertEquals(3, response.getTags().getValue().size());
    assertEquals(false, response.getTags().getEstimated());

    assertEquals(new ArrayList<>(Collections.singletonList("Point")), response.getGeometryTypes().getValue());
    assertEquals(false, response.getGeometryTypes().getEstimated());

    // =========== INSERT 11k ==========
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(3).collect(Collectors.toList());

    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> {
          Feature f = new Feature()
              .withGeometry(
                  new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties());
          pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
          return f;
        }).limit(11000).collect(Collectors.toList()));

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    /* Needed to trigger update on pg_stat*/
    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("ANALYZE public.\"foo\";");
    }

    statisticsJson = invokeLambda(eventJson);
    // =========== GetStatistics ==========
    response = XyzSerializable.deserialize(statisticsJson);

    assertEquals(new Long(11003), response.getCount().getValue());

    assertEquals(true, response.getCount().getEstimated());
    assertEquals(true, response.getByteSize().getEstimated());
    assertEquals(true, response.getBbox().getEstimated());
    assertEquals(true, response.getTags().getEstimated());
    assertEquals(true, response.getGeometryTypes().getEstimated());

    assertEquals(PropertiesStatistics.Searchable.PARTIAL, response.getProperties().getSearchable());

    for (PropertyStatistics prop : response.getProperties().getValue()) {
      assertTrue(pKeys.contains(prop.getKey()));
      assertEquals(prop.getCount(), 11003);
    }
    // =========== DELETE SPACE ==========
    String deleteSpaceResponse = invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    assertDeleteSpaceResponse(deleteSpaceResponse);
    logger.info("Delete space tested successfully - " + deleteSpaceResponse);
  }

  @Test
  public void testAutoIndexing() throws Exception {
    // =========== INSERT further 11k ==========
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(3).collect(Collectors.toList());

    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> {
          Feature f = new Feature()
              .withGeometry(
                  new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties());
          pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(3)));
          return f;
        }).limit(11000).collect(Collectors.toList()));

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    /* Needed to trigger update on pg_stat*/
    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='foo';");
      stmt.execute("ANALYZE public.\"foo\";");
    }

    HealthCheckEvent health = new HealthCheckEvent();
    health.setConnectorParams(new HashMap<String, Object>() {{
      put("propertySearch", true);
    }});
    invokeLambda(health.serialize());
  }

  @Test
  public void testUpsertFeature() {
/*
    // =========== INSERT ==========
    String insertJsonFile = "/UpsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOTools.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    logger.info("Insert of upsert-operation successful");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = new FeatureCollection().capture(Json.parse(insertResponse));
    featureCollection.features().get(0).properties().put("name", "Toyota");
    featureCollection.features().get(0).properties().nsXyz().tags().add("green");
    featureCollection.features().get(1).properties().put("name", "Tesla");
    featureCollection.features().get(1).properties().nsXyz().tags().add("green");
    final String updateRequest = Json.stringify(featureCollection);
    String updateResponse = invokeLambda(updateRequest);
    assertUpdate(updateRequest, updateResponse, false);
    logger.info("Update of upsert-operation successful");
*/
  }

  @Test
  public void testCrudFeatureWithoutHash() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    logger.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    logger.info("Insert feature tested successfully");

    // =========== COUNT ==========
    String countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
    assertCount(insertRequest, countResponse);
    logger.info("Count feature tested successfully");

    // =========== SEARCH ==========
    String searchResponse = invokeLambdaFromFile("/events/SearchForFeaturesEvent.json");
    assertRead(insertRequest, searchResponse, false);
    logger.info("Search feature tested successfully");

    // =========== SEARCH WITH PROPERTIES ========
    String searchPropertiesResponse = invokeLambdaFromFile("/events/SearchForFeaturesByPropertiesEvent.json");
    assertRead(insertRequest, searchPropertiesResponse, false);
    logger.info("Search Properties feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
    String featuresList = XyzSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {
    });
    String updateRequest = "{\n" +
        "    \"type\": \"ModifyFeaturesEvent\",\n" +
        "    \"space\": \"foo\",\n" +
        "    \"params\": {},\n" +
        "    \"updateFeatures\": " + featuresList + "\n" +
        "}";
    updateRequest = updateRequest.replaceAll("Tesla", "Honda");
    String updateResponse = invokeLambda(updateRequest);
    assertUpdate(updateRequest, updateResponse, false);
    logger.info("Update feature tested successfully");

    // =========== DELETE FEATURES ==========
    invokeLambdaFromFile("/events/DeleteFeaturesByTagEvent.json");
    logger.info("Delete feature tested successfully");

    // =========== DELETE SPACE ==========
    String deleteSpaceResponse = invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    assertDeleteSpaceResponse(deleteSpaceResponse);
    logger.info("Delete space tested successfully - " + deleteSpaceResponse);
  }

  @Test
  public void testSearchAndCountByPropertiesAndTags() throws Exception {
    // prepare the data
    TypeReference<Map<String, Object>> tr = new TypeReference<Map<String, Object>>() {
    };
    ObjectMapper mapper = new ObjectMapper();

    String insertJsonFile = "/events/InsertFeaturesForSearchTestEvent.json";
    invokeLambdaFromFile(insertJsonFile);

    // retrieve the basic event
    String basic = IOUtils.toString(GSContext.class.getResourceAsStream("/events/BasicSearchByPropertiesAndTagsEvent.json"));

    // Test 1
    Map<String, Object> test1 = mapper.readValue(basic, tr);
    Map<String, Object> properties1 = new HashMap<>();
    properties1.put("key", "properties.name");
    properties1.put("operation", "EQUALS");
    properties1.put("values", Stream.of("Toyota").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test1, properties1);
    addTagsToSearchObject(test1, "yellow");
    invokeAndAssert(test1, 1, "Toyota");

    // Test 2
    Map<String, Object> test2 = mapper.readValue(basic, tr);
    Map<String, Object> properties2 = new HashMap<>();
    properties2.put("key", "properties.size");
    properties2.put("operation", "LESS_THAN_OR_EQUALS");
    properties2.put("values", Stream.of(1).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test2, properties2);
    invokeAndAssert(test2, 2, "Ducati", "BikeX");

    // Test 3
    Map<String, Object> test3 = mapper.readValue(basic, tr);
    Map<String, Object> properties3 = new HashMap<>();
    properties3.put("key", "properties.car");
    properties3.put("operation", "EQUALS");
    properties3.put("values", Stream.of(true).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test3, properties3);
    invokeAndAssert(test3, 1, "Toyota");

    // Test 4
    Map<String, Object> test4 = mapper.readValue(basic, tr);
    Map<String, Object> properties4 = new HashMap<>();
    properties4.put("key", "properties.car");
    properties4.put("operation", "EQUALS");
    properties4.put("values", Stream.of(false).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test4, properties4);
    invokeAndAssert(test4, 1, "Ducati");

    // Test 5
    Map<String, Object> test5 = mapper.readValue(basic, tr);
    Map<String, Object> properties5 = new HashMap<>();
    properties5.put("key", "properties.size");
    properties5.put("operation", "GREATER_THAN");
    properties5.put("values", Stream.of(5).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test5, properties5);
    addTagsToSearchObject(test5, "red");
    invokeAndAssert(test5, 1, "Toyota");

    // Test 6
    Map<String, Object> test6 = mapper.readValue(basic, tr);
    Map<String, Object> properties6 = new HashMap<>();
    properties6.put("key", "properties.size");
    properties6.put("operation", "LESS_THAN");
    properties6.put("values", Stream.of(5).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test6, properties6);
    addTagsToSearchObject(test6, "red");
    invokeAndAssert(test6, 1, "Ducati");

    // Test 7
    Map<String, Object> test7 = mapper.readValue(basic, tr);
    Map<String, Object> properties7 = new HashMap<>();
    properties7.put("key", "properties.name");
    properties7.put("operation", "EQUALS");
    properties7.put("values", Stream.of("Toyota", "Tesla").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test7, properties7);
    invokeAndAssert(test7, 2, "Toyota", "Tesla");

    // Test 8
    Map<String, Object> test8 = mapper.readValue(basic, tr);
    invokeAndAssert(test8, 4, "Toyota", "Tesla", "Ducati", "BikeX");

    // Test 9
    Map<String, Object> test9 = mapper.readValue(basic, tr);
    Map<String, Object> properties9 = new HashMap<>();
    properties9.put("key", "properties.name");
    properties9.put("operation", "EQUALS");
    properties9.put("values", Stream.of("Test").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test9, properties9);
    invokeAndAssert(test9, 0);

    // Test 10
    Map<String, Object> test10 = mapper.readValue(basic, tr);
    Map<String, Object> properties10 = new HashMap<>();
    properties10.put("key", "properties.name");
    properties10.put("operation", "EQUALS");
    properties10.put("values", Stream.of("Toyota").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test10, properties10);
    addTagsToSearchObject(test10, "cyan");
    invokeAndAssert(test10, 0);

    // Test 11
    Map<String, Object> test11 = mapper.readValue(basic, tr);
    Map<String, Object> properties11_1 = new HashMap<>();
    properties11_1.put("key", "properties.name");
    properties11_1.put("operation", "EQUALS");
    properties11_1.put("values", Stream.of("Toyota", "Ducati", "BikeX").collect(Collectors.toList()));
    Map<String, Object> properties11_2 = new HashMap<>();
    properties11_2.put("key", "properties.size");
    properties11_2.put("operation", "EQUALS");
    properties11_2.put("values", Stream.of(1D, 0.3D).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test11, properties11_1, properties11_2);
    invokeAndAssert(test11, 2, "Ducati", "BikeX");

    // Test 12
    Map<String, Object> test12 = mapper.readValue(basic, tr);
    Map<String, Object> properties12_1 = new HashMap<>();
    properties12_1.put("key", "properties.name");
    properties12_1.put("operation", "EQUALS");
    properties12_1.put("values", Stream.of("Toyota", "Ducati").collect(Collectors.toList()));
    Map<String, Object> properties12_2 = new HashMap<>();
    properties12_2.put("key", "properties.name");
    properties12_2.put("operation", "EQUALS");
    properties12_2.put("values", Stream.of("Toyota").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test12, properties12_1);
    addPropertiesQueryToSearchObject(test12, true, properties12_2);
    invokeAndAssert(test12, 2, "Toyota", "Ducati");

    // Test 13
    Map<String, Object> test13 = mapper.readValue(basic, tr);
    Map<String, Object> properties13_1 = new HashMap<>();
    properties13_1.put("key", "properties.name");
    properties13_1.put("operation", "EQUALS");
    properties13_1.put("values", Stream.of("Toyota").collect(Collectors.toList()));
    Map<String, Object> properties13_2 = new HashMap<>();
    properties13_2.put("key", "properties.name");
    properties13_2.put("operation", "EQUALS");
    properties13_2.put("values", Stream.of("Ducati").collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test13, properties13_1);
    addPropertiesQueryToSearchObject(test13, true, properties13_2);
    invokeAndAssert(test13, 2, "Toyota", "Ducati");

    // Test 14
    Map<String, Object> test14 = mapper.readValue(basic, tr);
    Map<String, Object> properties14_1 = new HashMap<>();
    properties14_1.put("key", "id");
    properties14_1.put("operation", "GREATER_THAN");
    properties14_1.put("values", Stream.of(0).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test14, properties14_1);

    String response = invokeLambda(mapper.writeValueAsString(test14));
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    List<Feature> responseFeatures = responseCollection.getFeatures();
    String id = responseFeatures.get(0).getId();
    assertEquals("Check size", 4, responseFeatures.size());

    Map<String, Object> test15 = mapper.readValue(basic, tr);
    Map<String, Object> properties15_1 = new HashMap<>();
    properties15_1.put("key", "id");
    properties15_1.put("operation", "EQUALS");
    properties15_1.put("values", Stream.of(id).collect(Collectors.toList()));
    addPropertiesQueryToSearchObject(test15, properties15_1);

    response = invokeLambda(mapper.writeValueAsString(test15));
    responseCollection = XyzSerializable.deserialize(response);
    responseFeatures = responseCollection.getFeatures();
    assertEquals(1, responseFeatures.size());

    // Test 15
    Map<String, Object> test16 = mapper.readValue(basic, tr);
    test16.put("type", "IterateFeaturesEvent");
    test16.put("handle", "1");
    invokeAndAssert(test16, 3, "Tesla", "Ducati", "BikeX");
  }

  private void invokeAndAssert(Map<String, Object> json, int size, String... names) throws Exception {
    String response = invokeLambda(new ObjectMapper().writeValueAsString(json));

    final FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    final List<Feature> responseFeatures = responseCollection.getFeatures();
    assertEquals("Check size", size, responseFeatures.size());

    for (int i = 0; i < size; i++) {
      assertEquals("Check name", names[i], responseFeatures.get(i).getProperties().get("name"));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addTagsToSearchObject(Map<String, Object> json, String... tags) {
    json.remove("tags");
    json.put("tags", new ArrayList<String>());
    ((List) json.get("tags")).add(new ArrayList(Arrays.asList(tags)));
  }

  @SafeVarargs
  private final void addPropertiesQueryToSearchObject(Map<String, Object> json, Map<String, Object>... objects) {
    addPropertiesQueryToSearchObject(json, false, objects);
  }

  @SafeVarargs
  private final void addPropertiesQueryToSearchObject(Map<String, Object> json, boolean or, Map<String, Object>... objects) {
    if (!json.containsKey("propertiesQuery")) {
      json.put("propertiesQuery", new ArrayList<List<Map<String, Object>>>());
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) final List<List<Map<String, Object>>> list = (List) json.get("propertiesQuery");
    if (or) {
      list.add(new ArrayList<>(Stream.of(objects).collect(Collectors.toList())));
      return;
    }

    if (list.size() == 0) {
      list.add(new ArrayList<>(Stream.of(objects).collect(Collectors.toList())));
      return;
    }

    list.get(0).addAll(Stream.of(objects).collect(Collectors.toList()));
  }

  @Test
  public void testDeleteFeaturesByTagDefault() throws Exception {
    testDeleteFeaturesByTag(false);
  }

  @Test
  public void testDeleteFeaturesByTagWithOldStates() throws Exception {
    testDeleteFeaturesByTag(true);
  }

  @Test
  public void testNullGeometry() throws Exception {

    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertNullGeometry.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    logger.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    logger.info("Preparation: Insert features");

    // =========== Validate that "geometry":null is serialized ==========
    String response = invokeLambdaFromFile("/events/GetFeaturesByIdEvent.json");
    assertTrue(response.indexOf("\"geometry\":null") > 0);
  }

  private void testDeleteFeaturesByTag(boolean includeOldStates) throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    logger.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    logger.info("Preparation: Insert features");

    // =========== COUNT ==========
    String countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
    Integer originalCount = JsonPath.read(countResponse, "$.count");
    logger.info("Preparation: feature count = {}", originalCount);

    // =========== DELETE SOME TAGGED FEATURES ==========
    logger.info("Delete tagged features");
    final DocumentContext deleteByTagEventDoc = getEventFromResource("/events/DeleteFeaturesByTagEvent.json");
    deleteByTagEventDoc.put("$", "params", Collections.singletonMap("includeOldStates", includeOldStates));
    String[][] tags = {{"yellow"}};
    deleteByTagEventDoc.put("$", "tags", tags);
    String deleteByTagEvent = deleteByTagEventDoc.jsonString();
    String deleteByTagResponse = invokeLambda(deleteByTagEvent);
    assertNoErrorInResponse(deleteByTagResponse);
    final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
    @SuppressWarnings("rawtypes") List features = jsonPathFeatures.read(deleteByTagResponse, jsonPathConf);
    if (includeOldStates) {
      assertNotNull("'features' element in DeleteByTagResponse is missing", features);
      assertTrue("'features' element in DeleteByTagResponse is empty", features.size() > 0);
    } else if (features != null) {
      assertEquals("unexpected features in DeleteByTagResponse", 0, features.size());
    }

    countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
    Integer count = JsonPath.read(countResponse, "$.count");
    assertTrue(originalCount > count);
    logger.info("Delete tagged features tested successfully");

    // =========== DELETE ALL FEATURES ==========
    deleteByTagEventDoc.put("$", "tags", null);
    String deleteAllEvent = deleteByTagEventDoc.jsonString();
    String deleteAllResponse = invokeLambda(deleteAllEvent);
    assertNoErrorInResponse(deleteAllResponse);
    features = jsonPathFeatures.read(deleteAllResponse, jsonPathConf);
    if (features != null) {
      assertEquals("unexpected features in DeleteByTagResponse", 0, features.size());
    }

    // TODO use deleted.length() when it's available
//    if (includeOldStates) {
//      final JsonPath jsonPathDeletedCount = JsonPath.compile("$.deletedCount");
//      Integer deletedCount = jsonPathDeletedCount.read(deleteAllResponse, jsonPathConf);
//      assertNotNull("'deletedCount' element in DeleteByTagResponse is missing", deletedCount);
//    }

    countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
    count = JsonPath.read(countResponse, "$.count");
    assertEquals(0, count.intValue());
    logger.info("Delete all features tested successfully");
  }

  @Test
  public void testModifyFeaturesDefault() throws Exception {
    testModifyFeatures(false);
  }

  @Test
  public void testModifyFeaturesWithOldStates() throws Exception {
    testModifyFeatures(true);
  }

  @Test
  public void testDeleteFeatures() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    logger.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
    List<Map> originalFeatures = jsonPathFeatures.read(insertResponse, jsonPathConf);

    final JsonPath jsonPathFeatureIds = JsonPath.compile("$.features..id");
    List<String> ids = jsonPathFeatureIds.read(insertResponse, jsonPathConf);
    logger.info("Preparation: Inserted features {}", ids);

    // =========== DELETE ==========
    final DocumentContext modifyFeaturesEventDoc = getEventFromResource("/events/InsertFeaturesEvent.json");
    modifyFeaturesEventDoc.delete("$.insertFeatures");

    Map<String, String> idsMap = new HashMap<>();
    ids.forEach(id -> idsMap.put(id, null));
    modifyFeaturesEventDoc.put("$", "deleteFeatures", idsMap);

    String deleteEvent = modifyFeaturesEventDoc.jsonString();
    String deleteResponse = invokeLambda(deleteEvent);
    assertNoErrorInResponse(deleteResponse);
    logger.info("Modify features tested successfully");

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void testModifyFeatures(boolean includeOldStates) throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    logger.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
    List<Map> originalFeatures = jsonPathFeatures.read(insertResponse, jsonPathConf);

    final JsonPath jsonPathFeatureIds = JsonPath.compile("$.features..id");
    List<String> ids = jsonPathFeatureIds.read(insertResponse, jsonPathConf);
    logger.info("Preparation: Inserted features {}", ids);

    // =========== UPDATE ==========
    logger.info("Modify features");
    final DocumentContext updateFeaturesEventDoc = getEventFromResource("/events/InsertFeaturesEvent.json");
    updateFeaturesEventDoc.put("$", "params", Collections.singletonMap("includeOldStates", includeOldStates));

    List<Map> updateFeatures = jsonPathFeatures.read(insertResponse, jsonPathConf);
    updateFeaturesEventDoc.delete("$.insertFeatures");
    updateFeatures.forEach((Map feature) -> {
      final Map<String, Object> properties = (Map<String, Object>) feature.get("properties");
      properties.put("test", "updated");
    });
    updateFeaturesEventDoc.put("$", "updateFeatures", updateFeatures);

    String updateFeaturesEvent = updateFeaturesEventDoc.jsonString();
    String updateFeaturesResponse = invokeLambda(updateFeaturesEvent);
    assertNoErrorInResponse(updateFeaturesResponse);

    List features = jsonPathFeatures.read(updateFeaturesResponse, jsonPathConf);
    assertNotNull("'features' element in ModifyFeaturesResponse is missing", features);
    assertTrue("'features' element in ModifyFeaturesResponse is empty", features.size() > 0);

    final JsonPath jsonPathOldFeatures = JsonPath.compile("$.oldFeatures");
    List oldFeatures = jsonPathOldFeatures.read(updateFeaturesResponse, jsonPathConf);
    if (includeOldStates) {
      assertNotNull("'oldFeatures' element in ModifyFeaturesResponse is missing", oldFeatures);
      assertTrue("'oldFeatures' element in ModifyFeaturesResponse is empty", oldFeatures.size() > 0);
      assertEquals(oldFeatures, originalFeatures);
    } else if (oldFeatures != null) {
      assertEquals("unexpected oldFeatures in ModifyFeaturesResponse", 0, oldFeatures.size());
    }

    // =========== DELETE ==========
    final DocumentContext modifyFeaturesEventDoc = getEventFromResource("/events/InsertFeaturesEvent.json");
    modifyFeaturesEventDoc.put("$", "params", Collections.singletonMap("includeOldStates", includeOldStates));
    modifyFeaturesEventDoc.delete("$.insertFeatures");

    Map<String, String> idsMap = new HashMap<>();
    ids.forEach(id -> idsMap.put(id, null));
    modifyFeaturesEventDoc.put("$", "deleteFeatures", idsMap);

    String deleteEvent = modifyFeaturesEventDoc.jsonString();
    String deleteResponse = invokeLambda(deleteEvent);
    assertNoErrorInResponse(deleteResponse);
    oldFeatures = jsonPathOldFeatures.read(deleteResponse, jsonPathConf);
    if (includeOldStates) {
      assertNotNull("'oldFeatures' element in ModifyFeaturesResponse is missing", oldFeatures);
      assertTrue("'oldFeatures' element in ModifyFeaturesResponse is empty", oldFeatures.size() > 0);
      assertEquals(oldFeatures, features);
    } else if (oldFeatures != null) {
      assertEquals("unexpected oldFeatures in ModifyFeaturesResponse", 0, oldFeatures.size());
    }

    logger.info("Modify features tested successfully");
  }

  private static final Configuration jsonPathConf = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);

  private void assertNoErrorInResponse(String response) {
    assertNull(JsonPath.compile("$.error").read(response, jsonPathConf));
  }

  private void assertReadFeatures(String space, boolean checkGuid, List<Feature> requestFeatures, List<Feature> responseFeatures) {
    if (requestFeatures == null) {
      return;
    }
    for (int i = 0; i < requestFeatures.size(); i++) {
      Feature requestFeature = requestFeatures.get(i);
      Feature responseFeature = responseFeatures.get(i);
      assertTrue("Check geometry", jsonCompare(requestFeature.getGeometry(), responseFeature.getGeometry()));
      assertEquals("Check name", (String) requestFeature.getProperties().get("name"), responseFeature.getProperties().get("name"));
      assertNotNull("Check id", responseFeature.getId());
      assertTrue("Check tags", jsonCompare(requestFeature.getProperties().getXyzNamespace().getTags(),
          responseFeature.getProperties().getXyzNamespace().getTags()));
      assertEquals("Check space", space, responseFeature.getProperties().getXyzNamespace().getSpace());
      assertNotEquals("Check createdAt", 0L, responseFeature.getProperties().getXyzNamespace().getCreatedAt());
      assertNotEquals("Check updatedAt", 0L, responseFeature.getProperties().getXyzNamespace().getUpdatedAt());
      assertNull("Check parent", responseFeature.getProperties().getXyzNamespace().getPuuid());

      if (checkGuid) {
        assertNotNull("Check uuid", responseFeature.getProperties().getXyzNamespace().getUuid());
      } else {
        assertNull("Check uuid", responseFeature.getProperties().getXyzNamespace().getUuid());
      }
    }
  }

  private void assertRead(String insertRequest, String response, boolean checkGuid) throws Exception {
    final FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    final List<Feature> responseFeatures = responseCollection.getFeatures();

    final ModifyFeaturesEvent gsModifyFeaturesEvent = XyzSerializable.deserialize(insertRequest);
    List<Feature> modifiedFeatures;

    modifiedFeatures = gsModifyFeaturesEvent.getInsertFeatures();
    assertReadFeatures(gsModifyFeaturesEvent.getSpace(), checkGuid, modifiedFeatures, responseFeatures);

    modifiedFeatures = gsModifyFeaturesEvent.getUpsertFeatures();
    assertReadFeatures(gsModifyFeaturesEvent.getSpace(), checkGuid, modifiedFeatures, responseFeatures);
  }

  private void assertUpdate(String updateRequest, String response, boolean checkGuid) throws Exception {
    ModifyFeaturesEvent gsModifyFeaturesEvent = XyzSerializable.deserialize(updateRequest);
    FeatureCollection featureCollection = XyzSerializable.deserialize(response);
    for (int i = 0; i < gsModifyFeaturesEvent.getUpdateFeatures().size(); i++) {
      Feature expectedFeature = gsModifyFeaturesEvent.getUpdateFeatures().get(i);
      Feature actualFeature = featureCollection.getFeatures().get(i);
      assertTrue("Check geometry", jsonCompare(expectedFeature.getGeometry(), actualFeature.getGeometry()));
      assertEquals("Check name", (String) expectedFeature.getProperties().get("name"), actualFeature.getProperties().get("name"));
      assertNotNull("Check id", actualFeature.getId());

      assertTrue("Check tags", jsonCompare(expectedFeature.getProperties().getXyzNamespace().getTags(),
          actualFeature.getProperties().getXyzNamespace().getTags()));
      assertEquals("Check space", gsModifyFeaturesEvent.getSpace(), actualFeature.getProperties().getXyzNamespace().getSpace());
      assertNotEquals("Check createdAt", 0L, actualFeature.getProperties().getXyzNamespace().getCreatedAt());
      assertNotEquals("Check updatedAt", 0L, actualFeature.getProperties().getXyzNamespace().getUpdatedAt());
      if (checkGuid) {
        assertEquals("Check parent", expectedFeature.getProperties().getXyzNamespace().getUuid(),
            actualFeature.getProperties().getXyzNamespace().getPuuid());
        assertNotNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
      } else {
        assertNull("Check parent", actualFeature.getProperties().getXyzNamespace().getPuuid());
        assertNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
      }
    }
  }

  private void assertCount(String insertRequest, String countResponse) {
    if (!JsonPath.<Boolean>read(countResponse, "$.estimated")) {
      assertEquals("Check inserted feature count vs fetched count", JsonPath.read(insertRequest, "$.insertFeatures.length()").toString(),
          JsonPath.read(countResponse, "$.count").toString());
    }
  }

  private void assertDeleteSpaceResponse(String deleteResponse) {
    assertEquals("Check delete space", JsonPath.read(deleteResponse, "$.status").toString(), "OK");
  }

  private DocumentContext getEventFromResource(String file) {
    InputStream inputStream = GSContext.class.getResourceAsStream(file);
    return JsonPath.parse(inputStream);
  }

  private static String invokeLambdaFromFile(String file) throws Exception {
    InputStream jsonStream = GSContext.class.getResourceAsStream(file);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    lambda.handleRequest(jsonStream, os, GSContext.newLocal());
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    logger.info("Response from lambda - {}", response);
    return response;
  }

  private String invokeLambda(String request) throws Exception {
    logger.info("Request to lambda - {}", request);
    InputStream jsonStream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    lambda.handleRequest(jsonStream, os, GSContext.newLocal());
    String response = IOUtils.toString(
        Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    logger.info("Response from lambda - {}", response);
    return response;
  }

  private static boolean jsonCompare(Object o1, Object o2) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode tree1 = mapper.convertValue(o1, JsonNode.class);
    JsonNode tree2 = mapper.convertValue(o2, JsonNode.class);
    return tree1.equals(tree2);
  }
}
