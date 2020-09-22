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

package com.here.xyz.psql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.MultiPolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.MultiPolygon;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.PropertiesStatistics;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class PSQLXyzConnectorIT extends PSQLAbstractIT {

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
    String response = invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());

    response = invokeLambdaFromFile("/events/DeleteSpaceFooTestEvent.json");
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());

    logger.info("Setup Completed.");
  }

  @After
  public void shutdown() throws Exception {
    logger.info("Shutdown...");
    invokeLambdaFromFile("/events/DeleteSpaceEvent.json");
    invokeLambdaFromFile("/events/DeleteSpaceFooTestEvent.json");
    logger.info("Shutdown Completed.");
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

  @Test
  public void testHistoryTableCreation() throws Exception {
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withSpaceDefinition(new Space().withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true));
    String response = invokeLambda(mse.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid)," +
              "(SELECT (to_regclass('\"foo\"') IS NOT NULL) as hst_table_exists) " +
              "FROM pg_trigger " +
              "WHERE tgname = 'tr_foo_history_writer';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if(!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      }else{
        assertTrue(resultSet.getBoolean("hst_table_exists"));
      }
    }
  }

  @Test
  public void testFullHistoryTableWriting() throws Exception {
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withConnectorParams(new HashMap<String,Object>(){{put("compactHistory", false);}})
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
            );

    String response = invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50, 8));
    Feature f = new Feature()
            .withId("1234")
            .withGeometry(point)
            .withProperties(new Properties().with("foo", 0).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    mfevent.setInsertFeatures(null);
    for (int i = 1; i <= 5; i++) {
      f.getProperties().with("foo", i);
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    // ============= DELETE ======================
    mfevent.setDeleteFeatures(new HashMap<String,String>(){{put("1234",null);}});
    mfevent.setUpdateFeatures(null);
    setPUUID(collection);
    invokeLambda(mfevent.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC;";

      ResultSet resultSet = stmt.executeQuery(sql);
      int oldestFooValue = 0;
      int rowCount = 0;

      // Check if 5 last versions are available in history table
      while (resultSet.next()) {
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));

        rowCount++;
        if(rowCount == 6){
          // Here we will find the deleted object which has the foo=5 from the last version.
          assertEquals(oldestFooValue, (int) feature.getProperties().get("foo"));
        }
        else
          assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
      }
      // Check if history table has 7 entries (1x insert +5x update +1x delete)
      assertEquals(7, rowCount);
    }
  }

  @Test
  public void testHistoryTableWriting() throws Exception {
    int maxVersionCount = 5;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
                    .withMaxVersionCount(maxVersionCount)
            );

    String response = invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50,8));
    Feature f = new Feature()
            .withId("1234")
            .withGeometry(point)
      .withProperties(new Properties().with("foo", 0).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    mfevent.setInsertFeatures(null);
    for (int i = 1; i <= 10 ; i++) {
      f.getProperties().with("foo",i);
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC;";

      ResultSet resultSet = stmt.executeQuery(sql);
      int oldestFooValue = 5;
      int rowCount = 0;

      // Check if 5 last versions are available in history table
      while (resultSet.next()){
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++,(int)feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has only 5 entries
      assertEquals(5,rowCount);
    }

    // set history to infinite
    mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.UPDATE)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
                    .withMaxVersionCount(-1)
            );
    invokeLambda(mse.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    for (int i = 11; i <= 20 ; i++) {
      f.getProperties().with("foo",i);//(new Properties().with("foo", i).withXyzNamespace(xyzNamespace));
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC;";

      ResultSet resultSet = stmt.executeQuery(sql);
      //Oldes history item has foo=9
      int oldestFooValue = 5;
      int rowCount = 0;

      // Check if all versions are available
      while (resultSet.next()){
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++,(int)feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has 15 entries
      assertEquals(15,rowCount);
    }

    // set withMaxVersionCount to 2
    mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.UPDATE)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
                    .withMaxVersionCount(2)
            );
    invokeLambda(mse.serialize());

    //Do one Update to fire the updated trigger
    f.getProperties().with("foo",21);
    mfevent.setUpdateFeatures(collection.getFeatures());
    setPUUID(collection);
    invokeLambda(mfevent.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC;";

      ResultSet resultSet = stmt.executeQuery(sql);
      //Oldest history item has foo=19 - all other should be deleted related to maxVersionCount=2 update
      int oldestFooValue = 19;
      int rowCount = 0;

      // Check if only two versions are left in the history
      while (resultSet.next()) {
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has 2 entries
      assertEquals(2,rowCount);
    }
  }

  @Test
  public void testHistoryTableDeletedFlag() throws Exception {
    int maxVersionCount = 5;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withParams(new HashMap<String,Object>(){{put("maxVersionCount", maxVersionCount);}})
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true));

    String response = invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50,8));
    Feature f = new Feature()
            .withId("1234")
            .withGeometry(point)
            .withProperties(new Properties().with("foo", 0).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    //DELETE feature
    mfevent.setUpdateFeatures(null);
    mfevent.setDeleteFeatures(new HashMap<String,String>(){{put("1234",null);}});
    invokeLambda(mfevent.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC LIMIT 1;";

      ResultSet resultSet = stmt.executeQuery(sql);
      resultSet.next();
      Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
      assertTrue(feature.getProperties().getXyzNamespace().isDeleted());
    }
  }

  @Test
  public void testHistoryTableTrigger() throws Exception {
    int maxVersionCount = 8;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
                    .withMaxVersionCount(maxVersionCount)
            );

    String response = invokeLambda(mse.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid) as trigger_def " +
              "FROM pg_trigger " +
              "WHERE tgname = 'tr_foo_history_writer';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if(!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      }else{
        assertTrue(resultSet.getString("trigger_def").contains("xyz_trigger_historywriter('"+maxVersionCount+"')"));
      }
    }
  }

  @Test
  public void testFullHistoryTableTrigger() throws Exception {
    int maxVersionCount = -1;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withConnectorParams(new HashMap<String,Object>(){{put("compactHistory", false);}})
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withEnableUUID(true)
                    .withEnableHistory(true)
                    .withMaxVersionCount(maxVersionCount)
            );

    String response = invokeLambda(mse.serialize());

    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid) as trigger_def " +
              "FROM pg_trigger " +
              "WHERE tgname = 'tr_foo_history_writer';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if(!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      }else{
        assertTrue(resultSet.getString("trigger_def").contains("xyz_trigger_historywriter_full('"+maxVersionCount+"')"));
      }
    }
  }

  @Test
  public void testTransactionalUUIDCases() throws Exception {
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);

    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== UPDATE With wrong UUID ==========
    FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
    for (Feature feature : featureCollection.getFeatures()) {
      feature.getProperties().put("foo","bar");
    }

    String modifiedFeatureId = featureCollection.getFeatures().get(1).getId();
    featureCollection.getFeatures().get(1).getProperties().getXyzNamespace().setUuid("wrong");

    Feature newFeature = new Feature().withId("test2").withProperties(new Properties().withXyzNamespace(xyzNamespace));
    List<Feature> insertFeatureList = new ArrayList<>();
    insertFeatureList.add(newFeature);

    List<String> idList = featureCollection.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
    idList.add("test2");

    setPUUID(featureCollection);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setUpdateFeatures(featureCollection.getFeatures());
    mfevent.setInsertFeatures(insertFeatureList);
    String response = invokeLambda(mfevent.serialize());
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);

    assertEquals(0, responseCollection.getFeatures().size());
    assertEquals(4, responseCollection.getFailed().size());

    // Transaction should have failed
    for (FeatureCollection.ModificationFailure failure : responseCollection.getFailed()) {
      if(failure.getId().equalsIgnoreCase(modifiedFeatureId))
        assertEquals("Object does not exist or UUID mismatch",failure.getMessage());
      else
        assertEquals("Transaction has failed",failure.getMessage());
      // Check if Id correct
      assertTrue(idList.contains(failure.getId()));
    }

    //Check if nothing got written
    SearchForFeaturesEvent searchEvent = new SearchForFeaturesEvent();
    searchEvent.setSpace("foo");
    searchEvent.setStreamId(RandomStringUtils.randomAlphanumeric(10));
    String eventJson = searchEvent.serialize();
    String searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);

    for (Feature feature : responseCollection.getFeatures()) {
      assertNull(feature.getProperties().get("foo"));
    }

    // =========== UPDATE With correct UUID ==========
    for (Feature feature : responseCollection.getFeatures()) {
      feature.getProperties().put("foo","bar");
    }

    setPUUID(responseCollection);

    mfevent.setUpdateFeatures(responseCollection.getFeatures());
    mfevent.setInsertFeatures(new ArrayList<>());

    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);
    assertEquals(3, responseCollection.getFeatures().size());
    assertEquals(3, responseCollection.getUpdated().size());
    assertNull(responseCollection.getFailed());

    // Check returned FeatureCollection
    for (Feature feature : responseCollection.getFeatures()) {
      assertEquals("bar",feature.getProperties().get("foo"));
    }

    // Check if updates got performed
    searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);
    assertEquals(3, responseCollection.getFeatures().size());

    for (Feature feature : responseCollection.getFeatures()) {
      assertEquals("bar",feature.getProperties().get("foo"));
    }

    // =========== Delete With wrong UUID ==========
    Map<String,String> idUUIDMap = new HashMap<>();
    Map<String,String> idMap = new HashMap<>();
    //get current UUIDS
    for (Feature feature : responseCollection.getFeatures()) {
      idUUIDMap.put(feature.getId(),feature.getProperties().getXyzNamespace().getUuid());
      idMap.put(feature.getId(),null);
    }

    idUUIDMap.put(modifiedFeatureId, "wrong");
    mfevent.setUpdateFeatures(new ArrayList<>());
    mfevent.setDeleteFeatures(idUUIDMap);

    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);

    assertEquals(0, responseCollection.getFeatures().size());
    assertEquals(3, responseCollection.getFailed().size());

    // Transaction should have failed
    for (FeatureCollection.ModificationFailure failure : responseCollection.getFailed()) {
      if(failure.getId().equalsIgnoreCase(modifiedFeatureId))
        assertEquals("Object does not exist or UUID mismatch",failure.getMessage());
      else
        assertEquals("Transaction has failed",failure.getMessage());
      // Check if Id correct
      assertTrue(idList.contains(failure.getId()));
    }

    // Check if deletes has failed
    searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);
    assertEquals(3, responseCollection.getFeatures().size());

    // =========== Delete without UUID ==========
    mfevent.setDeleteFeatures(idMap);
    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);

    assertEquals(0, responseCollection.getFeatures().size());
    assertNull(responseCollection.getFailed());

    // Check if deletes are got performed
    searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);
    assertEquals(0, responseCollection.getFeatures().size());
  }

  @Test
  public void testStreamUUIDCases() throws Exception {
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L).withUuid("4e16d729-e4f7-4ea9-b0da-4af4ac53c5c4");

    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== UPDATE With wrong UUID ==========
    FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
    for (Feature feature : featureCollection.getFeatures()) {
      feature.getProperties().put("foo", "bar");
    }

    String modifiedFeatureId = featureCollection.getFeatures().get(1).getId();
    featureCollection.getFeatures().get(1).getProperties().getXyzNamespace().setUuid("wrong");

    Feature newFeature = new Feature().withId("test2").withProperties(new Properties().withXyzNamespace(xyzNamespace));
    List<Feature> insertFeatureList = new ArrayList<>();
    insertFeatureList.add(newFeature);

    List<String> idList = featureCollection.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
    idList.add("test2");

    setPUUID(featureCollection);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(false);
    mfevent.setEnableUUID(true);
    mfevent.setUpdateFeatures(featureCollection.getFeatures());
    mfevent.setInsertFeatures(insertFeatureList);
    String response = invokeLambda(mfevent.serialize());
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);

    assertEquals(3, responseCollection.getFeatures().size());
    assertEquals(1, responseCollection.getFailed().size());
    assertTrue(responseCollection.getFailed().get(0).getId().equalsIgnoreCase(modifiedFeatureId));

    List<String> inserted = responseCollection.getInserted();

    FeatureCollection.ModificationFailure failure = responseCollection.getFailed().get(0);
    assertEquals(DatabaseWriter.UPDATE_ERROR_UUID,failure.getMessage());
    assertEquals(modifiedFeatureId,failure.getId());

    //Check if updates got written (correct UUID)
    SearchForFeaturesEvent searchEvent = new SearchForFeaturesEvent();
    searchEvent.setSpace("foo");
    searchEvent.setStreamId(RandomStringUtils.randomAlphanumeric(10));
    String eventJson = searchEvent.serialize();
    String searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);

    for (Feature feature : responseCollection.getFeatures()) {
      // The new Feature and the failed updated one should not have the property foo
      if(feature.getId().equalsIgnoreCase(modifiedFeatureId) || feature.getId().equalsIgnoreCase(inserted.get(0)))
        assertNull(feature.getProperties().get("foo"));
      else
        assertEquals("bar",feature.getProperties().get("foo"));
      assertTrue(idList.contains(failure.getId()));
    }

    // =========== UPDATE With correct UUID ==========
    for (Feature feature : responseCollection.getFeatures()) {
      feature.getProperties().put("foo","bar2");
    }

    setPUUID(responseCollection);

    mfevent.setUpdateFeatures(responseCollection.getFeatures());
    mfevent.setInsertFeatures(new ArrayList<>());

    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);
    assertEquals(4, responseCollection.getFeatures().size());
    assertEquals(4, responseCollection.getUpdated().size());
    assertNull(responseCollection.getFailed());

    for (Feature feature : responseCollection.getFeatures()) {
      assertEquals("bar2",feature.getProperties().get("foo"));
    }

    // =========== Delete With wrong UUID ==========
    Map<String,String> idUUIDMap = new HashMap<>();
    Map<String,String> idMap = new HashMap<>();
    //get current UUIDS
    for (Feature feature : responseCollection.getFeatures()) {
      idUUIDMap.put(feature.getId(),feature.getProperties().getXyzNamespace().getUuid());
      idMap.put(feature.getId(),null);
    }

    idUUIDMap.put(modifiedFeatureId, "wrong");
    mfevent.setUpdateFeatures(new ArrayList<>());
    mfevent.setDeleteFeatures(idUUIDMap);

    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);

    assertEquals(0, responseCollection.getFeatures().size());
    assertEquals(3, responseCollection.getDeleted().size());
    assertEquals(1, responseCollection.getFailed().size());

    // Only the feature with wrong UUID should have failed
    failure = responseCollection.getFailed().get(0);
    assertEquals(DatabaseWriter.UPDATE_ERROR_UUID,failure.getMessage());
    assertEquals(modifiedFeatureId, failure.getId());

    // Check if deletes are got performed
    searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);
    assertEquals(1, responseCollection.getFeatures().size());

    // =========== Delete without UUID ==========
    mfevent.setDeleteFeatures(idMap);
    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);

    assertEquals(0, responseCollection.getFeatures().size());
    assertEquals(1, responseCollection.getDeleted().size());
    assertEquals(modifiedFeatureId, (responseCollection.getDeleted().get(0)));
    // Check if deletes are got performed
    searchResponse = invokeLambda(eventJson);
    responseCollection = XyzSerializable.deserialize(searchResponse);
    assertEquals(0, responseCollection.getFeatures().size());
  }

  private void setPUUID(FeatureCollection featureCollection) throws JsonProcessingException {
    for (Feature feature : featureCollection.getFeatures()){
      feature.getProperties().getXyzNamespace().setPuuid(feature.getProperties().getXyzNamespace().getUuid());
      feature.getProperties().getXyzNamespace().setUuid(UUID.randomUUID().toString());
    }
  }

  @Test
  public void testModifyFeatureFailuresWithUUID() throws Exception {
    testModifyFeatureFailures(true);
  }

  @Test
  public void testModifyFeatureFailuresWithoutUUID() throws Exception {
    testModifyFeatureFailures(false);
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

    // =========== QUERY BBOX - +TAGS ==========
    queryEvent = "{\n"
        + "\t\"margin\": 20,\n"
        + "\t\"streamId\": \"Z1YaJv1PCHCl00000waR\",\n"
        + "\t\"level\": 3,\n"
        + "\t\"bbox\": [-170, -170, 170, 170],\n"
        + "\t\"type\": \"GetFeaturesByBBoxEvent\",\n"
        + "\t\"space\": \"foo\",\n"
        + "\t\"tags\": [[\"yellow\"]],\n"
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
    setPUUID(featureCollection);
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

    FeatureCollection responseCollection = XyzSerializable.deserialize(updateResponse);
    setPUUID(responseCollection);

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
    setPUUID(featureCollection);
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
    FeatureCollection responseCollection = XyzSerializable.deserialize(updateResponse);

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
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    logger.info("Insert feature tested successfully");

    // =========== GetStatistics ==========
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

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(3).collect(Collectors.toList());

    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> {
          Feature f = new Feature()
              .withGeometry(
                  new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties().withXyzNamespace(xyzNamespace));
          pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
          return f;
        }).limit(11000).collect(Collectors.toList()));

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setInsertFeatures(collection.getFeatures()); // TODO use get11kFeatureCollection() and extract pKeys
    invokeLambda(mfevent.serialize());

    /* Needed to trigger update on pg_stat*/
    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("ANALYZE \"foo\";");
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
      if(prop.getKey().equalsIgnoreCase("name"))
        continue;
      assertTrue(pKeys.contains(prop.getKey()));
      assertEquals(prop.getCount() > 10000, true);
    }
  }

  @Test
  public void testAutoIndexing() throws Exception {
    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setInsertFeatures(get11kFeatureCollection().getFeatures());
    invokeLambda(mfevent.serialize());

    /** Needed to trigger update on pg_stat */
    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='foo';");
      stmt.execute("ANALYZE \"foo\";");
    }

    //Triggers dbMaintenance
    invokeLambdaFromFile("/events/HealthCheckEvent.json");

    // =========== GetStatistics ==========
    GetStatisticsEvent event = new GetStatisticsEvent();
    event.setSpace("foo");
    event.setStreamId(RandomStringUtils.randomAlphanumeric(10));
    String eventJson = event.serialize();
    String statisticsJson = invokeLambda(eventJson);
    StatisticsResponse response = XyzSerializable.deserialize(statisticsJson);

    assertNotNull(response);

    assertEquals(new Long(11000), response.getCount().getValue());
    assertEquals(true,  response.getCount().getEstimated());
    assertEquals(PropertiesStatistics.Searchable.PARTIAL, response.getProperties().getSearchable());

    List<PropertyStatistics> propStatistics = response.getProperties().getValue();
    for (PropertyStatistics propStat: propStatistics ) {
      if(propStat.getKey().equalsIgnoreCase("test")){
        assertEquals("number", propStat.getDatatype());
        assertEquals(false, propStat.isSearchable());
        assertTrue(propStat.getCount() < 11000);
      }else{
        assertEquals("string", propStat.getDatatype());
        assertEquals(true, propStat.isSearchable());
        assertEquals(11000 , propStat.getCount());
      }
    }

    /* Clean-up maintenance entry */
    try (final Connection connection = lambda.dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='foo';");
    }
  }

  @Test
  public void testUpsertFeature() throws Exception {
    invokeLambdaFromFile("/events/InsertFeaturesEvent.json");

    // =========== UPSERT ==========
    String jsonFile = "/events/UpsertFeaturesEvent.json";
    String response = invokeLambdaFromFile(jsonFile);
    logger.info("RAW RESPONSE: " + response);
    String request = IOUtils.toString(GSContext.class.getResourceAsStream(jsonFile));
    assertRead(request, response, false);
    logger.info("Upsert feature tested successfully");
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
}
