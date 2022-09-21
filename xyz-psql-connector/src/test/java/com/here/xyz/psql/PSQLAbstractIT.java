/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class PSQLAbstractIT {

  static final Logger logger = LogManager.getLogger();
  static Random random = new Random();
  static PSQLXyzConnector lambda;

  static Map<String, Object> defaultTestConnectorParams = new HashMap<String,Object>(){
    {put("connectorId","test-connector");put("propertySearch", true);}};

  public static void initEnv(Map<String, Object>  connectorParameters) throws Exception {
    logger.info("Setup environment...");

    lambda = new PSQLXyzConnector();
    lambda.reset();
    lambda.setEmbedded(true);

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;

    HealthCheckEvent event = new HealthCheckEvent()
      .withMinResponseTime(100)
      .withConnectorParams(connectorParameters);

    invokeLambda(event.serialize());
    logger.info("Setup environment Completed.");
  }

  public void deleteTestSpace(Map<String, Object>  connectorParameters) throws Exception {
    logger.info("Setup Test...");
    invokeDeleteTestSpace(connectorParameters);
    logger.info("Setup Test Completed.");
  }

  public void shutdownEnv(Map<String, Object>  connectorParameters) throws Exception {
    logger.info("Shutdown...");
    invokeDeleteTestSpace(connectorParameters);
    logger.info("Shutdown Completed.");
  }

  private void invokeDeleteTestSpace(Map<String, Object>  connectorParameters) throws Exception {
    logger.info("Setup Test...");

    connectorParameters = connectorParameters == null ? defaultTestConnectorParams : connectorParameters;
    ModifySpaceEvent mse = new ModifySpaceEvent()
            .withSpace("foo")
            .withOperation(ModifySpaceEvent.Operation.DELETE)
            .withConnectorParams(connectorParameters);

    String response = invokeLambda(mse.serialize());
    assertEquals("Check response status", "OK", JsonPath.read(response, "$.status").toString());

    logger.info("Setup Test Completed.");
  }

  void testModifyFeatureFailures(boolean withUUID) throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = withUUID ? "/events/InsertFeaturesEventTransactional.json" : "/events/InsertFeaturesEvent.json";
    final String insertResponse = invokeLambdaFromFile(insertJsonFile);
    final String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
    final FeatureCollection insertRequestCollection = XyzSerializable.deserialize(insertResponse);
    assertRead(insertRequest, insertResponse, withUUID);
    logger.info("Insert feature tested successfully");

    // =========== DELETE NOT EXISTING FEATURE ==========
    //Stream
    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
            .withConnectorParams(defaultTestConnectorParams)
            .withSpace("foo")
            .withTransaction(false)
            .withDeleteFeatures(Collections.singletonMap("doesnotexist", null));
    if(withUUID)
      mfevent.setEnableUUID(true);

    String response = invokeLambda(mfevent.serialize());
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    assertEquals("doesnotexist", responseCollection.getFailed().get(0).getId());
    assertEquals(0,responseCollection.getFeatures().size());
    assertNull(responseCollection.getUpdated());
    assertNull(responseCollection.getInserted());
    assertNull(responseCollection.getDeleted());

    if(withUUID)
      assertEquals(DatabaseWriter.DELETE_ERROR_UUID, responseCollection.getFailed().get(0).getMessage());
    else
      assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

    //Transactional
    mfevent.setTransaction(true);
    response = invokeLambda(mfevent.serialize());

    // Transaction should have failed
    ErrorResponse errorResponse = XyzSerializable.deserialize(response);
    assertEquals(XyzError.CONFLICT, errorResponse.getError());
    ArrayList failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
    assertEquals(1, failedList.size());

    HashMap<String,String> failure1 = ((HashMap<String,String>)failedList.get(0));
    assertEquals("doesnotexist", failure1.get("id"));

    if(withUUID)
      assertEquals(DatabaseWriter.DELETE_ERROR_UUID, failure1.get("message"));
    else
      assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, failure1.get("message"));

    // =========== INSERT EXISTING FEATURE ==========
    //Stream
    Feature existing = insertRequestCollection.getFeatures().get(0);
    existing.getProperties().getXyzNamespace().setPuuid(existing.getProperties().getXyzNamespace().getUuid());

    mfevent.setInsertFeatures(new ArrayList<Feature>(){{add(existing);}});
    mfevent.setDeleteFeatures(new HashMap<>());
    mfevent.setTransaction(false);
    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);
    assertEquals(existing.getId(), responseCollection.getFailed().get(0).getId());
    assertEquals(DatabaseWriter.INSERT_ERROR_GENERAL, responseCollection.getFailed().get(0).getMessage());
    assertEquals(0,responseCollection.getFeatures().size());
    assertNull(responseCollection.getUpdated());
    assertNull(responseCollection.getInserted());
    assertNull(responseCollection.getDeleted());

    //Transactional
    mfevent.setTransaction(true);
    response = invokeLambda(mfevent.serialize());

    errorResponse = XyzSerializable.deserialize(response);
    assertEquals(XyzError.CONFLICT, errorResponse.getError());
    failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
    assertEquals(0, failedList.size());
    assertEquals(DatabaseWriter.TRANSACTION_ERROR_GENERAL, errorResponse.getErrorMessage());

    // =========== UPDATE NOT EXISTING FEATURE ==========
    //Stream
    //Change ID to not existing one
    existing.setId("doesnotexist");
    mfevent.setInsertFeatures(new ArrayList<>());
    mfevent.setUpdateFeatures(new ArrayList<Feature>(){{add(existing);}});
    mfevent.setTransaction(false);

    response = invokeLambda(mfevent.serialize());
    responseCollection = XyzSerializable.deserialize(response);
    assertEquals(existing.getId(), responseCollection.getFailed().get(0).getId());
    assertEquals(0,responseCollection.getFeatures().size());
    assertNull(responseCollection.getUpdated());
    assertNull(responseCollection.getInserted());
    assertNull(responseCollection.getDeleted());

    if(withUUID)
      assertEquals(DatabaseWriter.UPDATE_ERROR_UUID, responseCollection.getFailed().get(0).getMessage());
    else
      assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

    //Transactional
    mfevent.setTransaction(true);
    response = invokeLambda(mfevent.serialize());

    errorResponse = XyzSerializable.deserialize(response);
    assertEquals(XyzError.CONFLICT, errorResponse.getError());
    failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
    assertEquals(1, failedList.size());

    failure1 = ((HashMap<String,String>)failedList.get(0));
    assertEquals("doesnotexist", failure1.get("id"));

    if(withUUID)
      assertEquals(DatabaseWriter.UPDATE_ERROR_UUID, failure1.get("message"));
    else
      assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, failure1.get("message"));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  void testModifyFeatures(boolean includeOldStates) throws Exception {
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

  static final Configuration jsonPathConf = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);

  void assertNoErrorInResponse(String response) {
    assertNull(JsonPath.compile("$.error").read(response, jsonPathConf));
  }

  void assertReadFeatures(String space, boolean checkGuid, List<Feature> requestFeatures, List<Feature> responseFeatures) {
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

  void assertRead(String insertRequest, String response, boolean checkGuid) throws Exception {
    final FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    final List<Feature> responseFeatures = responseCollection.getFeatures();

    final ModifyFeaturesEvent gsModifyFeaturesEvent = XyzSerializable.deserialize(insertRequest);
    List<Feature> modifiedFeatures;

    modifiedFeatures = gsModifyFeaturesEvent.getInsertFeatures();
    assertReadFeatures(gsModifyFeaturesEvent.getSpace(), checkGuid, modifiedFeatures, responseFeatures);

    modifiedFeatures = gsModifyFeaturesEvent.getUpsertFeatures();
    assertReadFeatures(gsModifyFeaturesEvent.getSpace(), checkGuid, modifiedFeatures, responseFeatures);
  }

  void assertUpdate(String updateRequest, String response, boolean checkGuid) throws Exception {
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
        assertNotNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid()); // After version 0.2.0
        assertNotNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
      } else {
        assertNull("Check parent", actualFeature.getProperties().getXyzNamespace().getPuuid());
        assertNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
      }
    }
  }

  void assertCount(String insertRequest, String countResponse) {
    if (!JsonPath.<Boolean>read(countResponse, "$.estimated")) {
      assertEquals("Check inserted feature count vs fetched count", JsonPath.read(insertRequest, "$.insertFeatures.length()").toString(),
          JsonPath.read(countResponse, "$.count").toString());
    }
  }

  void assertDeleteSpaceResponse(String deleteResponse) {
    assertEquals("Check delete space", JsonPath.read(deleteResponse, "$.status").toString(), "OK");
  }

  DocumentContext getEventFromResource(String file) {
    InputStream inputStream = GSContext.class.getResourceAsStream(file);
    return JsonPath.parse(inputStream);
  }

  static String invokeLambdaFromFile(String file) throws Exception {
    InputStream jsonStream = GSContext.class.getResourceAsStream(file);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    lambda.handleRequest(jsonStream, os, GSContext.newLocal());
    String response = IOUtils.toString(Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    logger.info("Response from lambda - {}", response);
    return response;
  }

  static String invokeLambda(String request) throws Exception {
    logger.info("Request to lambda - {}", request);
    InputStream jsonStream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    lambda.handleRequest(jsonStream, os, GSContext.newLocal());
    String response = IOUtils.toString(
        Payload.prepareInputStream(new ByteArrayInputStream(os.toByteArray())));
    logger.info("Response from lambda - {}", response);
    return response;
  }

  static boolean jsonCompare(Object o1, Object o2) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode tree1 = mapper.convertValue(o1, JsonNode.class);
    JsonNode tree2 = mapper.convertValue(o2, JsonNode.class);
    return tree1.equals(tree2);
  }

  static Feature generateFeature(XyzNamespace xyzNamespace, List<String> propertyKeys) {
    propertyKeys = propertyKeys == null ? Collections.emptyList() : propertyKeys;

    return new Feature()
        .withGeometry(
            new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
        .withProperties(propertyKeys.stream().reduce(new Properties().withXyzNamespace(xyzNamespace), (properties, k) -> {
          properties.put(k, RandomStringUtils.randomAlphanumeric(3));
          return properties;
        }, (a, b) -> a));
  }

  static FeatureCollection get11kFeatureCollection() throws Exception {
    final XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    final List<String> propertyKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(3).collect(Collectors.toList());

    FeatureCollection collection = new FeatureCollection();
    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> generateFeature(xyzNamespace, propertyKeys)).limit(11000).collect(Collectors.toList()));

    /** This property does not get auto-indexed */
    for (int i = 0; i < 11000 ; i++) {
      if(i % 5 == 0)
        collection.getFeatures().get(i).getProperties().with("test",1);
    }

    return collection;
  }

  static void setPUUID(FeatureCollection featureCollection) throws JsonProcessingException {
    for (Feature feature : featureCollection.getFeatures()){
      feature.getProperties().getXyzNamespace().setPuuid(feature.getProperties().getXyzNamespace().getUuid());
      feature.getProperties().getXyzNamespace().setUuid(UUID.randomUUID().toString());
    }
  }

  static void invokeAndAssert(Map<String, Object> json, int size, String... names) throws Exception {
    String response = invokeLambda(new ObjectMapper().writeValueAsString(json));

    final FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    final List<Feature> responseFeatures = responseCollection.getFeatures();
    assertEquals("Check size", size, responseFeatures.size());

    for (int i = 0; i < size; i++) {
      assertEquals("Check name", names[i], responseFeatures.get(i).getProperties().get("name"));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static void addTagsToSearchObject(Map<String, Object> json, String... tags) {
    json.remove("tags");
    json.put("tags", new ArrayList<String>());
    ((List) json.get("tags")).add(new ArrayList(Arrays.asList(tags)));
  }

  @SafeVarargs
  protected static final void addPropertiesQueryToSearchObject(Map<String, Object> json, Map<String, Object>... objects) {
    addPropertiesQueryToSearchObject(json, false, objects);
  }

  @SafeVarargs
  protected static final void addPropertiesQueryToSearchObject(Map<String, Object> json, boolean or, Map<String, Object>... objects) {
    if (!json.containsKey("propertiesQuery")) {
      json.put("propertiesQuery", new ArrayList<List<Map<String, Object>>>());
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) final List<List<Map<String, Object>>> list = (List) json.get("propertiesQuery");
    if (or) {
      list.add(Arrays.asList(objects));
      return;
    }

    if (list.size() == 0) {
      list.add(Arrays.asList(objects));
      return;
    }

    list.get(0).addAll(Arrays.asList(objects));
  }
}
