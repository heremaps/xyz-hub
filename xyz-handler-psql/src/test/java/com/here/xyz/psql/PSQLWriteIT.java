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
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PSQLWriteIT extends PSQLAbstractIT {

  @BeforeAll
  public static void init() throws Exception {
    initEnv(null);
  }

  @AfterAll
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(null);
  }

  @Test
  public void testTableCreated() throws Exception {
    String response = invokeLambdaFromFile("/events/TestCreateTable.json");
    assertEquals("Check response status", JsonPath.read(response, "$.type").toString(), "FeatureCollection");
  }

  @Test
  public void testUpsertFeature() throws Exception {
    invokeLambdaFromFile("/events/InsertFeaturesEvent.json");

    // =========== UPSERT ==========
    String jsonFile = "/events/UpsertFeaturesEvent.json";
    String response = invokeLambdaFromFile(jsonFile);
    LOGGER.info("RAW RESPONSE: " + response);
    String request = IOUtils.toString(this.getClass().getResourceAsStream(jsonFile));
    assertRead(request, response, false);
    LOGGER.info("Upsert feature tested successfully");
  }

  @Test
  public void testCrudFeatureWithHash() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventWithHash.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    LOGGER.info("Insert feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = JsonSerializable.deserialize(insertResponse);
    setPUUID(featureCollection);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    //mfevent.setConnectorParams(defaultTestConnectorParams);
    //mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setUpdateFeatures(featureCollection.getFeatures());

    String updateRequest = mfevent.serialize();
    updateRequest = updateRequest.replaceAll("Tesla", "Honda");
    String updateResponse = invokeLambda(updateRequest);

    FeatureCollection responseCollection = JsonSerializable.deserialize(updateResponse);
    setPUUID(responseCollection);

    assertUpdate(updateRequest, updateResponse, true);
    assertUpdate(updateRequest, updateResponse, true);
    LOGGER.info("Update feature tested successfully");
  }

  @Test
  public void testCrudFeatureWithTransaction() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, true);
    LOGGER.info("Insert feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = JsonSerializable.deserialize(insertResponse);
    setPUUID(featureCollection);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    //mfevent.setConnectorParams(defaultTestConnectorParams);
    //mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setUpdateFeatures(featureCollection.getFeatures());

    String updateResponse = invokeLambda(mfevent.serialize());

    assertUpdate(mfevent.serialize(), updateResponse, true);
    LOGGER.info("Update feature tested successfully");

    // =========== LoadFeaturesEvent ==========
    String loadFeaturesEvent = "/events/LoadFeaturesEvent.json";
    String loadResponse = invokeLambdaFromFile(loadFeaturesEvent);
    featureCollection = JsonSerializable.deserialize(loadResponse);
    assertNotNull(featureCollection.getFeatures());
    assertEquals(1, featureCollection.getFeatures().size());
    assertEquals("test", featureCollection.getFeatures().get(0).getId());

    // =========== DELETE ==========
    final String deleteId = featureCollection.getFeatures().get(0).getId();

    mfevent = new ModifyFeaturesEvent();
    //mfevent.setConnectorParams(defaultTestConnectorParams);
    //mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);
    mfevent.setDeleteFeatures(new HashMap<String, String>() {{
      put(deleteId, null);
    }});

    String deleteResponse = invokeLambda(mfevent.serialize());
    featureCollection = JsonSerializable.deserialize(deleteResponse);
    assertNotNull(featureCollection.getFeatures());
    assertEquals(0, featureCollection.getFeatures().size());
    assertEquals(1, featureCollection.getDeleted().size());
  }

  @Test
  public void testCrudFeatureWithoutHash() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    LOGGER.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    LOGGER.info("Insert feature tested successfully");

    // =========== COUNT ==========
    String statsResponse = invokeLambdaFromFile("/events/GetStatisticsEvent.json");
    assertCount(insertRequest, statsResponse);
    LOGGER.info("Count feature tested successfully");

    // =========== SEARCH ==========
    String searchResponse = invokeLambdaFromFile("/events/SearchForFeaturesEvent.json");
    assertRead(insertRequest, searchResponse, false);
    LOGGER.info("Search feature tested successfully");

    // =========== SEARCH WITH PROPERTIES ========
    String searchPropertiesResponse = invokeLambdaFromFile("/events/SearchForFeaturesByPropertiesEvent.json");
    assertRead(insertRequest, searchPropertiesResponse, false);
    LOGGER.info("Search Properties feature tested successfully");

    // =========== UPDATE ==========
    FeatureCollection featureCollection = JsonSerializable.deserialize(insertResponse);
    String featuresList = JsonSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {
    });

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    //mfevent.setConnectorParams(defaultTestConnectorParams);
    //mfevent.setSpaceId("foo");
    mfevent.setUpdateFeatures(featureCollection.getFeatures());

    String updateRequest = mfevent.serialize();
    updateRequest = updateRequest.replaceAll("Tesla", "Honda");
    String updateResponse = invokeLambda(updateRequest);

    assertUpdate(updateRequest, updateResponse, false);
    LOGGER.info("Update feature tested successfully");

    // =========== DELETE FEATURES ==========
    invokeLambdaFromFile("/events/DeleteFeaturesByTagEvent.json");
    LOGGER.info("Delete feature tested successfully");
  }

  @Test
  public void testNullGeometry() throws Exception {

    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertNullGeometry.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    LOGGER.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    LOGGER.info("Preparation: Insert features");

    // =========== Validate that "geometry":null is serialized ==========
    String response = invokeLambdaFromFile("/events/GetFeaturesByIdEvent.json");
    assertTrue(response.indexOf("\"geometry\":null") > 0);
  }

  @Test
  public void testModifyFeaturesDefault() throws Exception {
    testModifyFeatures(false);
  }

  @Test
  public void testModifyFeaturesWithOldStates() throws Exception {
    testModifyFeatures(true);
  }

  protected void assertUpdate(String updateRequest, String response, boolean checkGuid) throws Exception {
    ModifyFeaturesEvent gsModifyFeaturesEvent = JsonSerializable.deserialize(updateRequest);
    FeatureCollection featureCollection = JsonSerializable.deserialize(response);
    for (int i = 0; i < gsModifyFeaturesEvent.getUpdateFeatures().size(); i++) {
      Feature expectedFeature = gsModifyFeaturesEvent.getUpdateFeatures().get(i);
      Feature actualFeature = featureCollection.getFeatures().get(i);
      assertTrue(jsonCompare(expectedFeature.getGeometry(), actualFeature.getGeometry()));
      assertEquals((String) expectedFeature.getProperties().get("name"), actualFeature.getProperties().get("name"));
      assertNotNull(actualFeature.getId());

      assertTrue(jsonCompare(expectedFeature.getProperties().getXyzNamespace().getTags(),
          actualFeature.getProperties().getXyzNamespace().getTags()));
      assertNotEquals(0L, actualFeature.getProperties().getXyzNamespace().getCreatedAt());
      assertNotEquals(0L, actualFeature.getProperties().getXyzNamespace().getUpdatedAt());
      if (checkGuid) {
        assertNotNull(actualFeature.getProperties().getXyzNamespace().getUuid()); // After version 0.2.0
        assertNotNull(actualFeature.getProperties().getXyzNamespace().getUuid());
      } else {
        assertNull(actualFeature.getProperties().getXyzNamespace().getPuuid());
        assertNull(actualFeature.getProperties().getXyzNamespace().getUuid());
      }
    }
  }

  protected void assertCount(String insertRequest, String countResponse) {
    if (!JsonPath.<Boolean>read(countResponse, "$.count.estimated")) {
      assertEquals("Check inserted feature count vs fetched count", JsonPath.read(insertRequest, "$.insertFeatures.length()").toString(),
          JsonPath.read(countResponse, "$.count.value").toString());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected void testModifyFeatures(boolean includeOldStates) throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    LOGGER.info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
    List<Map> originalFeatures = jsonPathFeatures.read(insertResponse, jsonPathConf);

    final JsonPath jsonPathFeatureIds = JsonPath.compile("$.features..id");
    List<String> ids = jsonPathFeatureIds.read(insertResponse, jsonPathConf);
    LOGGER.info("Preparation: Inserted features {}", ids);

    // =========== UPDATE ==========
    LOGGER.info("Modify features");
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
    assertNotNull(features, "'features' element in ModifyFeaturesResponse is missing");
    assertTrue(features.size() > 0, "'features' element in ModifyFeaturesResponse is empty");

    final JsonPath jsonPathOldFeatures = JsonPath.compile("$.oldFeatures");
    List oldFeatures = jsonPathOldFeatures.read(updateFeaturesResponse, jsonPathConf);
    if (includeOldStates) {
      assertNotNull(oldFeatures, "'oldFeatures' element in ModifyFeaturesResponse is missing");
      assertTrue(oldFeatures.size() > 0, "'oldFeatures' element in ModifyFeaturesResponse is empty");
      assertEquals(oldFeatures, originalFeatures);
    } else if (oldFeatures != null) {
      assertEquals(0, oldFeatures.size(), "unexpected oldFeatures in ModifyFeaturesResponse");
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
      assertNotNull(oldFeatures, "'oldFeatures' element in ModifyFeaturesResponse is missing");
      assertTrue(oldFeatures.size() > 0, "'oldFeatures' element in ModifyFeaturesResponse is empty");
      assertEquals(oldFeatures, features);
    } else if (oldFeatures != null) {
      assertEquals(0, oldFeatures.size(), "unexpected oldFeatures in ModifyFeaturesResponse");
    }

    LOGGER.info("Modify features tested successfully");
  }
}
