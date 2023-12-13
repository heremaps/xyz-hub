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
package com.here.naksha.app.service;

import static com.here.naksha.app.common.TestNakshaAppInitializer.localPsqlBasedNakshaApp;
import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.TestUtil.getHeader;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestNakshaAppInitializer;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.psql.PsqlStorage;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NakshaAppTest {

  static TestNakshaAppInitializer nakshaAppInitializer;
  static NakshaHubConfig config;

  static NakshaTestWebClient nakshaClient;

  static CreateFeatureTestHelper createFeatureTests;
  static ReadFeaturesByIdsTestHelper readFeaturesByIdsTests;
  static UpdateFeatureTestHelper updateFeatureTestHelper;
  static ReadFeaturesByBBoxTestHelper readFeaturesByBBoxTestHelper;
  static ReadFeaturesByTileTestHelper readFeaturesByTileTestHelper;
  static SearchFeaturesTestHelper searchFeaturesTestHelper;

  @BeforeAll
  static void prepare() throws InterruptedException, ExecutionException {
    nakshaAppInitializer = localPsqlBasedNakshaApp(); // to use mock, call NakshaAppInitializer.mockedNakshaApp()
    cleanUpDb(nakshaAppInitializer.testDbUrl);
    NakshaApp app = nakshaAppInitializer.initNaksha();
    config = app.getHub().getConfig();
    app.start();
    Thread.sleep(5000); // wait for server to come up
    // create standard Http client and request which will be used across tests
    nakshaClient = new NakshaTestWebClient();

    // create test helpers
    createFeatureTests = new CreateFeatureTestHelper(app, nakshaClient);
    readFeaturesByIdsTests = new ReadFeaturesByIdsTestHelper(app, nakshaClient);
    updateFeatureTestHelper = new UpdateFeatureTestHelper(app, nakshaClient);
    readFeaturesByBBoxTestHelper = new ReadFeaturesByBBoxTestHelper(app, nakshaClient);
    readFeaturesByTileTestHelper = new ReadFeaturesByTileTestHelper(app, nakshaClient);
    searchFeaturesTestHelper = new SearchFeaturesTestHelper(app, nakshaClient);
  }

  @Test
  @Order(1)
  void tc0001_testCreateStorages() throws Exception {
    // Test API : POST /hub/storages
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0001_createStorage/create_storage.json");
    final String expectedBodyPart = loadFileOrFail("TC0001_createStorage/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/storages", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting new storage in response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(2)
  void tc0002_testCreateDuplicateStorage() throws Exception {
    // Test API : POST /hub/storages
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0002_createDupStorage/create_storage.json");
    final String expectedBodyPart = loadFileOrFail("TC0002_createDupStorage/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/storages", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(409, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting conflict error message", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0003_testCreateStorageMissingClassName() throws Exception {
    // Test API : POST /hub/storages
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0003_createStorageMissingClassName/create_storage.json");
    final String expectedBodyPart = loadFileOrFail("TC0003_createStorageMissingClassName/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/storages", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(400, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting failure response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(2)
  void tc0004_testInvalidUrlPath() throws Exception {
    // Test API : GET /hub/invalid_storages
    final HttpResponse<String> response =
        nakshaClient.get("hub/invalid_storages", UUID.randomUUID().toString());

    // Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
  }

  @Test
  @Order(2)
  void tc0020_testGetStorageById() throws Exception {
    // Test API : GET /hub/storages/{storageId}
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0001_createStorage/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/storages/um-mod-dev", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created storage", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(2)
  void tc0021_testGetStorageByWrongId() throws Exception {
    // Test API : GET /hub/storages/{storageId}
    // 1. Load test data
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/storages/nothingness", streamId);

    // 3. Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(2)
  void tc0040_testGetStorages() throws Exception {
    // Test API : GET /hub/storages
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0040_getStorages/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/storages", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created storage", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(2)
  void tc0060_testUpdateStorage() throws Exception {
    // Test API : PUT /hub/storages/{storageId}
    // Given:
    final String updateStorageJson = loadFileOrFail("TC0060_updateStorage/update_storage.json");
    final String expectedRespBody = loadFileOrFail("TC0060_updateStorage/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response = nakshaClient.put("hub/storages/um-mod-dev", updateStorageJson, streamId);

    // Then:
    assertEquals(200, response.statusCode());
    JSONAssert.assertEquals(expectedRespBody, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(2)
  void tc0061_testUpdateNonexistentStorage() throws Exception {
    // Test API : PUT /hub/storages/{storageId}
    // Given:
    final String updateStorageJson = loadFileOrFail("TC0061_updateNonexistentStorage/update_storage.json");
    final String expectedErrorResponse = loadFileOrFail("TC0061_updateNonexistentStorage/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/storages/this-id-does-not-exist", updateStorageJson, streamId);

    // Then:
    assertEquals(404, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(3)
  void tc0062_testUpdateStorageWithoutClassName() throws Exception {
    // Test API : PUT /hub/storages/{storageId}
    // Given:
    final String updateStorageJson = loadFileOrFail("TC0062_updateStorageWithoutClassName/update_storage.json");
    final String expectedErrorResponse = loadFileOrFail("TC0062_updateStorageWithoutClassName/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response = nakshaClient.put("hub/storages/um-mod-dev", updateStorageJson, streamId);

    // Then:
    assertEquals(400, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(3)
  void tc0063_testUpdateStorageWithWithMismatchingId() throws Exception {
    // Test API : PUT /hub/storages/{storageId}
    // Given:
    final String bodyWithDifferentStorageId =
        loadFileOrFail("TC0063_updateStorageWithMismatchingId/update_storage.json");
    final String expectedErrorResponse = loadFileOrFail("TC0063_updateStorageWithMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/storages/not-really-um-mod-dev", bodyWithDifferentStorageId, streamId);

    // Then:
    assertEquals(400, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(2)
  void tc0100_testCreateEventHandler() throws Exception {
    // Test API : POST /hub/handlers
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0100_createEventHandler/create_event_handler.json");
    final String expectedCreationResponse = loadFileOrFail("TC0100_createEventHandler/response_create_1.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call creating handler
    final HttpResponse<String> response = nakshaClient.post("hub/handlers", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting new handler in response",
        expectedCreationResponse,
        response.body(),
        JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0101_testDuplicateEventHandler() throws Exception {
    // Test API : POST /hub/handlers
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0100_createEventHandler/create_event_handler.json");
    final String expectedDuplicateResponse = loadFileOrFail("TC0101_duplicateEventHandler/response_conflict.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call creating handler
    final HttpResponse<String> response = nakshaClient.post("hub/handlers", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(409, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting duplicated handler in response",
        expectedDuplicateResponse,
        response.body(),
        JSONCompareMode.LENIENT);
  }

  @Test
  @Order(3)
  void tc0102_testCreateHandlerMissingClassName() throws Exception {
    // Test API : POST /hub/handlers
    // 1. Load test data
    final String bodyJson = loadFileOrFail("TC0102_createHandlerNoClassName/create_event_handler.json");
    final String expectedBodyPart = loadFileOrFail("TC0102_createHandlerNoClassName/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/handlers", bodyJson, streamId);

    // 3. Perform assertions
    assertEquals(400, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting failure response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0120_testGetHandlerById() throws Exception {
    // Test API : GET /hub/handlers/{handlerId}
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0100_createEventHandler/response_create_1.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/handlers/test-handler", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting handler response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0121_testGetHandlerByWrongId() throws Exception {
    // Test API : GET /hub/handlers/{handlerId}
    // 1. Load test data
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/handlers/not-real-handler", streamId);

    // 3. Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0140_testGetHandlers() throws Exception {
    // Test API : GET /hub/handlers
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0140_getHandlers/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/handlers", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created handler", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0160_testUpdateEventHandler() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given:
    final String updateEventHandlerJson = loadFileOrFail("TC0160_updateEventHandler/update_event_handler.json");
    final String expectedRespBody = loadFileOrFail("TC0160_updateEventHandler/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/handlers/test-handler", updateEventHandlerJson, streamId);

    // Then:
    assertEquals(200, response.statusCode());
    JSONAssert.assertEquals(expectedRespBody, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(3)
  void tc0161_testUpdateNonexistentEventHandler() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given:
    final String updateEventHandlerJson =
        loadFileOrFail("TC0161_updateNonexistentEventHandler/update_event_handler.json");
    final String expectedRespBody = loadFileOrFail("TC0161_updateNonexistentEventHandler/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/handlers/non-existent-test-handler", updateEventHandlerJson, streamId);

    // Then:
    assertEquals(404, response.statusCode());
    JSONAssert.assertEquals(expectedRespBody, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(3)
  void tc0162_testUpdateEventHandlerWithMismatchingId() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given:
    final String updateOtherHandlerJson =
        loadFileOrFail("TC0162_updateEventHandlerWithMismatchingId/update_event_handler.json");
    final String expectedErrorResponse = loadFileOrFail("TC0162_updateEventHandlerWithMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/handlers/test-handler", updateOtherHandlerJson, streamId);

    // Then:
    assertEquals(400, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(3)
  void tc0200_testCreateSpace() throws Exception {
    // Test API : POST /hub/spaces
    // 1. Load test data
    final String spaceJson = loadFileOrFail("TC0200_createSpace/create_space.json");
    final String expectedBodyPart = loadFileOrFail("TC0200_createSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/spaces", spaceJson, streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting new space in response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(4)
  void tc0201_testCreateDuplicateSpace() throws Exception {
    // Test API : POST /hub/spaces
    // 1. Load test data
    final String duplicatedSpace = loadFileOrFail("TC0200_createSpace/create_space.json");
    final String expectedBodyPart = loadFileOrFail("TC0201_createDupSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.post("hub/spaces", duplicatedSpace, streamId);

    // 3. Perform assertions
    assertEquals(409, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting conflict error message", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(4)
  void tc0220_testGetSpaceById() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0200_createSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/test-space", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals("Expecting space response", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(4)
  void tc0221_testGetSpaceByWrongId() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}
    // 1. Load test data
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/not-real-space", streamId);

    // 3. Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(4)
  void tc0240_testGetSpaces() throws Exception {
    // Test API : GET /hub/spaces
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0240_getSpaces/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = nakshaClient.get("hub/spaces", streamId);

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created space", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(5)
  void tc0260_testUpdateSpace() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given:
    final String updateStorageJson = loadFileOrFail("TC0260_updateSpace/update_space.json");
    final String expectedRespBody = loadFileOrFail("TC0260_updateSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response = nakshaClient.put("hub/spaces/test-space", updateStorageJson, streamId);

    // Then:
    assertEquals(200, response.statusCode());
    JSONAssert.assertEquals(expectedRespBody, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(5)
  void tc0261_testUpdateNonexistentSpace() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given:
    final String updateSpaceJson = loadFileOrFail("TC0261_updateNonexistentSpace/update_space.json");
    final String expectedErrorResponse = loadFileOrFail("TC0261_updateNonexistentSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/non-existent-space", updateSpaceJson, streamId);

    // Then:
    assertEquals(404, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(5)
  void tc0263_testUpdateSpaceWithWithMismatchingId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given:
    final String bodyWithDifferentSpaceId = loadFileOrFail("TC0263_updateSpaceWithMismatchingId/update_space.json");
    final String expectedErrorResponse = loadFileOrFail("TC0263_updateSpaceWithMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/test-space", bodyWithDifferentSpaceId, streamId);

    // Then:
    assertEquals(400, response.statusCode());
    JSONAssert.assertEquals(expectedErrorResponse, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID));
  }

  @Test
  @Order(6)
  void tc0300_testCreateFeaturesWithNewIds() throws Exception {
    createFeatureTests.tc0300_testCreateFeaturesWithNewIds();
  }

  @Test
  @Order(7)
  void tc0301_testCreateFeaturesWithGivenIds() throws Exception {
    createFeatureTests.tc0301_testCreateFeaturesWithGivenIds();
  }

  @Test
  @Order(7)
  void tc0302_testCreateFeaturesWithPrefixId() throws Exception {
    createFeatureTests.tc0302_testCreateFeaturesWithPrefixId();
  }

  @Test
  @Order(7)
  void tc0303_testCreateFeaturesWithAddTags() throws Exception {
    createFeatureTests.tc0303_testCreateFeaturesWithAddTags();
  }

  @Test
  @Order(7)
  void tc0304_testCreateFeaturesWithRemoveTags() throws Exception {
    createFeatureTests.tc0304_testCreateFeaturesWithRemoveTags();
  }

  @Test
  @Order(7)
  void tc0305_testCreateFeaturesWithDupIds() throws Exception {
    createFeatureTests.tc0305_testCreateFeaturesWithDupIds();
  }

  @Test
  @Order(7)
  void tc0307_testCreateFeaturesWithNoHandler() throws Exception {
    createFeatureTests.tc0307_testCreateFeaturesWithNoHandler();
  }

  @Test
  @Order(7)
  void tc0308_testCreateFeaturesWithNoSpace() throws Exception {
    createFeatureTests.tc0308_testCreateFeaturesWithNoSpace();
  }

  @Test
  @Order(8)
  void tc0400_testReadFeaturesWithIds() throws Exception {
    readFeaturesByIdsTests.tc0400_testReadFeaturesByIds();
  }

  @Test
  @Order(9)
  void tc0401_testReadFeaturesForMissingIds() throws Exception {
    readFeaturesByIdsTests.tc0401_testReadFeaturesForMissingIds();
  }

  @Test
  @Order(9)
  void tc0402_testReadFeaturesWithoutIds() throws Exception {
    readFeaturesByIdsTests.tc0402_testReadFeaturesWithoutIds();
  }

  @Test
  @Order(9)
  void tc0403_testReadFeaturesByIdsFromMissingSpace() throws Exception {
    readFeaturesByIdsTests.tc0403_testReadFeaturesByIdsFromMissingSpace();
  }

  @Test
  @Order(9)
  void tc0404_testReadFeatureById() throws Exception {
    readFeaturesByIdsTests.tc0404_testReadFeatureById();
  }

  @Test
  @Order(9)
  void tc0405_testReadFeatureForMissingId() throws Exception {
    readFeaturesByIdsTests.tc0405_testReadFeatureForMissingId();
  }

  @Test
  @Order(9)
  void tc0406_testReadFeatureByIdFromMissingSpace() throws Exception {
    readFeaturesByIdsTests.tc0406_testReadFeatureByIdFromMissingSpace();
  }

  @Test
  @Order(9)
  void tc0407_testReadFeaturesWithCommaSeparatedIds() throws Exception {
    readFeaturesByIdsTests.tc0407_testReadFeaturesWithCommaSeparatedIds();
  }

  @Test
  @Order(10)
  void tc0500_testUpdateFeatures() throws Exception {
    updateFeatureTestHelper.tc0500_testUpdateFeatures();
  }

  @Test
  @Order(11)
  void tc0501_testUpdateFeatureById() throws Exception {
    updateFeatureTestHelper.tc0501_testUpdateFeatureById();
  }

  @Test
  @Order(11)
  void tc0502_testUpdateFeatureByWrongUriId() throws Exception {
    updateFeatureTestHelper.tc0502_testUpdateFeatureByWrongUriId();
  }

  @Test
  @Order(11)
  void tc0503_testUpdateFeatureWithMismatchingId() throws Exception {
    updateFeatureTestHelper.tc0503_testUpdateFeatureWithMismatchingId();
  }

  @Test
  @Order(11)
  void tc0504_testUpdateFeaturesNoId() throws Exception {
    updateFeatureTestHelper.tc0504_testUpdateFeaturesNoId();
  }

  @Test
  @Order(12)
  void tc0505_testUpdateFeaturesWithUuid() throws Exception {
    updateFeatureTestHelper.tc0505_testUpdateFeaturesWithUuid();
  }

  @Test
  @Order(12)
  void tc0506_testUpdateFeatureWithUuid() throws Exception {
    updateFeatureTestHelper.tc0506_testUpdateFeatureWithUuid();
  }

  @Test
  @Order(13)
  void tc0700_testGetByBBoxWithSingleTag() throws Exception {
    readFeaturesByBBoxTestHelper.tc0700_testGetByBBoxWithSingleTag();
  }

  @Test
  @Order(14)
  void tc0701_testGetByBBoxWithTagOrCondition() throws Exception {
    readFeaturesByBBoxTestHelper.tc0701_testGetByBBoxWithTagOrCondition();
  }

  @Test
  @Order(14)
  void tc0702_testGetByBBoxWithTagAndCondition() throws Exception {
    readFeaturesByBBoxTestHelper.tc0702_testGetByBBoxWithTagAndCondition();
  }

  @Test
  @Order(14)
  void tc0703_testGetByBBoxWithTagOrOrConditions() throws Exception {
    readFeaturesByBBoxTestHelper.tc0703_testGetByBBoxWithTagOrOrConditions();
  }

  @Test
  @Order(14)
  void tc0704_testGetByBBoxWithTagOrAndConditions() throws Exception {
    readFeaturesByBBoxTestHelper.tc0704_testGetByBBoxWithTagOrAndConditions();
  }

  @Test
  @Order(14)
  void tc0705_testGetByBBoxWithTagAndOrAndConditions() throws Exception {
    readFeaturesByBBoxTestHelper.tc0705_testGetByBBoxWithTagAndOrAndConditions();
  }

  @Test
  @Order(14)
  void tc0706_testGetByBBoxWithLimit() throws Exception {
    readFeaturesByBBoxTestHelper.tc0706_testGetByBBoxWithLimit();
  }

  @Test
  @Order(14)
  void tc0707_testGetByBBox() throws Exception {
    readFeaturesByBBoxTestHelper.tc0707_testGetByBBox();
  }

  @Test
  @Order(14)
  void tc0708_testGetByBBox2AndTagAndCondition() throws Exception {
    readFeaturesByBBoxTestHelper.tc0708_testGetByBBox2AndTagAndCondition();
  }

  @Test
  @Order(14)
  void tc0709_testGetByBBoxWithoutBBox() throws Exception {
    readFeaturesByBBoxTestHelper.tc0709_testGetByBBoxWithoutBBox();
  }

  @Test
  @Order(14)
  void tc0710_testGetByBBoxWithInvalidCoordinate() throws Exception {
    readFeaturesByBBoxTestHelper.tc0710_testGetByBBoxWithInvalidCoordinate();
  }

  @Test
  @Order(14)
  void tc0711_testGetByBBoxWithInvalidTagDelimiter() throws Exception {
    readFeaturesByBBoxTestHelper.tc0711_testGetByBBoxWithInvalidTagDelimiter();
  }

  @Test
  @Order(14)
  void tc0712_testGetByBBoxWithNonNormalizedTag() throws Exception {
    readFeaturesByBBoxTestHelper.tc0712_testGetByBBoxWithNonNormalizedTag();
  }

  @Test
  @Order(14)
  void tc0713_testGetByBBoxWithMixedTagConditions() throws Exception {
    readFeaturesByBBoxTestHelper.tc0713_testGetByBBoxWithMixedTagConditions();
  }

  @Test
  @Order(14)
  void tc0714_testGetByBBoxWithTagMismatch() throws Exception {
    readFeaturesByBBoxTestHelper.tc0714_testGetByBBoxWithTagMismatch();
  }

  @Test
  @Order(14)
  void tc0715_testGetByBBoxWithBBoxMismatch() throws Exception {
    readFeaturesByBBoxTestHelper.tc0715_testGetByBBoxWithBBoxMismatch();
  }

  @Test
  @Order(15)
  void tc0800_testGetByTileWithSingleTag() throws Exception {
    readFeaturesByTileTestHelper.tc0800_testGetByTileWithSingleTag();
  }

  @Test
  @Order(16)
  void tc0801_testGetByTileWithTagOrCondition() throws Exception {
    readFeaturesByTileTestHelper.tc0801_testGetByTileWithTagOrCondition();
  }

  @Test
  @Order(16)
  void tc0802_testGetByTileWithTagAndCondition() throws Exception {
    readFeaturesByTileTestHelper.tc0802_testGetByTileWithTagAndCondition();
  }

  @Test
  @Order(16)
  void tc0803_testGetByTileWithTagOrOrConditions() throws Exception {
    readFeaturesByTileTestHelper.tc0803_testGetByTileWithTagOrOrConditions();
  }

  @Test
  @Order(16)
  void tc0804_testGetByTileWithTagOrAndConditions() throws Exception {
    readFeaturesByTileTestHelper.tc0804_testGetByTileWithTagOrAndConditions();
  }

  @Test
  @Order(16)
  void tc0805_testGetByTileWithTagAndOrAndConditions() throws Exception {
    readFeaturesByTileTestHelper.tc0805_testGetByTileWithTagAndOrAndConditions();
  }

  @Test
  @Order(16)
  void tc0806_testGetByTileWithLimit() throws Exception {
    readFeaturesByTileTestHelper.tc0806_testGetByTileWithLimit();
  }

  @Test
  @Order(16)
  void tc0807_testGetByTile() throws Exception {
    readFeaturesByTileTestHelper.tc0807_testGetByTile();
  }

  @Test
  @Order(16)
  void tc0808_testGetByTile2AndTagAndCondition() throws Exception {
    readFeaturesByTileTestHelper.tc0808_testGetByTile2AndTagAndCondition();
  }

  @Test
  @Order(16)
  void tc0809_testGetByTileWithoutBBox() throws Exception {
    readFeaturesByTileTestHelper.tc0809_testGetByTileWithoutTile();
  }

  @Test
  @Order(16)
  void tc0810_testGetByTileWithInvalidTileId() throws Exception {
    readFeaturesByTileTestHelper.tc0810_testGetByTileWithInvalidTileId();
  }

  @Test
  @Order(16)
  void tc0811_testGetByTileWithInvalidTagDelimiter() throws Exception {
    readFeaturesByTileTestHelper.tc0811_testGetByTileWithInvalidTagDelimiter();
  }

  @Test
  @Order(16)
  void tc0812_testGetByTileWithNonNormalizedTag() throws Exception {
    readFeaturesByTileTestHelper.tc0812_testGetByTileWithNonNormalizedTag();
  }

  @Test
  @Order(16)
  void tc0813_testGetByTileWithMixedTagConditions() throws Exception {
    readFeaturesByTileTestHelper.tc0813_testGetByTileWithMixedTagConditions();
  }

  @Test
  @Order(16)
  void tc0814_testGetByTileWithTagMismatch() throws Exception {
    readFeaturesByTileTestHelper.tc0814_testGetByTileWithTagMismatch();
  }

  @Test
  @Order(16)
  void tc0815_testGetByTileWithTileMismatch() throws Exception {
    readFeaturesByTileTestHelper.tc0815_testGetByTileWithTileMismatch();
  }

  @Test
  @Order(16)
  void tc0816_testGetByTileWithUnsupportedTileType() throws Exception {
    readFeaturesByTileTestHelper.tc0816_testGetByTileWithUnsupportedTileType();
  }

  @Test
  @Order(16)
  void tc0900_testSearchFeatures() throws URISyntaxException, InterruptedException, JSONException, IOException {
    searchFeaturesTestHelper.tc0900_testSearchFeatures();
  }

  @Test
  @Order(17)
  void tc0901_testSearchNoResults() throws URISyntaxException, IOException, InterruptedException, JSONException {
    searchFeaturesTestHelper.tc0901_testSearchNoResults();
  }

  @Test
  @Order(17)
  void tc0902_testSearchWrongSpace() throws URISyntaxException, IOException, InterruptedException, JSONException {
    searchFeaturesTestHelper.tc0902_testSearchWrongSpace();
  }

  @AfterAll
  static void close() {
    if (nakshaAppInitializer != null) {
      NakshaApp app = nakshaAppInitializer.getNaksha();
      if (app != null) {
        app.stopInstance();
      }
    }
  }

  private static void cleanUpDb(String testUrl) {
    if (testUrl != null && !testUrl.isBlank()) {
      try (PsqlStorage psqlStorage = new PsqlStorage(testUrl)) {
        psqlStorage.dropSchema();
      }
    }
  }
}
