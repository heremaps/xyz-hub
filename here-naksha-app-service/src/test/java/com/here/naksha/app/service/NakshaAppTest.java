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

import static com.here.naksha.app.common.TestUtil.*;
import static com.here.naksha.app.service.NakshaApp.newInstance;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.psql.PsqlStorage;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NakshaAppTest {

  static NakshaApp app = null;
  static NakshaHubConfig config = null;

  static final String NAKSHA_HTTP_URI = "http://localhost:8080/";
  static HttpClient httpClient;
  static HttpRequest stdHttpRequest;

  static final String TEST_DATA_FOLDER = "src/test/resources/unit_test_data/";
  static final String HDR_STREAM_ID = "Stream-Id";

  @BeforeAll
  static void prepare() throws InterruptedException, URISyntaxException {
    String dbUrl = System.getenv("TEST_NAKSHA_PSQL_URL");
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null) password = "password";
    if (dbUrl == null)
      dbUrl = "jdbc:postgresql://localhost/postgres?user=postgres&password=" + password
          + "&schema=naksha_test_maint_app";

    app = newInstance("mock-config", dbUrl);
    config = app.getHub().getConfig();
    app.start();
    Thread.sleep(5000); // wait for server to come up
    // create standard Http client and request which will be used across tests
    httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.of(10, SECONDS)).build();
    stdHttpRequest = HttpRequest.newBuilder()
        .uri(new URI(NAKSHA_HTTP_URI))
        .header("Content-Type", "application/json")
        .build();
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
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/storages"))
        .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
        .header(HDR_STREAM_ID, streamId)
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

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
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/storages"))
        .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
        .header(HDR_STREAM_ID, streamId)
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // 3. Perform assertions
    assertEquals(409, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting conflict error message", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0003_testGetStorages() throws Exception {
    // Test API : GET /hub/storages
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0003_getStorages/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/storages"))
        .GET()
        .header(HDR_STREAM_ID, streamId)
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created storage", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0004_testInvalidUrlPath() throws Exception {
    // Test API : GET /hub/invalid_storages
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/invalid_storages"))
        .GET()
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
  }

  @Test
  @Order(3)
  void tc0005_testGetStorageById() throws Exception {
    // Test API : GET /hub/storages/{storageId}
    // 1. Load test data
    final String expectedBodyPart = loadFileOrFail("TC0001_createStorage/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/storages/um-mod-dev"))
        .GET()
        .header(HDR_STREAM_ID, streamId)
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // 3. Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Expecting previously created storage", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(3)
  void tc0006_testGetStorageByWrongId() throws Exception {
    // Test API : GET /hub/storages/{storageId}
    // 1. Load test data
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpRequest request = HttpRequest.newBuilder(stdHttpRequest, (k, v) -> true)
        .uri(new URI(NAKSHA_HTTP_URI + "hub/storages/nothingness"))
        .GET()
        .header(HDR_STREAM_ID, streamId)
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // 3. Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @Test
  @Order(4)
  void tc0300_testCreateFeatures() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Given: Storage (mock implementation) configured in Admin storage
    final Storage storage = parseJsonFileOrFail("TC0300_createFeatures/create_storage.json", Storage.class);
    final WriteFeatures<?> stRequest =
        createFeatureRequest(NakshaAdminCollection.STORAGES, storage, IfExists.REPLACE, IfConflict.REPLACE);
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(stRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating Stroage");
      writer.commit();
    }

    // Given: EventHandler (uses above Storage) configured in Admin storage
    final EventHandler eventHandler =
        parseJsonFileOrFail("TC0300_createFeatures/create_event_handler.json", EventHandler.class);
    final WriteFeatures<?> ehRequest = createFeatureRequest(
        NakshaAdminCollection.EVENT_HANDLERS, eventHandler, IfExists.REPLACE, IfConflict.REPLACE);
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(ehRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating EventHandler");
      writer.commit();
    }

    // Given: Space (uses above EventHandler) configured in Admin storage
    final Space space = parseJsonFileOrFail("TC0300_createFeatures/create_space.json", Space.class);
    final WriteFeatures<?> spRequest =
        createFeatureRequest(NakshaAdminCollection.SPACES, space, IfExists.REPLACE, IfConflict.REPLACE);
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(spRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating Space");
      writer.commit();
    }

    // Given: Create Features request (against above Space)
    final XyzFeature feature = parseJsonFileOrFail("TC0300_createFeatures/create_features.json", XyzFeature.class);
    final WriteFeatures<?> fRequest = createFeatureRequest(space.getId(), feature, IfExists.FAIL, IfConflict.FAIL);

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(fRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating Features " + result);
      writer.commit();
    }

    // Then:
    // TODO : Verify

  }

  @AfterAll
  static void close() throws InterruptedException {
    // TODO: Find a way to gracefully shutdown the server.
    //       To do some manual testing with the running service, uncomment this:
    if (app != null) {
      // drop schema after test execution
      if (app.getHub().getAdminStorage() instanceof PsqlStorage psqlStorage) {
        psqlStorage.dropSchema();
      }
      // app.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(3600));
      app.stopInstance();
    }
  }
}
