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
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.lib.core.models.storage.*;
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

  static CreateFeatureTestHelper createFeatureTests;

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

    // create test helpers
    createFeatureTests = new CreateFeatureTestHelper(app, NAKSHA_HTTP_URI, httpClient, stdHttpRequest);
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
  void tc0300_testCreateFeaturesWithNewIds() throws Exception {
    createFeatureTests.tc0300_testCreateFeaturesWithNewIds();
  }

  @Test
  @Order(5)
  void tc0301_testCreateFeaturesWithGivenIds() throws Exception {
    createFeatureTests.tc0301_testCreateFeaturesWithGivenIds();
  }

  @Test
  @Order(6)
  void tc0302_testCreateFeaturesWithPrefixId() throws Exception {
    createFeatureTests.tc0302_testCreateFeaturesWithPrefixId();
  }

  @Test
  @Order(7)
  void tc0303_testCreateFeaturesWithAddTags() throws Exception {
    createFeatureTests.tc0303_testCreateFeaturesWithAddTags();
  }

  @Test
  @Order(8)
  void tc0304_testCreateFeaturesWithRemoveTags() throws Exception {
    createFeatureTests.tc0304_testCreateFeaturesWithRemoveTags();
  }

  @AfterAll
  static void close() throws InterruptedException {
    if (app != null) {
      // drop schema after test execution
      if (app.getHub().getAdminStorage() instanceof PsqlStorage psqlStorage) {
        psqlStorage.dropSchema();
      }
      // To do some manual testing with the running service, uncomment this:
      // app.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(3600));
      app.stopInstance();
    }
  }
}
