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

import static com.here.naksha.app.service.NakshaHub.newHub;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

class NakshaHubTest {

  static NakshaHub hub;

  static final String NAKSHA_HTTP_URI = "http://localhost:8080/";
  static HttpClient httpClient;
  static HttpRequest stdHttpRequest;

  static final String TEST_DATA_FOLDER = "src/test/resources/unit_test_data/";
  static final String HDR_STREAM_ID = "Stream-Id";

  @BeforeAll
  static void prepare() throws Exception {
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null) password = "password";
    hub = newHub(
        "jdbc:postgresql://localhost/postgres?user=postgres&password=" + password + "&schema=naksha_test",
        "local"); // this string concat only happens once, its ok ;)
    hub.start();
    Thread.sleep(5000); // wait for server to come up
    // create standard Http client and request which will be used across tests
    httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.of(10, SECONDS)).build();
    stdHttpRequest = HttpRequest.newBuilder()
        .uri(new URI(NAKSHA_HTTP_URI))
        .header("Content-Type", "application/json")
        .build();
  }

  private String readTestFile(final String filePath) throws Exception {
    return new String(Files.readAllBytes(Paths.get(TEST_DATA_FOLDER + filePath)));
  }

  private String getHeader(final HttpResponse<?> response, final String header) {
    final List<String> values = response.headers().map().get(header);
    // if list has only one node, return just string element, otherwise toString() of entire list
    return (values == null) ? null : (values.size() > 1 ? values.toString() : values.get(0));
  }

  @Test
  void tc0001_testGetStorages() throws Exception {
    // Test API : GET /hub/storages
    // 1. Load test data
    final String expectedBodyPart = readTestFile("TC0001_getStorages/body_part.json");
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
        "Expecting default psql Storage", expectedBodyPart, response.body(), JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  @AfterAll
  static void close() throws InterruptedException {
    // TODO: Find a way to gracefully shutdown the server.
    //       To do some manual testing with the running service, uncomment this:
    // hub.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(3600));
  }
}
