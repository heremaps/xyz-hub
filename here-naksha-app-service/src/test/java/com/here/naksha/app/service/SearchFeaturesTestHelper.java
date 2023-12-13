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

import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.TestUtil.getHeader;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import com.here.naksha.lib.core.models.naksha.Space;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

class SearchFeaturesTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public SearchFeaturesTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
    this.app = app;
    this.nakshaClient = nakshaClient;
  }

  private void standardAssertions(
      final @NotNull HttpResponse<String> actualResponse,
      final int expectedStatusCode,
      final @NotNull String expectedBodyPart,
      final @NotNull String expectedStreamId)
      throws JSONException {
    assertEquals(expectedStatusCode, actualResponse.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Get Feature response body doesn't match",
        expectedBodyPart,
        actualResponse.body(),
        JSONCompareMode.LENIENT);
    assertEquals(expectedStreamId, getHeader(actualResponse, HDR_STREAM_ID), "StreamId mismatch");
  }

  void tc0900_testSearchFeatures() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features getting returned for given Tile and given single tag value
    String streamId;
    HttpResponse<String> response;

    // Given: Storage (mock implementation) configured in Admin storage
    final String storageJson = loadFileOrFail("ReadFeatures/Search/TC0900_search/create_storage.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/storages", storageJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Storage");

    // And: EventHandler (uses above Storage) configured in Admin storage
    final String handlerJson = loadFileOrFail("ReadFeatures/Search/TC0900_search/create_event_handler.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/handlers", handlerJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // And: Space (uses above EventHandler) configured in Admin storage
    final String spaceJson = loadFileOrFail("ReadFeatures/Search/TC0900_search/create_space.json");
    final Space space = TestUtil.parseJson(spaceJson, Space.class);
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces", spaceJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Space");

    // And: New Features persisted in above Space
    String bodyJson = loadFileOrFail("ReadFeatures/Search/TC0900_search/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating new Features");

    // And: search query
    final String tagsQueryParam = "tags=one+four";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC0900_search/search_response.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + space.getId() + "/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  void tc0901_testSearchNoResults() throws URISyntaxException, IOException, InterruptedException, JSONException {
    // Given: search query not matching saved features
    final String tagsQueryParam = "tags=seven";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/Search/TC0901_searchNoResults/search_response.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    HttpResponse<String> response =
        nakshaClient.get("hub/spaces/tc_900_space/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  void tc0902_testSearchWrongSpace() throws URISyntaxException, IOException, InterruptedException, JSONException {
    // Given: search query not matching saved features
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/Search/TC0902_searchWrongSpace/search_response.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    HttpResponse<String> response =
        nakshaClient.get("hub/spaces/NOT_tc_900_space/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }
}
