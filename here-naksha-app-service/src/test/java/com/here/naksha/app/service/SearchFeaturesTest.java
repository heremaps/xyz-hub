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

import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SearchFeaturesTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "search_features_test_space";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/Search/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/Search/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc1000_testSearchFeatures() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features getting returned for given Tile and given single tag value
    // Given: search query
    final String tagsQueryParam = "tags=one+four";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1000_search/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1001_testSearchNoResults() throws URISyntaxException, IOException, InterruptedException, JSONException {
    // Given: search query not matching saved features
    final String tagsQueryParam = "tags=seven";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/Search/TC1001_searchNoResults/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response =
        nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1002_testSearchWrongSpace() throws URISyntaxException, IOException, InterruptedException, JSONException {
    // Given: search query not matching saved features
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/Search/TC1002_searchWrongSpace/search_response.json");
    final String streamId = UUID.randomUUID().toString();
    final String wrongSpaceId = "wrong_space_id";

    // When: Get Features By Tile request is submitted to NakshaHub
    HttpResponse<String> response =
        nakshaClient.get("hub/spaces/" + wrongSpaceId + "/search" + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }
}
