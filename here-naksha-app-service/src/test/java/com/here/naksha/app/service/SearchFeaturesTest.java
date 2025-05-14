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
import static com.here.naksha.app.common.TestUtil.urlEncoded;

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
  void tc1000_testSearchFeatures() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate features getting returned for given Tag filter parameters
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
  void tc1001_testSearchNoResults() throws URISyntaxException, IOException, InterruptedException {
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
  void tc1002_testSearchWrongSpace() throws URISyntaxException, IOException, InterruptedException {
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

  @Test
  void tc1003_testSearchWithTagsAndProp() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate features getting returned for given Tag and Property search conditions
    // with response displaying only selected properties

    // Given: search query
    final String tagsQueryParam = "tags=one+four";
    final String propQueryParam = "p.speedLimit='60'";
    final String streamId = UUID.randomUUID().toString();
    final String selectionParam = "selection=p.speedLimit,%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags"));
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1003_searchWithTagsAndProp/search_response.json")
            .replaceAll("\\{\\{streamId}}",streamId);

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam + "&" + propQueryParam + "&" + selectionParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match",true);
  }

  @Test
  void tc1004_testSearchWithTagsMatchAndPropDoesNot() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate No features returned when Tag filter matches but Property filters don't

    // Given: search query
    final String tagsQueryParam = "tags=one+four";
    final String propQueryParam = "p.speedLimit='70'";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1004_searchWithTagsMatchAndPropDoesNot/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1005_testSearchWithOnlyPropFilter() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate features returned matches with Property filter (even when Tags filter not supplied)

    // Given: search query
    final String propQueryParam = "p.speedLimit='70'";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1005_searchWithOnlyPropFilter/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + propQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1006_testSearchWithoutFilters() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate API returns error when neither Tags nor Property filters provided

    // Given: search query
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1006_searchWithoutFilters/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(400)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1007_testSearchInvalidPropSelection() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate features getting returned for given Tag and Property search conditions
    // with response displaying no properties as the selection property does not exist

    // Given: search query
    final String tagsQueryParam = "tags=one+four";
    final String propQueryParam = "p.speedLimit='60'";
    final String selectionParam = "selection=p.unknown_prop";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1007_testSearchInvalidPropSelection/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam + "&" + propQueryParam + "&" + selectionParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1008_testSearchInvalidDelimiterPropSelection() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Return error result because of wrong delimiter in selection params

    // Given: search query
    final String tagsQueryParam = "tags=one+four";
    final String propQueryParam = "p.speedLimit='60'";
    final String selectionParam = "selection=p.speedLimit+%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags"));
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1008_testSearchInvalidDelimiterPropSelection/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + tagsQueryParam + "&" + propQueryParam + "&" + selectionParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(400)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc1009_testSearchWithArrayBasedPropPathFilter() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/search
    // Validate features returned matches with Property filter (even when Tags filter not supplied)

    // Given: search query
    final String propQueryParam = "p.adminInfo.0.admin.id=%s".formatted(urlEncoded("urn:here::here:Admin:20128885"))
            + "&"
            + "p.adminInfo.0.country.id=%s".formatted(urlEncoded("urn:here::here:Admin:20128885"));
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Search/TC1009_searchWithArrayBasedPropPathFilter/search_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/search" + "?" + propQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }
}
