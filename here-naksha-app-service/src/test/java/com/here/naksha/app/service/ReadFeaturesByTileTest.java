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
import com.here.naksha.app.common.ResponseAssertions;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

class ReadFeaturesByTileTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_tile_test_space";

  /*
  For this test suite, we upfront create various Features using different combination of Tags and Geometry.
  To know what exact features we create, check the create_features.json test file for test tc0800_xx().
  And then in subsequent tests, we validate the various GetByTile APIs using different query parameters.
  */
  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByTile/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByTile/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc0800_testGetByTileWithSingleTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features getting returned for given Tile and given single tag value
    // Given: Features By Tile request (against above space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0801_testGetByTileWithTagOrCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile and Tag OR condition
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=two,three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0801_TagOrCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0802_testGetByTileWithTagAndCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile and Tag AND condition
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0802_TagAndCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0803_testGetByTileWithTagOrOrConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag OR condition using comma separated value
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=three" + "&tags=four,five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0803_TagOrOrCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0804_testGetByTileWithTagOrAndConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and combination of Tag OR and AND conditions
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=one" + "&tags=two+three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0804_TagOrAndCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0805_testGetByTileWithTagAndOrAndConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and combination of Tag AND, OR, AND conditions
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=three+four" + "&tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0805_TagAndOrAndCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0806_testGetByTileWithLimit() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and limit
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String limitQueryParam = "limit=2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0806_WithLimit/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get(
            "hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam + "&"
                + limitQueryParam,
            streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0807_testGetByTile() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition
    // Given: Features By Tile request (against configured space)
    final String tileId = "120203302030322200";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0807_TileOnly/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0808_testGetByTile2AndTagAndCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag AND condition
    // Given: Features By Tile request (against configured space)
    final String tileId = "120203302030322200";
    final String tagsQueryParam = "tags=three+four";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0808_Tile2_TagAndCondition/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0809_testGetByTileWithoutTile() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile coordinates are not provided
    // Given: Features By Tile request (against configured space)
    final String tileId = "";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0809_WithoutTile/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0810_testGetByTileWithInvalidTileId() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile Id is invalid
    // Given: Features By Tile request (against configured space)
    final String tileId = "A";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0810_InvalidTileId/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0811_testGetByTileWithInvalidTagDelimiter() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile condition is valid but invalid Tag delimiter is used
    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one@two";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0811_InvalidTagDelimiter/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0812_testGetByTileWithNonNormalizedTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag combination having NonNormalized Tag value
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=non-matching-tag+" + urlEncoded("@ThRee");
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0812_NonNormalizedTag/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0813_testGetByTileWithMixedTagConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag combination having mixed AND/OR conditions
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=six,three+four" + "&tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0813_MixedTagConditions/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0814_testGetByTileWithTagMismatch() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate NO features returned when features match given Tile, but NOT the given tags
    // Given: Features By Tile request (against configured space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0814_NonMatchingTag/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0815_testGetByTileWithTileMismatch() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate NO features returned when features match given Tags, but NOT the given Tile id
    // Given: Features By Tile request (against configured space)
    final String tileId = "0";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0815_NonMatchingTile/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0816_testGetByTileWithUnsupportedTileType() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error is returned when unsupported Tile Type is, even though tileId and Tags are valid
    // Given: Features By Tile request (against configured space)
    final String unsupportedTileType = "here-quadkey";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0816_UnsupportedTileType/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
        .get(
            "hub/spaces/" + SPACE_ID + "/tile/" + unsupportedTileType + "/" + tileId + "?" + tagsQueryParam,
            streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }
}
