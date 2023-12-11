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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import com.here.naksha.lib.core.models.naksha.Space;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class ReadFeaturesByTileTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public ReadFeaturesByTileTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
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

  /*
  For this test suite, we upfront create various Features using different combination of Tags and Geometry.
  To know what exact features we create, check the create_features.json test file for test tc0800_xx().
  And then in subsequent tests, we validate the various GetByTile APIs using different query parameters.
  */

  public void tc0800_testGetByTileWithSingleTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features getting returned for given Tile and given single tag value
    String streamId;
    HttpResponse<String> response;

    // Given: Storage (mock implementation) configured in Admin storage
    final String storageJson = loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/create_storage.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/storages", storageJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Storage");

    // Given: EventHandler (uses above Storage) configured in Admin storage
    final String handlerJson = loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/create_event_handler.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/handlers", handlerJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Space (uses above EventHandler) configured in Admin storage
    final String spaceJson = loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/create_space.json");
    final Space space = TestUtil.parseJson(spaceJson, Space.class);
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces", spaceJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Space");

    // Given: New Features persisted in above Space
    String bodyJson = loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating new Features");

    // Given: Features By Tile request (against above space)
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0800_SingleTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get(
        "hub/spaces/" + space.getId() + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0801_testGetByTileWithTagOrCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile and Tag OR condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=two,three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0801_TagOrCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0802_testGetByTileWithTagAndCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile and Tag AND condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0802_TagAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0803_testGetByTileWithTagOrOrConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag OR condition using comma separated value
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=three" + "&tags=four,five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0803_TagOrOrCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0804_testGetByTileWithTagOrAndConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and combination of Tag OR and AND conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one" + "&tags=two+three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0804_TagOrAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0805_testGetByTileWithTagAndOrAndConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and combination of Tag AND, OR, AND conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=three+four" + "&tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0805_TagAndOrAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0806_testGetByTileWithLimit() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and limit
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String limitQueryParam = "limit=2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0806_WithLimit/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get(
        "hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam + "&" + limitQueryParam,
        streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0807_testGetByTile() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "120203302030322200";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0807_TileOnly/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0808_testGetByTile2AndTagAndCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag AND condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "120203302030322200";
    final String tagsQueryParam = "tags=three+four";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0808_Tile2_TagAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0809_testGetByTileWithoutTile() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile coordinates are not provided
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0809_WithoutTile/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0810_testGetByTileWithInvalidTileId() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile Id is invalid
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "A";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0810_InvalidTileId/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0811_testGetByTileWithInvalidTagDelimiter() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error when Tile condition is valid but invalid Tag delimiter is used
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one@two";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0811_InvalidTagDelimiter/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0812_testGetByTileWithNonNormalizedTag() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag combination having NonNormalized Tag value
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=non-matching-tag+" + urlEncoded("@ThRee");
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0812_NonNormalizedTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0813_testGetByTileWithMixedTagConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate features returned match with given Tile condition and Tag combination having mixed AND/OR conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=six,three+four" + "&tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0813_MixedTagConditions/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0814_testGetByTileWithTagMismatch() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate NO features returned when features match given Tile, but NOT the given tags
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "1";
    final String tagsQueryParam = "tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0814_NonMatchingTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0815_testGetByTileWithTileMismatch() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate NO features returned when features match given Tags, but NOT the given Tile id
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String tileId = "0";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0815_NonMatchingTile/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/tile/quadkey/" + tileId + "?" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0816_testGetByTileWithUnsupportedTileType() throws Exception {
    // NOTE : This test depends on setup done as part of tc0800_testGetByTileWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/tile/{type}/{tileId}
    // Validate API error is returned when unsupported Tile Type is, even though tileId and Tags are valid
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Tile request (against configured space)
    final String spaceId = "local-space-4-features-by-tile";
    final String unsupportedTileType = "here-quadkey";
    final String tileId = "1";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByTile/TC0816_UnsupportedTileType/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By Tile request is submitted to NakshaHub
    response = nakshaClient.get(
        "hub/spaces/" + spaceId + "/tile/" + unsupportedTileType + "/" + tileId + "?" + tagsQueryParam,
        streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }
}
