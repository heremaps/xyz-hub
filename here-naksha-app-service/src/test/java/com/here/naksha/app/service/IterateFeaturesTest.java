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
import static com.here.naksha.app.common.TestUtil.parseJson;
import static com.here.naksha.app.common.TestUtil.urlEncoded;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class IterateFeaturesTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "iterate_features_test_space";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/Iterate/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/Iterate/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc1100_testIterateAllInSinglePage() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in single iteration without handle,
    // when number of features are less than limit specified
    // Given: iterate parameters
    final String limitQueryParam = "limit=5";
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1100_allInSinglePage/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
        .hasNoNextPageToken();
  }

  @Test
  void tc1101_testIterateInTwoPages() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in two iterations, with:
    // - first iteration returning features and nextPageToken
    // - second iteration accepting handle and returning all other features without nextPageToken

    // Given: iterate parameters for first request
    final String limitQueryParam = "limit=3";
    final String firstExpectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1101_inTwoPages/iterate_response_1.json");
    final String firstStreamId = UUID.randomUUID().toString();

    // When: First iterate Features request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam, firstStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(firstStreamId)
        .hasJsonBody(firstExpectedBodyPart, "First Iterate response body doesn't match");

    // Given: iterate parameters for second request
    final String handleQueryParam = "handle=" + urlEncoded(parseJson(response.body(), XyzFeatureCollection.class).getNextPageToken());
    final String secondExpectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1101_inTwoPages/iterate_response_2.json");
    final String secondStreamId = UUID.randomUUID().toString();

    // When: Second iterate Features request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + handleQueryParam, secondStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(secondStreamId)
        .hasJsonBody(secondExpectedBodyPart, "Final Iterate response body doesn't match")
        .hasNoNextPageToken();
  }

  @Test
  void tc1102_testIterateInThreePages() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in three iterations, with:
    // - first iteration returning features and nextPageToken
    // - second iteration accepting handle and returning next set of features and nextPageToken
    // - third iteration accepting handle and returning all other features without nextPageToken

    // Given: iterate parameters for first request
    final String limitQueryParam = "limit=2";
    final String firstExpectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1102_inThreePages/iterate_response_1.json");
    final String firstStreamId = UUID.randomUUID().toString();
    // When: First iterate Features request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam, firstStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(firstStreamId)
        .hasJsonBody(firstExpectedBodyPart, "First Iterate response body doesn't match");

    // Given: iterate parameters for second request
    final String secondHandleQueryParam = "handle=" + urlEncoded(parseJson(response.body(), XyzFeatureCollection.class).getNextPageToken());
    final String secondExpectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1102_inThreePages/iterate_response_2.json");
    final String secondStreamId = UUID.randomUUID().toString();
    // When: Second iterate Features request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + secondHandleQueryParam, secondStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(secondStreamId)
        .hasJsonBody(secondExpectedBodyPart, "Second Iterate response body doesn't match");

    // Given: iterate parameters for third request
    final String thirdHandleQueryParam = "handle=" + urlEncoded(parseJson(response.body(), XyzFeatureCollection.class).getNextPageToken());
    final String thirdExpectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1102_inThreePages/iterate_response_3.json");
    final String thirdStreamId = UUID.randomUUID().toString();
    // When: Third iterate Features request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + thirdHandleQueryParam, thirdStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(thirdStreamId)
        .hasJsonBody(thirdExpectedBodyPart, "Final Iterate response body doesn't match")
        .hasNoNextPageToken();
  }


  @Test
  void tc1103_testIterateWithInvalidHandle() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate API returns error (gracefully) when unexpected handle value is supplied

    // Given: iterate parameters
    final String handleQueryParam = "handle=" + urlEncoded("{\"limit\":0,\"offset\":5}");
    final String expectedBodyPart = loadFileOrFail("ReadFeatures/Iterate/TC1103_invalidHandle/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + handleQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Iterate Feature Error response body doesn't match");
  }

  @Test
  void tc1104_testIterateInTwoPagesWithPropSelection() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in two iterations, with:
    // - first iteration returning features and nextPageToken, selecting: 1 normal prop, 1 URI encoded prop, 1 prop not existing
    // - second iteration accepting handle but use the wrong selection delimiter (should return 400)

    // Given: iterate parameters for first request
    final String limitQueryParam = "limit=3";
    final String firstStreamId = UUID.randomUUID().toString();
    final String firstExpectedBodyPart = loadFileOrFail(
        "ReadFeatures/Iterate/TC1104_testIterateInTwoPagesWithPropSelection/iterate_response_1.json")
        .replaceAll("\\{\\{streamId}}", firstStreamId);
    final String selectionParams = "selection=p.speedLimit,p.unknown_prop,%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags"));

    // When: First iterate Features request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam + "&" + selectionParams,
        firstStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(firstStreamId)
        .hasJsonBody(firstExpectedBodyPart, "First Iterate response body doesn't match", true);

    // Given: iterate parameters for second request
    final String handleQueryParam = "handle=" + urlEncoded(parseJson(response.body(), XyzFeatureCollection.class).getNextPageToken());
    final String secondExpectedBodyPart = loadFileOrFail(
        "ReadFeatures/Iterate/TC1104_testIterateInTwoPagesWithPropSelection/iterate_response_2.json");
    final String secondStreamId = UUID.randomUUID().toString();
    final String selectionWrongDelimiterParams = "selection=p.speedLimit+p.unknown_prop";

    // When: Second iterate Features request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + handleQueryParam + "&" + selectionWrongDelimiterParams,
        secondStreamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(secondStreamId)
        .hasJsonBody(secondExpectedBodyPart, "Final Iterate response body doesn't match");
  }
}
