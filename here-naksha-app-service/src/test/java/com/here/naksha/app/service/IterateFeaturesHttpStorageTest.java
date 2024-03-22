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

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import com.here.naksha.app.service.testutil.GzipUtil;
import netscape.javascript.JSException;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;

@WireMockTest(httpPort = 9095)
class IterateFeaturesHttpStorageTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "iterate_features_http_test_space";
  private static final String TEST_DIR_PATH = "ReadFeatures/IterateHttpStorage/";

  private static final UrlPathPattern HTTP_STORAGE_URL_PATH = urlPathEqualTo("/my_env/my_storage/my_feat_type/iterate");

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, TEST_DIR_PATH + "setup");
  }

  @Test
  void tc1100_testIterateAllInSinglePage() throws URISyntaxException, InterruptedException, JSONException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in single iteration without handle,
    // when number of features are less than limit specified
    // Given: iterate parameters
    final String limitQueryParam = "limit=5";
    final String expectedBodyPart = loadFileOrFail(TEST_DIR_PATH + "TC1100_allInSinglePage/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    stubFor(get(HTTP_STORAGE_URL_PATH)
            .withQueryParam("limit", equalTo("5"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
            .hasNoNextPageToken();

    verify(1, getRequestedFor(HTTP_STORAGE_URL_PATH));
  }

  @Test
  void tc1200_testEmptyCollectionResponse() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in single iteration without handle,
    // when number of features are less than limit specified
    // Given: iterate parameters
    final String expectedBodyPart = loadFileOrFail(TEST_DIR_PATH + "TC1200_emptyCollectionResponse/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    stubFor(get(HTTP_STORAGE_URL_PATH)
            .willReturn(okJson(expectedBodyPart)));

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
            .hasFeatureCount(0)
            .hasNoNextPageToken();

    verify(1, getRequestedFor(HTTP_STORAGE_URL_PATH));
  }

  @Test
  void tc1201_testLessFeaturesThanLimit() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in single iteration without handle,
    // when number of features are less than limit specified
    // Given: iterate parameters
    final String limitQueryParam = "limit=5";
    final String expectedBodyPart = loadFileOrFail(TEST_DIR_PATH + "TC1201_lessFeaturesThanLimit/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    // Given: response with 2 featuresq
    stubFor(get(HTTP_STORAGE_URL_PATH)
            .withQueryParam("limit", equalTo("5"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate" + "?" + limitQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
            .hasFeatureCount(2)
            .hasNoNextPageToken();

    verify(1, getRequestedFor(HTTP_STORAGE_URL_PATH));
  }

  @Test
  void tc1202_testGzipEncodedResponse() throws URISyntaxException, InterruptedException, IOException {
    // Test API : GET /hub/spaces/{spaceId}/iterate
    // Validate all features getting returned in single iteration without handle,
    // when number of features are less than limit specified
    // Given: iterate parameters
    final String expectedBodyPart = loadFileOrFail(TEST_DIR_PATH + "TC1202_gzipEncodedResponse/iterate_response.json");
    final String streamId = UUID.randomUUID().toString();

    GzipUtil.stubOkGzipEncoded(get(HTTP_STORAGE_URL_PATH), expectedBodyPart);

    // When: Iterate Features request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/iterate", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
            .hasNoNextPageToken();

    verify(1, getRequestedFor(HTTP_STORAGE_URL_PATH));
  }
}