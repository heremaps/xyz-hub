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
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.createHandler;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

@WireMockTest(httpPort = 9090)
class ReadFeaturesByBBoxHttpStorageTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String HTTP_SPACE_ID = "read_features_by_bbox_space_4_http_storage";
  private static final String PSQL_SPACE_ID = "read_features_by_bbox_space_4_psql_storage";
  private static final String VIEW_SPACE_ID = "read_features_by_bbox_space_4_view_storage";
  private static final String ENDPOINT = "/my_env/my_storage/my_feat_type/bbox";

  /*
  For this test suite, we upfront create various Features using different combination of Tags and Geometry.
  To know what exact features we create, check the create_features.json test file for test tc0700_xx().
  And then in subsequent tests, we validate the various GetByBBox APIs using different query parameters.
  */
  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    // Set up Http Storage based Space
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByBBoxHttpStorage/setup/http_storage_space");
    // Set up (standard) Psql Storage based Space
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByBBoxHttpStorage/setup/psql_storage_space");
    // Set up View Space over Psql and Http Storage based spaces
    createHandler(nakshaClient, "ReadFeatures/ByBBoxHttpStorage/setup/view_space/create_sourceId_handler.json");
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByBBoxHttpStorage/setup/view_space");
    // Load some test data in PsqlStorage based Space
    final String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/setup/psql_storage_space/create_features.json");
    final HttpResponse<String> response = nakshaClient.post("hub/spaces/" + PSQL_SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
    assertThat(response).hasStatus(200);
  }

  @Test
  void tc0700_testGetByBBoxWithSingleTag_willIgnoreTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features getting returned for given BBox coordinate and given single tag value
    // Given: Features By BBox request (against above space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0700_SingleTag/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("west", equalTo("-180.0"))
            .withQueryParam("south", equalTo("-90.0"))
            .withQueryParam("east", equalTo("180.0"))
            .withQueryParam("north", equalTo("90.0"))
            .willReturn(okJson(expectedBodyPart)));
    // Now the tags are not supported and will be ignored.

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + HTTP_SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0706_testGetByBBoxWithLimit() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and limit

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one";
    final String limitQueryParam = "limit=2";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0706_WithLimit/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("west", equalTo("-180.0"))
            .withQueryParam("south", equalTo("-90.0"))
            .withQueryParam("east", equalTo("180.0"))
            .withQueryParam("north", equalTo("90.0"))
            .withQueryParam("limit", equalTo("2"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get(
                    "hub/spaces/" + HTTP_SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam + "&"
                            + limitQueryParam,
                    streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0707_testGetByBBox() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0707_BBoxOnly/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("west", equalTo("8.6476"))
            .withQueryParam("south", equalTo("50.1175"))
            .withQueryParam("east", equalTo("8.6729"))
            .withQueryParam("north", equalTo("50.1248"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + HTTP_SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0710_testGetByBBoxWithInvalidCoordinate() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox coordinates are invalid
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-181&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0710_InvalidCoordinate/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + HTTP_SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(400)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request did not reach endpoint
    verify(0, getRequestedFor(urlPathEqualTo(ENDPOINT)));
  }


  @Test
  void tc0711_testGetByBBoxOnViewSpace() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition using View space over psql and http storage based spaces

    // Given: Features By BBox request (against view space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String httpStorageMockResponse =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0711_BBoxOnViewSpace/http_storage_response.json");
    final String expectedViewResponse =
            loadFileOrFail("ReadFeatures/ByBBoxHttpStorage/TC0711_BBoxOnViewSpace/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("west", equalTo("-180.0"))
            .withQueryParam("south", equalTo("-90.0"))
            .withQueryParam("east", equalTo("180.0"))
            .withQueryParam("north", equalTo("90.0"))
            .willReturn(okJson(httpStorageMockResponse)));

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + VIEW_SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedViewResponse, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @ParameterizedTest
  @MethodSource("propSearchTestParams")
  void tc800_testPropertySearch(String inputQueryString, RequestPatternBuilder outputQueryPattern) throws Exception {
    final String bboxQueryParam = "west=-180.0&north=90.0&east=180.0&south=-90.0&limit=30000";

    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    nakshaClient.get("hub/spaces/" + HTTP_SPACE_ID + "/bbox?" + bboxQueryParam + "&" + inputQueryString, streamId);

    stubFor(any(anyUrl()).willReturn(ok()));

    verify(1, outputQueryPattern);
  }

  private static Stream<Arguments> propSearchTestParams(){
    return PropertySearchSamples.queryParams();
  }
}
