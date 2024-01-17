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
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.urlEncoded;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ReadFeaturesByBBoxTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_bbox_test_space";

  /*
  For this test suite, we upfront create various Features using different combination of Tags and Geometry.
  To know what exact features we create, check the create_features.json test file for test tc0700_xx().
  And then in subsequent tests, we validate the various GetByBBox APIs using different query parameters.
  */
  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByBBox/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByBBox/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc0700_testGetByBBoxWithSingleTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features getting returned for given BBox coordinate and given single tag value
    // Given: Features By BBox request (against above space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0701_testGetByBBoxWithTagOrCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag OR condition
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=two,three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0701_TagOrCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0702_testGetByBBoxWithTagAndCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag AND condition
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0702_TagAndCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0703_testGetByBBoxWithTagOrOrConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag OR condition using comma separated value

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=three" + "&tags=four,five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0703_TagOrOrCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0704_testGetByBBoxWithTagOrAndConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and combination of Tag OR and AND conditions

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one" + "&tags=two+three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0704_TagOrAndCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0705_testGetByBBoxWithTagAndOrAndConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and combination of Tag AND, OR, AND conditions

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=three+four" + "&tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0705_TagAndOrAndCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
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
        loadFileOrFail("ReadFeatures/ByBBox/TC0706_WithLimit/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get(
            "hub/spaces/" + SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam + "&"
                + limitQueryParam,
            streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0707_testGetByBBox() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0707_BBoxOnly/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0708_testGetByBBox2AndTagAndCondition() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag AND condition
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=three+four";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0708_BBox2_TagAndCondition/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0709_testGetByBBoxWithoutBBox() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox coordinates are not provided
    // Given: Features By BBox request (against configured space)
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0709_WithoutBBox/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/bbox", streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0710_testGetByBBoxWithInvalidCoordinate() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox coordinates are invalid

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-181&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0710_InvalidCoordinate/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient.get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0711_testGetByBBoxWithInvalidTagDelimiter() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox condition is valid but invalid Tag delimiter is used
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one@two";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0711_InvalidTagDelimiter/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0712_testGetByBBoxWithNonNormalizedTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag combination having NonNormalized Tag value
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=non-matching-tag+" + urlEncoded("@ThRee");
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0712_NonNormalizedTag/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0713_testGetByBBoxWithMixedTagConditions() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag combination having mixed AND/OR conditions
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=six,three+four" + "&tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0713_MixedTagConditions/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0714_testGetByBBoxWithTagMismatch() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate NO features returned when features match given BBox, but NOT the given tags
    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0714_NonMatchingTag/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0715_testGetByBBoxWithBBoxMismatch() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate NO features returned when features match given Tags, but NOT the given BBox coordinates
    // Given: Features By BBox request (against configured space)

    final String bboxQueryParam = "west=8.2&south=49.9&east=8.3&north=50";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0715_NonMatchingBBox/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
        .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0716_testBBox2WithSearchOnStringEqualNumberEqual() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (String equals, Number equals)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.speedLimit='70'&p.length=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0716_BBox2_PropStrEqualNumEqual/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0717_testBBox2WithSearchOnStringNotEqualNumberNotEqual() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (String Not equals, Number Not equals)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.speedLimit!='70'&p.length!=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0717_BBox2_PropStrNotEqualNumNotEqual/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0718_testBBox2WithSearchOnFieldNullNumberEqual() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Field Missing, Number Equals)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.direction=.null&p.length=10";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0718_BBox2_PropFieldNullNumEqual/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0719_testBBox2WithSearchOnFieldNotNullNumberGreater() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Field Not Null, Number greater)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.direction!=.null&p.length=gt=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0719_BBox2_PropFieldNotNullNumGreater/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0720_testBBox2WithSearchOnNumberGTE() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Number greater than or equal to)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.length=gte=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0720_BBox2_PropNumGTE/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0721_testBBox2WithSearchOnNumberLTE() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Number lesser than or equal to)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.length=lte=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0721_BBox2_PropNumLTE/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0722_testBBox2WithSearchOnNumberGT() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Number greater than)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.length=gt=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0722_BBox2_PropNumGT/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0723_testBBox2WithSearchOnNumberLT() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Number lesser than)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.length=lt=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0723_BBox2_PropNumLT/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0724_testBBox2WithSearchOnArrayContains() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Array "contains")

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.references=cs=%s,%s".formatted(
            urlEncoded("{\"id\":\"urn:here::here:Topology:1\"}"),
            urlEncoded("{\"id\":\"urn:here::here:Topology:2\"}")
    );
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0724_BBox2_PropArrayContains/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0725_testBBox2WithSearchOnObjectContains() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Object "contains")

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "%s=cs=%s".formatted(
            urlEncoded("p.@ns:com:here:mom:meta"),
            urlEncoded("{\"sourceId\":\"Task_2\"}")
    );
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0725_BBox2_PropObjectContains/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0726_testBBox2WithSearchUsingInvalidDelimiter() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate graceful error returned when BBox parameters are valid
    // but property search parameters has invalid delimiter

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=%s,%s".formatted(
            urlEncoded("@ThRee"),
            urlEncoded("@Four")
    );
    final String propQueryParam = "p.length=gte=10+0.5"; // invalid delimiter +
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0726_BBox2_PropInvalidDelimiter/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(400)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0727_testBBox2WithSearchUsingInvalidStringOperation() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate graceful error returned when BBox parameters are valid
    // but property search parameters has invalid operation on String value

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=%s,%s".formatted(
            urlEncoded("@ThRee"),
            urlEncoded("@Four")
    );
    final String propQueryParam = "p.speedLimit=gte='60'"; // invalid operation =gte= on String
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0727_BBox2_PropInvalidStringOperation/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(400)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0728_testBBox2WithPropWithTags() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with all the given filter parameters
    // (i.e. BBox co-ordinates, Tags search, Prop search)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=%s,%s".formatted(
            urlEncoded("@ThRee"),
            urlEncoded("@Four")
    );
    final String propQueryParam = "p.length=gt=10.5";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0728_BBox2_Prop_Tags/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0729_testBBox2WithPropNoMatch() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate NO features returned when BBox coordinates and Tags filters are matching
    // but given property search filter doesn't match any features

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=%s,%s".formatted(
            urlEncoded("@ThRee"),
            urlEncoded("@Four")
    );
    final String propQueryParam = "p.length=gt=12"; // No features should match this condition
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0729_BBox2_PropNoMatch/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0730_testBBox2WithPropBoolEqual() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox and Search conditions (Boolean equals)

    // Given: Features By BBox request (against configured space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String propQueryParam = "p.isBidirectional=true";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByBBox/TC0730_BBox2_PropBoolEqual/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }






}
