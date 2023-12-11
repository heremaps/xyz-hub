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
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.*;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class ReadFeaturesByBBoxTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public ReadFeaturesByBBoxTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
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
  To know what exact features we create, check the create_features.json test file for test tc0700_xx().
  And then in subsequent tests, we validate the various GetByBBox APIs using different query parameters.
  */

  public void tc0700_testGetByBBoxWithSingleTag() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features getting returned for given BBox coordinate and given single tag value
    String streamId;
    HttpResponse<String> response;

    // Given: Storage (mock implementation) configured in Admin storage
    final String storageJson = loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/create_storage.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/storages", storageJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Storage");

    // Given: EventHandler (uses above Storage) configured in Admin storage
    final String handlerJson = loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/create_event_handler.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/handlers", handlerJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Space (uses above EventHandler) configured in Admin storage
    final String spaceJson = loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/create_space.json");
    final Space space = TestUtil.parseJson(spaceJson, Space.class);
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces", spaceJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Space");

    // Given: New Features persisted in above Space
    String bodyJson = loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating new Features");

    // Given: Features By BBox request (against above space)
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0700_SingleTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response = nakshaClient.get(
        "hub/spaces/" + space.getId() + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0701_testGetByBBoxWithTagOrCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag OR condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=two,three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0701_TagOrCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0702_testGetByBBoxWithTagAndCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag AND condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0702_TagAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0703_testGetByBBoxWithTagOrOrConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag OR condition using comma separated value
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=three" + "&tags=four,five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0703_TagOrOrCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0704_testGetByBBoxWithTagOrAndConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and combination of Tag OR and AND conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one" + "&tags=two+three";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0704_TagOrAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0705_testGetByBBoxWithTagAndOrAndConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and combination of Tag AND, OR, AND conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=three+four" + "&tags=four+five";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0705_TagAndOrAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0706_testGetByBBoxWithLimit() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and limit
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one";
    final String limitQueryParam = "limit=2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0706_WithLimit/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response = nakshaClient.get(
        "hub/spaces/" + spaceId + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam + "&" + limitQueryParam,
        streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0707_testGetByBBox() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0707_BBoxOnly/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0708_testGetByBBox2AndTagAndCondition() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag AND condition
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String tagsQueryParam = "tags=three+four";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0708_BBox2_TagAndCondition/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0709_testGetByBBoxWithoutBBox() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox coordinates are not provided
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0709_WithoutBBox/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/bbox", streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0710_testGetByBBoxWithInvalidCoordinate() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox coordinates are invalid
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-181&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0710_InvalidCoordinate/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response = nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0711_testGetByBBoxWithInvalidTagDelimiter() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate API error when BBox condition is valid but invalid Tag delimiter is used
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=one@two";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0711_InvalidTagDelimiter/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0712_testGetByBBoxWithNonNormalizedTag() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag combination having NonNormalized Tag value
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=non-matching-tag+" + urlEncoded("@ThRee");
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0712_NonNormalizedTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0713_testGetByBBoxWithMixedTagConditions() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate features returned match with given BBox condition and Tag combination having mixed AND/OR conditions
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=six,three+four" + "&tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0713_MixedTagConditions/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0714_testGetByBBoxWithTagMismatch() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate NO features returned when features match given BBox, but NOT the given tags
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=non-existing-tag";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0714_NonMatchingTag/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0715_testGetByBBoxWithBBoxMismatch() throws Exception {
    // NOTE : This test depends on setup done as part of tc0700_testGetByBBoxWithSingleTag

    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate NO features returned when features match given Tags, but NOT the given BBox coordinates
    String streamId;
    HttpResponse<String> response;

    // Given: Features By BBox request (against configured space)
    final String spaceId = "local-space-4-features-by-bbox";
    // final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String bboxQueryParam = "west=8.2&south=49.9&east=8.3&north=50";
    final String tagsQueryParam = "tags=one";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByBBox/TC0715_NonMatchingBBox/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    response =
        nakshaClient.get("hub/spaces/" + spaceId + "/bbox?" + bboxQueryParam + "&" + tagsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }
}
