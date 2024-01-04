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
import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.TestUtil.getHeader;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.parseJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;

class UpdateFeatureTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "update_feature_test_space";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "UpdateFeatures/setup");
    String initialFeaturesJson = loadFileOrFail("UpdateFeatures/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc0500_testUpdateFeatures() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();
    final String bodyJson = loadFileOrFail("UpdateFeatures/TC0500_updateFeatures/update_request.json");
    final String expectedBodyPart = loadFileOrFail("UpdateFeatures/TC0500_updateFeatures/response.json");

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Update Feature response body doesn't match");
  }

  @Test
  void tc0501_testUpdateFeatureById() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}

    // Read request body
    final String bodyJson = loadFileOrFail("UpdateFeatures/TC0501_updateOneFeatureById/update_request_and_response.json");
    final String expectedBodyPart = bodyJson;
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient()
        .put("hub/spaces/" + SPACE_ID + "/features/my-custom-feature-1", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Update Feature response body doesn't match");
  }

  @Test
  void tc0502_testUpdateFeatureByWrongUriId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}
    // Given: Read request body
    final String bodyJson = loadFileOrFail("UpdateFeatures/TC0502_updateFeatureWithWrongUriId/request.json");
    final String expectedBodyPart = loadFileOrFail("UpdateFeatures/TC0502_updateFeatureWithWrongUriId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/" + SPACE_ID + "/features/wrong-id", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Update Feature error response doesn't match");
  }

  @Test
  void tc0503_testUpdateFeatureWithMismatchingId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}
    // Given: Existing feature
    final String createJson = loadFileOrFail("UpdateFeatures/TC0503_updateFeatureMismatchingId/create_features.json");
    final String updateJson = loadFileOrFail("UpdateFeatures/TC0503_updateFeatureMismatchingId/update.json");
    final String expectedBodyPart = loadFileOrFail("UpdateFeatures/TC0503_updateFeatureMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces/" + SPACE_ID, createJson, streamId);

    // When: updating existing feature with mismatching (id) update rquest
    final HttpResponse<String> response = getNakshaClient()
        .put("hub/spaces/" + SPACE_ID + "/features/tc_503_test_feature", updateJson, streamId);

    // Then: Update failed
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Update Feature error response doesn't match");
  }

  @Test
  void tc0504_testUpdateFeaturesNoId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    // Given: Read request body
    final String streamId = UUID.randomUUID().toString();
    final String bodyJson = loadFileOrFail("UpdateFeatures/TC0504_updateFeaturesNoIds/request.json");
    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");

    final XyzFeatureCollection responseFeatureCollection = parseJson(response.body(), XyzFeatureCollection.class);
    Assertions.assertNotNull(responseFeatureCollection);
    final List<String> inserted = responseFeatureCollection.getInserted();
    final List<XyzFeature> insertedFeatures = responseFeatureCollection.getFeatures();
    JSONAssert.assertEquals(
        "{inserted:[" + inserted.size() + "]}",
        response.body(),
        new ArraySizeComparator(JSONCompareMode.LENIENT));

    for (int i = 0; i < inserted.size(); i++) {
      assertEquals(
          inserted.get(i),
          insertedFeatures.get(i).getId(),
          "Mismatch between inserted v/s feature ID in the response at idx : " + i);
      assertNotNull(
          insertedFeatures.get(i).getProperties().getXyzNamespace().getUuid(),
          "UUID found missing in response for feature at idx : " + i);
    }
  }

  @Test
  void tc0505_testUpdateFeaturesWithUuid() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();
    final HttpResponse<String> getResponse =
        getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/my-custom-feature-2", streamId);
    final XyzFeature feature = parseJson(getResponse.body(), XyzFeature.class);
    Assertions.assertNotNull(feature);
    final XyzProperties newPropsOldUuid = feature.getProperties();
    final XyzProperties newPropsOutdatedUuid = newPropsOldUuid.deepClone();
    final XyzProperties nullUuidProps = new XyzProperties();
    // Old UUID
    newPropsOldUuid.put("speedLimit", "30");
    // New UUID
    newPropsOutdatedUuid.put("speedLimit", "120");
    // Null UUID
    nullUuidProps.put("uuid", null);
    nullUuidProps.put("overriden", "yesyesyes");

    // Execute request, correct UUID, should success
    feature.setProperties(newPropsOldUuid);
    final HttpResponse<String> responseUpdateSuccess = getNakshaClient()
        .put(
            "hub/spaces/" + SPACE_ID + "/features",
            """
                {
                "type": "FeatureCollection",
                "features": [
                """ + feature + "]}",
            streamId);

    // Perform first assertions
    assertEquals(200, responseUpdateSuccess.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(responseUpdateSuccess, HDR_STREAM_ID), "StreamId mismatch");
    final XyzFeatureCollection responseFeatureCollection =
        parseJson(responseUpdateSuccess.body(), XyzFeatureCollection.class);
    Assertions.assertNotNull(responseFeatureCollection);
    final XyzFeature updatedFeature =
        responseFeatureCollection.getFeatures().get(0);
    Assertions.assertEquals("30", updatedFeature.getProperties().get("speedLimit"));

    // Execute request, outdated UUID, should fail
    feature.setProperties(newPropsOutdatedUuid);
    final HttpResponse<String> responseUpdateFail = getNakshaClient()
        .put(
            "hub/spaces/" + SPACE_ID + "/features",
            """
                {
                "type": "FeatureCollection",
                "features": [
                """ + feature + "]}",
            streamId);

    // Perform second assertions
    assertEquals(409, responseUpdateFail.statusCode(), "ResCode mismatch");

    // Execute request, null UUID, should success with overriding
    feature.setProperties(nullUuidProps);
    final HttpResponse<String> responseOverriding = getNakshaClient()
        .put(
            "hub/spaces/" + SPACE_ID + "/features",
            """
                {
                "type": "FeatureCollection",
                "features": [
                """ + feature + "]}",
            streamId);

    // Perform third assertions
    assertEquals(200, responseOverriding.statusCode(), "ResCode mismatch");
    final XyzFeatureCollection featureCollection = parseJson(responseOverriding.body(), XyzFeatureCollection.class);
    Assertions.assertNotNull(featureCollection);
    final XyzFeature overridenFeature = featureCollection.getFeatures().get(0);
    Assertions.assertEquals("yesyesyes", overridenFeature.getProperties().get("overriden"));
    // Old properties like speedLimit should no longer be available
    // The feature has been completely overwritten by the PUT request with null UUID
    Assertions.assertFalse(overridenFeature.getProperties().containsKey("speedLimit"));
  }

  @Test
  void tc0506_testUpdateFeatureWithUuid() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}
    // Given: existing feature
    final String createJson = loadFileOrFail("UpdateFeatures/TC0506_updateWithUuid/create_features.json");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", createJson, streamId);

    final HttpResponse<String> getResponse =
        getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/tc_506_test_feature", streamId);
    final XyzFeature feature = parseJson(getResponse.body(), XyzFeature.class);
    Assertions.assertNotNull(feature);

    // When: preparing new properties
    final XyzProperties newPropsOldUuid = feature.getProperties();
    newPropsOldUuid.put("speedLimit", "30");
    newPropsOldUuid.put("this_test_id", "tc_506");
    // And: executing new properties update
    feature.setProperties(newPropsOldUuid);
    final HttpResponse<String> responseUpdateSuccess = getNakshaClient()
        .put("hub/spaces/" + SPACE_ID + "/features/tc_506_test_feature", feature.toString(), streamId);

    // Then: repsonse indicate success
    assertThat(responseUpdateSuccess)
        .hasStatus(200)
        .hasStreamIdHeader(streamId);

    // And: update properties match
    final XyzFeature updatedFeature = parseJson(responseUpdateSuccess.body(), XyzFeature.class);
    Assertions.assertEquals("30", updatedFeature.getProperties().get("speedLimit"));
    Assertions.assertEquals("tc_506", updatedFeature.getProperties().get("this_test_id"));

    // When: trying to update with outdated UUID
    final XyzProperties newPropsOutdatedUuid = newPropsOldUuid.deepClone();
    newPropsOutdatedUuid.put("speedLimit", "120");
    feature.setProperties(newPropsOutdatedUuid);
    final HttpResponse<String> responseUpdateFail = getNakshaClient()
        .put("hub/spaces/" + SPACE_ID + "/features/tc_506_test_feature", feature.toString(), streamId);

    // Then: update fails
    assertThat(responseUpdateFail).hasStatus(409);

    // When: updating with null uuid props
    final XyzProperties nullUuidProps = new XyzProperties();
    nullUuidProps.put("uuid", null);
    nullUuidProps.put("overriden", "yesyesyes");
    feature.setProperties(nullUuidProps);
    final HttpResponse<String> responseOverriding = getNakshaClient()
        .put("hub/spaces/" + SPACE_ID + "/features/tc_506_test_feature", feature.toString(), streamId);

    // Then: update suceeds
    assertThat(responseOverriding).hasStatus(200);
    final XyzFeature overridenFeature = parseJson(responseOverriding.body(), XyzFeature.class);
    Assertions.assertEquals("yesyesyes", overridenFeature.getProperties().get("overriden"));
    // Old properties like isImportant should no longer be available
    // The feature has been completely overwritten by the PUT request with null UUID
    Assertions.assertFalse(overridenFeature.getProperties().containsKey("this_test_id"));
  }
}
