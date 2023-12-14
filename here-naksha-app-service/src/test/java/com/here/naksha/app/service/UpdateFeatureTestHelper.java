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

import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.TestUtil.getHeader;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.parseJson;
import static com.here.naksha.app.common.TestUtil.parseJsonFileOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.naksha.Space;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;

public class UpdateFeatureTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public UpdateFeatureTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
    this.app = app;
    this.nakshaClient = nakshaClient;
  }

  void tc0500_testUpdateFeatures() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();

    // Preparation: create storage, event handler, space, and initial features
    final String storage = loadFileOrFail("TC0500_updateFeatures/create_storage.json");
    nakshaClient.post("hub/storages", storage, streamId);
    final String handler = loadFileOrFail("TC0500_updateFeatures/create_handler.json");
    nakshaClient.post("hub/handlers", handler, streamId);
    final String spaceJson = loadFileOrFail("TC0500_updateFeatures/create_space.json");
    nakshaClient.post("hub/spaces", spaceJson, streamId);
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);
    final String createFeaturesJson = loadFileOrFail("TC0500_updateFeatures/create_features.json");
    nakshaClient.post("hub/spaces/" + space.getId() + "/features", createFeaturesJson, streamId);
    // Read request body
    final String bodyJson = loadFileOrFail("TC0500_updateFeatures/update_request.json");
    final String expectedBodyPart = loadFileOrFail("TC0500_updateFeatures/response.json");

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Update Feature response body doesn't match",
        expectedBodyPart,
        response.body(),
        JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  void tc0501_testUpdateFeatureById() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}

    // Read request body
    final String bodyJson = loadFileOrFail("TC0501_updateOneFeatureById/update_request_and_response.json");
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);
    final String expectedBodyPart = bodyJson;
    final String streamId = UUID.randomUUID().toString();

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/" + space.getId() + "/features/my-custom-feature-1", bodyJson, streamId);

    // Then: Perform assertions
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Update Feature response body doesn't match",
        expectedBodyPart,
        response.body(),
        JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  void tc0502_testUpdateFeatureByWrongUriId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}

    // Read request body
    final String bodyJson = loadFileOrFail("TC0502_updateFeatureWithWrongUriId/request.json");
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);
    final String expectedBodyPart = loadFileOrFail("TC0502_updateFeatureWithWrongUriId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/" + space.getId() + "/features/wrong-id", bodyJson, streamId);

    // Then: Perform assertions
    assertEquals(404, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Update Feature error response doesn't match",
        expectedBodyPart,
        response.body(),
        JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  void tc0503_testUpdateFeatureWithMismatchingId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}

    // Read request body
    final String bodyJson = loadFileOrFail("TC0502_updateFeatureWithWrongUriId/request.json");
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);
    final String expectedBodyPart = loadFileOrFail("TC0503_updateFeatureMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/" + space.getId() + "/features/my-custom-feature-1", bodyJson, streamId);

    // Then: Perform assertions
    assertEquals(400, response.statusCode(), "ResCode mismatch");
    JSONAssert.assertEquals(
        "Update Feature error response doesn't match",
        expectedBodyPart,
        response.body(),
        JSONCompareMode.LENIENT);
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
  }

  void tc0504_testUpdateFeaturesNoId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();

    // Read request body
    final String bodyJson = loadFileOrFail("TC0504_updateFeaturesNoIds/request.json");
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);
    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.put("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);

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

  void tc0505_testUpdateFeaturesWithUuid() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);

    final HttpResponse<String> getResponse =
        nakshaClient.get("hub/spaces/" + space.getId() + "/features/my-custom-feature-2", streamId);
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
    final HttpResponse<String> responseUpdateSuccess = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features",
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
    final HttpResponse<String> responseUpdateFail = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features",
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
    final HttpResponse<String> responseOverriding = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features",
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

  void tc0506_testUpdateFeatureWithUuid() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}/features/{featureId}
    final String streamId = UUID.randomUUID().toString();
    final Space space = parseJsonFileOrFail("TC0500_updateFeatures/create_space.json", Space.class);

    final HttpResponse<String> getResponse =
        nakshaClient.get("hub/spaces/" + space.getId() + "/features/newly-inserted", streamId);
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
    final HttpResponse<String> responseUpdateSuccess = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features/newly-inserted", feature.toString(), streamId);

    // Perform first assertions
    assertEquals(200, responseUpdateSuccess.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(responseUpdateSuccess, HDR_STREAM_ID), "StreamId mismatch");
    final XyzFeature updatedFeature = parseJson(responseUpdateSuccess.body(), XyzFeature.class);
    Assertions.assertEquals("30", updatedFeature.getProperties().get("speedLimit"));
    Assertions.assertEquals("no", updatedFeature.getProperties().get("isImportant"));

    // Execute request, outdated UUID, should fail
    feature.setProperties(newPropsOutdatedUuid);
    final HttpResponse<String> responseUpdateFail = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features/newly-inserted", feature.toString(), streamId);

    // Perform second assertions
    assertEquals(409, responseUpdateFail.statusCode(), "ResCode mismatch");

    // Execute request, null UUID, should success with overriding
    feature.setProperties(nullUuidProps);
    final HttpResponse<String> responseOverriding = nakshaClient.put(
        "hub/spaces/" + space.getId() + "/features/newly-inserted", feature.toString(), streamId);

    // Perform third assertions
    assertEquals(200, responseOverriding.statusCode(), "ResCode mismatch");
    final XyzFeature overridenFeature = parseJson(responseOverriding.body(), XyzFeature.class);
    Assertions.assertEquals("yesyesyes", overridenFeature.getProperties().get("overriden"));
    // Old properties like isImportant should no longer be available
    // The feature has been completely overwritten by the PUT request with null UUID
    Assertions.assertFalse(overridenFeature.getProperties().containsKey("isImportant"));
  }
}
