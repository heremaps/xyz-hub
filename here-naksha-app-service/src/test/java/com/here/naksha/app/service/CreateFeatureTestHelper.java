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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.Space;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;

public class CreateFeatureTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public CreateFeatureTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
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
        "Create Feature response body doesn't match",
        expectedBodyPart,
        actualResponse.body(),
        JSONCompareMode.LENIENT);
    assertEquals(expectedStreamId, getHeader(actualResponse, HDR_STREAM_ID), "StreamId mismatch");
  }

  private void additionalCustomAssertions(
      final @NotNull String reqBody, final @NotNull String resBody, final @Nullable String prefixId)
      throws JSONException {
    final FeatureCollectionRequest collectionRequest = parseJson(reqBody, FeatureCollectionRequest.class);
    final XyzFeatureCollection collectionResponse = parseJson(resBody, XyzFeatureCollection.class);
    final List<String> insertedIds = collectionResponse.getInserted();
    final List<XyzFeature> features = collectionResponse.getFeatures();
    JSONAssert.assertEquals(
        "{inserted:[" + collectionRequest.getFeatures().size() + "]}",
        resBody,
        new ArraySizeComparator(JSONCompareMode.LENIENT));
    assertEquals(
        insertedIds.size(),
        features.size(),
        "Mismatch between inserted and features list size in the response");
    for (int i = 0; i < insertedIds.size(); i++) {
      if (prefixId != null) {
        assertTrue(
            insertedIds.get(i).startsWith(prefixId),
            "Feature Id in the response doesn't start with given prefix Id : " + prefixId);
      }
      assertEquals(
          insertedIds.get(i),
          features.get(i).getId(),
          "Mismatch between inserted v/s feature ID in the response at idx : " + i);
      assertNotNull(
          features.get(i).getProperties().getXyzNamespace().getUuid(),
          "UUID found missing in response for feature at idx : " + i);
    }
  }

  public void tc0300_testCreateFeaturesWithNewIds() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with dynamic new Ids
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: Storage (mock implementation) configured in Admin storage
    final String storageJson = loadFileOrFail("TC0300_createFeaturesWithNewIds/create_storage.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/storages", storageJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Storage");

    // Given: EventHandler (uses above Storage) configured in Admin storage
    final String eventHandlerJson = loadFileOrFail("TC0300_createFeaturesWithNewIds/create_event_handler.json");
    response = nakshaClient.post("hub/handlers", eventHandlerJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Space (uses above EventHandler) configured in Admin storage
    final Space space = parseJsonFileOrFail("TC0300_createFeaturesWithNewIds/create_space.json", Space.class);
    final String spaceJsonString = loadFileOrFail("TC0300_createFeaturesWithNewIds/create_space.json");
    response = nakshaClient.post("hub/spaces", spaceJsonString, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Create Features request (against above Space)
    final String bodyJson = loadFileOrFail("TC0300_createFeaturesWithNewIds/create_features.json");
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("TC0300_createFeaturesWithNewIds/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("TC0300_createFeaturesWithNewIds/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(bodyJson, response.body(), null);
  }

  public void tc0301_testCreateFeaturesWithGivenIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given Ids
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: existing space
    final String spaceId = "um-mod-topology-dev";
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("TC0301_createFeaturesWithGivenIds/create_features.json");
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("TC0301_createFeaturesWithGivenIds/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("TC0301_createFeaturesWithGivenIds/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(bodyJson, response.body(), null);
  }

  public void tc0302_testCreateFeaturesWithPrefixId() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given prefix Id
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: existing space
    final String spaceId = "um-mod-topology-dev";
    // Given: Feature ID prefix
    final String prefixId = "my-custom-prefix:";
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("TC0302_createFeaturesWithPrefixId/create_features.json");
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("TC0302_createFeaturesWithPrefixId/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("TC0302_createFeaturesWithPrefixId/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post(
        "hub/spaces/" + spaceId + "/features?prefixId=" + utf8Encoded(prefixId), bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(bodyJson, response.body(), prefixId);
  }

  public void tc0303_testCreateFeaturesWithAddTags() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given addTags param
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: existing space
    final String spaceId = "um-mod-topology-dev";
    // Given: addTags API query param
    final String tagQueryParam = "addTags=New_Normalized_Tag"
        + "&addTags=" + utf8Encoded("@New_Non_Normalized_Tag")
        + "&addTags=Existing_Normalized_Tag"
        + "&addTags=" + utf8Encoded("@Existing_Non_Normalized_Tag");
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("TC0303_createFeaturesWithAddTags/create_features.json");
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("TC0303_createFeaturesWithAddTags/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("TC0303_createFeaturesWithAddTags/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features?" + tagQueryParam, bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(bodyJson, response.body(), null);
  }

  public void tc0304_testCreateFeaturesWithRemoveTags() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given removeTags param
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: existing space
    final String spaceId = "um-mod-topology-dev";
    // Given: removeTags API query param
    final String tagQueryParam = "removeTags=non_existing_tag"
        + "&removeTags=Existing_Normalized_Tag"
        + "&removeTags=" + URLEncoder.encode("@Existing_Non_Normalized_Tag", UTF_8);
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("TC0304_createFeaturesWithRemoveTags/create_features.json");
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("TC0304_createFeaturesWithRemoveTags/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("TC0304_createFeaturesWithRemoveTags/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features?" + tagQueryParam, bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(bodyJson, response.body(), null);
  }

  public void tc0305_testCreateFeaturesWithDupIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request gets failed if we attempt creating features with existing IDs
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: existing space
    final String spaceId = "um-mod-topology-dev";
    // Given: New Features in place
    final String bodyJson = loadFileOrFail("TC0305_createFeaturesWithDupIds/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch");

    // When: Create Features request is submitted with the same Ids
    final String expectedBodyPart = loadFileOrFail("TC0305_createFeaturesWithDupIds/feature_response_part.json");
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 409, expectedBodyPart, streamId);
  }

  public void tc0307_testCreateFeaturesWithNoHandler() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request gets failed if we attempt creating features with no associated Event Handler
    String streamId = UUID.randomUUID().toString();

    // Given: Space (without EventHandler) configured in Admin storage
    final Space space = parseJsonFileOrFail("TC0307_createFeaturesWithNoHandler/create_space.json", Space.class);
    HttpResponse<String> response = nakshaClient.post("hub/spaces", space.toString(), streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Create Features request (against above Space)
    final String bodyJson = loadFileOrFail("TC0307_createFeaturesWithNoHandler/create_features.json");
    final String expectedBodyPart = loadFileOrFail("TC0307_createFeaturesWithNoHandler/feature_response_part.json");

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }

  public void tc0308_testCreateFeaturesWithNoSpace() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request gets failed if we attempt creating features on missing space
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: Create Features request (against missing space)
    final String spaceId = "missing_space";
    final String bodyJson = loadFileOrFail("TC0308_createFeaturesWithNoSpace/create_features.json");
    final String expectedBodyPart = loadFileOrFail("TC0308_createFeaturesWithNoSpace/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }

  private String utf8Encoded(String text) {
    return URLEncoder.encode(text, UTF_8);
  }
}
