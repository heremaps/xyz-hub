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
import static com.here.naksha.app.common.TestUtil.parseJsonFileOrFail;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.naksha.Space;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;

class CreateFeatureTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
  private static final String SPACE_ID = "create_features_test_space";

  @BeforeAll
  static void setup(){
    setupSpaceAndRelatedResources(nakshaClient, "CreateFeatures/setup");
  }

  @Test
  void tc0300_testCreateFeaturesWithNewIds() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Given: Create Features request (against above Space)
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0300_createFeaturesWithNewIds/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0300_createFeaturesWithNewIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
        .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
        .hasInsertedIdsMatchingFeatureIds(null)
        .hasUuids()
    ;
  }

  @Test
  void tc0301_testCreateFeaturesWithGivenIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given Ids
    String streamId;
    HttpResponse<String> response;

    // Given: Create Features request
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0301_createFeaturesWithGivenIds/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0301_createFeaturesWithGivenIds/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
        .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
        .hasInsertedIdsMatchingFeatureIds(null)
        .hasUuids()
    ;
  }

  // Disabled - we don't want to verify prefix now
  //  @Test
  void tc0302_testCreateFeaturesWithPrefixId() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given prefix Id
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: Feature ID prefix
    final String prefixId = "1000";
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0302_createFeaturesWithPrefixId/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0302_createFeaturesWithPrefixId/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient()
        .post("hub/spaces/" + SPACE_ID + "/features?prefixId=" + urlEncoded(prefixId), bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
        .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
        .hasInsertedIdsMatchingFeatureIds(prefixId)
        .hasUuids()
    ;
  }

  @Test
  void tc0303_testCreateFeaturesWithAddTags() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given addTags param
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: addTags API query param
    final String tagQueryParam = "addTags=100"
        + "&addTags=New_Normalized_Tag"
        + "&addTags=" + urlEncoded("@New_Non_Normalized_Tag")
        + "&addTags=Existing_Normalized_Tag"
        + "&addTags=" + urlEncoded("@Existing_Non_Normalized_Tag");
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0303_createFeaturesWithAddTags/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0303_createFeaturesWithAddTags/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features?" + tagQueryParam, bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
        .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
        .hasInsertedIdsMatchingFeatureIds(null)
        .hasUuids()
    ;
  }

  @Test
  void tc0304_testCreateFeaturesWithRemoveTags() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features getting created successfully with the given removeTags param
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: removeTags API query param
    final String tagQueryParam = "removeTags=non_existing_tag"
        + "&removeTags=Existing_Normalized_Tag"
        + "&removeTags=" + URLEncoder.encode("@Existing_Non_Normalized_Tag", UTF_8);
    // Given: Create Features request
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0304_createFeaturesWithRemoveTags/create_features.json");
    final String expectedBodyPart =
        loadFileOrFail("CreateFeatures/TC0304_createFeaturesWithRemoveTags/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features?" + tagQueryParam, bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
        .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
        .hasInsertedIdsMatchingFeatureIds(null)
        .hasUuids()
    ;
  }

  @Test
  void tc0305_testCreateFeaturesWithDupIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request is successful, when features with the same ID submitted again (replacing existing features)
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: New Features in place
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0305_createFeaturesWithDupIds/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch");

    // When: Create Features request is submitted with the same Ids
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0305_createFeaturesWithDupIds/feature_response_part.json");
    response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match");
  }

  @Test
  void tc0307_testCreateFeaturesWithNoHandler() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request gets failed if we attempt creating features with no associated Event Handler
    String streamId = UUID.randomUUID().toString();

    // Given: Space (without EventHandler) configured in Admin storage
    final Space space = parseJsonFileOrFail("CreateFeatures/TC0307_createFeaturesWithNoHandler/create_space.json", Space.class);
    HttpResponse<String> response = getNakshaClient().post("hub/spaces", space.toString(), streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Event Handler");

    // Given: Create Features request (against above Space)
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0307_createFeaturesWithNoHandler/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0307_createFeaturesWithNoHandler/feature_response_part.json");

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient().post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match");
  }

  @Test
  void tc0308_testCreateFeaturesWithNoSpace() throws Exception {
    // NOTE : This test depends on setup done as part of tc0300_testCreateFeaturesWithNewIds

    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate request gets failed if we attempt creating features on missing space
    String streamId;
    HttpRequest request;
    HttpResponse<String> response;

    // Given: Create Features request (against missing space)
    final String spaceId = "missing_space";
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0308_createFeaturesWithNoSpace/create_features.json");
    final String expectedBodyPart = loadFileOrFail("CreateFeatures/TC0308_createFeaturesWithNoSpace/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = getNakshaClient().post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match");
  }

  @Test
  void tc0309_testCreateFeaturesWithUuid() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();

    // Given: new Features in Space
    final String bodyJson = loadFileOrFail("CreateFeatures/TC0309_createFeaturesWithUuid/create_features.json");
    final HttpResponse<String> response =
        getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch");

    // Given: existing feature is fetched
    final HttpResponse<String> getResponse =
        getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/my-custom-id-309-1", streamId);
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
        .post(
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
        .post(
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
        .post(
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
}
