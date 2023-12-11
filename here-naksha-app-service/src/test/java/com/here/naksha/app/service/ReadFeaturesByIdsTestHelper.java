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
import static com.here.naksha.app.common.TestUtil.newTestNakshaContext;
import static com.here.naksha.app.common.TestUtil.parseJson;
import static com.here.naksha.app.common.TestUtil.parseJsonFileOrFail;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.IfConflict;
import com.here.naksha.lib.core.models.storage.IfExists;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class ReadFeaturesByIdsTestHelper {

  final @NotNull NakshaApp app;
  final @NotNull NakshaTestWebClient nakshaClient;

  public ReadFeaturesByIdsTestHelper(final @NotNull NakshaApp app, final @NotNull NakshaTestWebClient nakshaClient) {
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

  private void additionalCustomAssertions(final @NotNull String resBody) {
    final XyzFeatureCollection collectionResponse = parseJson(resBody, XyzFeatureCollection.class);
    final List<XyzFeature> features = collectionResponse.getFeatures();
    for (int i = 0; i < features.size(); i++) {
      assertNotNull(
          features.get(i).getProperties().getXyzNamespace().getUuid(),
          "UUID found missing in response for feature at idx : " + i);
    }
  }

  public void tc0400_testReadFeaturesByIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate features getting returned for existing Ids and not failing due to missing ids
    String streamId;
    HttpResponse<String> response;

    // TODO : Need to replace setup steps when EventHandler and Space REST API tests are available
    // Given: Storage (mock implementation) configured in Admin storage
    final String storageJson =
        loadFileOrFail("ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/create_storage.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/storages", storageJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating Storage");

    // Given: EventHandler (uses above Storage) configured in Admin storage
    final EventHandler eventHandler = parseJsonFileOrFail(
        "ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/create_event_handler.json", EventHandler.class);
    final WriteXyzFeatures ehRequest = createFeatureRequest(
        NakshaAdminCollection.EVENT_HANDLERS, eventHandler, IfExists.REPLACE, IfConflict.REPLACE);
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(ehRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating EventHandler");
      writer.commit(true);
    }

    // Given: Space (uses above EventHandler) configured in Admin storage
    final Space space =
        parseJsonFileOrFail("ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/create_space.json", Space.class);
    final WriteXyzFeatures spRequest =
        createFeatureRequest(NakshaAdminCollection.SPACES, space, IfExists.REPLACE, IfConflict.REPLACE);
    try (final IWriteSession writer =
        app.getHub().getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(spRequest);
      assertTrue(result instanceof SuccessResult, "Failed creating Space");
      writer.commit(true);
    }

    // Given: New Features persisted in above Space
    String bodyJson = loadFileOrFail("ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/create_features.json");
    streamId = UUID.randomUUID().toString();
    response = nakshaClient.post("hub/spaces/" + space.getId() + "/features", bodyJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch. Failed creating new Features");

    // Given: Features By Ids request (against above space)
    final String idsQueryParam =
        "id=my-custom-id-400-1" + "&id=my-custom-id-400-2" + "&id=missing-id-1" + "&id=missing-id-2";
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/feature_response_part.json");
    final String expectedBodyPart = loadFileOrFail(
        "ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + space.getId() + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(response.body());
  }

  public void tc0401_testReadFeaturesForMissingIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate empty collection getting returned for missing ids
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Ids request (against configured space)
    final String spaceId = "local-space-4-feature-by-id";
    final String idsQueryParam = "?id=1000" + "&id=missing-id-1" + "&id=missing-id-2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0401_MissingIds/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features" + idsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);
  }

  public void tc0402_testReadFeaturesWithoutIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate request gets failed due to missing Id parameter
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Ids request (against configured space)
    final String spaceId = "local-space-4-feature-by-id";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0402_WithoutIds/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features", streamId);

    // Then: Perform assertions
    standardAssertions(response, 400, expectedBodyPart, streamId);
  }

  public void tc0403_testReadFeaturesByIdsFromMissingSpace() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate request getting failed due to missing space
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Ids request (against configured space)
    final String spaceId = "missing-space";
    final String idsQueryParam = "&id=some-id-1";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0403_ByIdsFromMissingSpace/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }

  public void tc0404_testReadFeatureById() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate feature getting returned for given Id
    String streamId;
    HttpResponse<String> response;

    // Given: Feature By Id request (against already existing space)
    final String spaceId = "local-space-4-feature-by-id";
    final String featureId = "my-custom-id-400-1";
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("ReadFeatures/ByIds/TC0404_ExistingId/feature_response_part.json");
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0404_ExistingId/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features/" + featureId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    final XyzFeature feature = parseJson(response.body(), XyzFeature.class);
    assertNotNull(
        feature.getProperties().getXyzNamespace().getUuid(), "UUID found missing in response for feature");
  }

  public void tc0405_testReadFeatureForMissingId() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate request gets failed when attempted to load feature for missing Id
    String streamId;
    HttpResponse<String> response;

    // Given: Feature By Id request, against existing space, for missing feature Id
    final String spaceId = "local-space-4-feature-by-id";
    final String featureId = "missing-id";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0405_MissingId/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features/" + featureId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }

  public void tc0406_testReadFeatureByIdFromMissingSpace() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate request gets failed when attempted to load feature from missing space
    String streamId;
    HttpResponse<String> response;

    // Given: Feature By Id request (against missing space)
    final String spaceId = "missing-space";
    final String featureId = "my-custom-id-400-1";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0406_ByIdFromMissingSpace/feature_response_part.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features/" + featureId, streamId);

    // Then: Perform assertions
    standardAssertions(response, 404, expectedBodyPart, streamId);
  }

  public void tc0407_testReadFeaturesWithCommaSeparatedIds() throws Exception {
    // NOTE : This test depends on setup done as part of tc0400_testReadFeaturesByIds

    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate features getting returned for Ids provided as comma separated values
    String streamId;
    HttpResponse<String> response;

    // Given: Features By Ids request (against existing space)
    final String spaceId = "local-space-4-feature-by-id";
    final String idsQueryParam = "id=my-custom-id-400-1,my-custom-id-400-2,missing-id-1,missing-id-2";
    // TODO: include geometry after Cursor-related changes ->
    // loadFileOrFail("ReadFeatures/ByIds/TC0407_CommaSeparatedIds/feature_response_part.json");
    final String expectedBodyPart = loadFileOrFail(
        "ReadFeatures/ByIds/TC0407_CommaSeparatedIds/feature_response_part_without_geometry.json");
    streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.get("hub/spaces/" + spaceId + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    standardAssertions(response, 200, expectedBodyPart, streamId);

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(response.body());
  }
}
