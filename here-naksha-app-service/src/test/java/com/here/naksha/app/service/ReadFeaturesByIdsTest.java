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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ReadFeaturesByIdsTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_ids_test_space";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByIds/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByIds/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc0400_testReadFeaturesByIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate features getting returned for existing Ids and not failing due to missing ids
    // Given: Features By Ids request (against above space)
    final String idsQueryParam =
        "id=my-custom-id-400-1" + "&id=my-custom-id-400-2" + "&id=missing-id-1" + "&id=missing-id-2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0400_ExistingAndMissingIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(response.body());
  }

  @Test
  void tc0401_testReadFeaturesForMissingIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate empty collection getting returned for missing ids
    // Given: Features By Ids request (against configured space)
    final String idsQueryParam = "?id=1000" + "&id=missing-id-1" + "&id=missing-id-2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0401_MissingIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0402_testReadFeaturesWithoutIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate request gets failed due to missing Id parameter
    // Given: Features By Ids request (against configured space)
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0402_WithoutIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0403_testReadFeaturesByIdsFromMissingSpace() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate request getting failed due to missing space
    // Given: Features By Ids request (against configured space)
    final String missingSpaceId = "missing-space";
    final String idsQueryParam = "&id=some-id-1";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0403_ByIdsFromMissingSpace/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + missingSpaceId + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0404_testReadFeatureById() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate feature getting returned for given Id
    // Given: Feature By Id request (against already existing space)
    final String featureId = "my-custom-id-400-1";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0404_ExistingId/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    final XyzFeature feature = parseJson(response.body(), XyzFeature.class);
    assertNotNull(
        feature.getProperties().getXyzNamespace().getUuid(), "UUID found missing in response for feature");
  }

  @Test
  void tc0405_testReadFeatureForMissingId() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate request gets failed when attempted to load feature for missing Id
    // Given: Feature By Id request, against existing space, for missing feature Id
    final String featureId = "missing-id";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0405_MissingId/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0406_testReadFeatureByIdFromMissingSpace() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate request gets failed when attempted to load feature from missing space
    // Given: Feature By Id request (against missing space)
    final String missingSpaceId = "missing-space";
    final String featureId = "my-custom-id-400-1";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0406_ByIdFromMissingSpace/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + missingSpaceId + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc0407_testReadFeaturesWithCommaSeparatedIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate features getting returned for Ids provided as comma separated values
    // Given: Features By Ids request (against existing space)
    final String idsQueryParam = "id=my-custom-id-400-1,my-custom-id-400-2,missing-id-1,missing-id-2";
    final String expectedBodyPart =
        loadFileOrFail("ReadFeatures/ByIds/TC0407_CommaSeparatedIds/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: Create Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: also match individual JSON attributes (in addition to whole object comparison above)
    additionalCustomAssertions(response.body());
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
}
