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
import static com.here.naksha.app.common.TestUtil.parseJsonFileOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DeleteFeatureTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "delete_features_test_space";

  public DeleteFeatureTest() {
    super(nakshaClient);
  }

  @BeforeAll
  static void setup() {
    setupSpaceAndRelatedResources(nakshaClient, "DeleteFeatures/setup");
  }

  @Test
  void tc0900_testDeleteFeatures() throws Exception {
    // Test API : DELETE /hub/spaces/{spaceId}/features
    // Given: initial features
    final String streamId = UUID.randomUUID().toString();
    final String createFeaturesJson = loadFileOrFail("DeleteFeatures/TC0900_deleteFeatures/create_features.json");
    HttpResponse<String> response = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch, failure creating initial features");

    // When: request is submitted to NakshaHub Space Storage instance
    response = nakshaClient
        .delete(
            "hub/spaces/" + SPACE_ID + "/features?id=feature-1-to-delete&id=feature-2-to-delete",
            streamId);

    // Then: Perform assertions
    final String expectedBodyPart = loadFileOrFail("DeleteFeatures/TC0900_deleteFeatures/response.json");
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Delete Feature response body doesn't match");
  }

  @Test
  void tc0901_testDeleteNonExistingFeatures() throws Exception {
    // Test API : DELETE /hub/spaces/{spaceId}/features
    final String streamId = UUID.randomUUID().toString();
    HttpResponse<String> response;

    final String expectedBodyPart = loadFileOrFail("DeleteFeatures/TC0901_deleteNonExistingFeatures/response.json");
    // Create features to be deleted
    final String createFeaturesJson = loadFileOrFail("DeleteFeatures/TC0900_deleteFeatures/create_features.json");
    response = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch, failure creating initial features");

    // Test deleting only non existing features
    // When: request is submitted to NakshaHub Space Storage instance
    response = nakshaClient
        .delete("hub/spaces/" + SPACE_ID + "/features?id=non-existing-phantom-feature", streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Delete Feature response body doesn't match");

    // Test deleting some phantom features among other existing features
    // When: request is submitted to NakshaHub Space Storage instance
    response = nakshaClient
        .delete(
            "hub/spaces/" + SPACE_ID
                + "/features?id=non-existing-phantom-feature&id=feature-1-to-delete&id=feature-2-to-delete",
            streamId);

    // Then: Perform assertions
    final String expectedDeleteOfExisting = loadFileOrFail("DeleteFeatures/TC0900_deleteFeatures/response.json");
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedDeleteOfExisting, "Delete Feature response body doesn't match");
  }

  @Test
  void tc0902_testDeleteFeatureById() throws Exception {
    // Test API : DELETE /hub/spaces/{spaceId}/features/{featureId}
    final String streamId = UUID.randomUUID().toString();
    HttpResponse<String> response;

    final String createFeaturesJson = loadFileOrFail("DeleteFeatures/TC0902_deleteFeatureById/create_features.json");
    response = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);
    assertEquals(200, response.statusCode(), "ResCode mismatch, failure creating initial features");
    final String expectedBodyPart = loadFileOrFail("DeleteFeatures/TC0902_deleteFeatureById/response.json");

    // When: request is submitted to NakshaHub Space Storage instance
    response = nakshaClient.delete("hub/spaces/" + SPACE_ID + "/features/feature-3-to-delete", streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Delete Feature response body doesn't match");
  }

  @Test
  void tc0903_testDeleteFeatureByWrongId() throws Exception {
    // Test API : DELETE /hub/spaces/{spaceId}/features/{featureId}
    final String streamId = UUID.randomUUID().toString();
    final String expectedBodyPart = loadFileOrFail("DeleteFeatures/TC0903_deleteFeatureByWrongId/response.json");

    // When: request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = nakshaClient
        .delete("hub/spaces/" + SPACE_ID + "/features/phantom-feature-not-real", streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Delete Feature response body doesn't match");
  }
}
