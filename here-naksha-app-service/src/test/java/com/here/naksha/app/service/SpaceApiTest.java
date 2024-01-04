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

import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.TestUtil.getHeader;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.parseJson;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.Space;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SpaceApiTest extends ApiTest {

  @Test
  void tc0200_testCreateSpace() throws Exception {
    // Test API : POST /hub/spaces
    // 1. Load test data
    final String spaceJson = loadFileOrFail("SpaceApi/TC0200_createSpace/create_space.json");
    final String expectedBodyPart = loadFileOrFail("SpaceApi/TC0200_createSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = getNakshaClient().post("hub/spaces", spaceJson, streamId);

    // 3. Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart);
  }

  @Test
  void tc0201_testCreateDuplicateSpace() throws Exception {
    // Test API : POST /hub/spaces
    // Given: registered space
    final String duplicatedSpace = loadFileOrFail("SpaceApi/TC0201_createDupSpace/create_space.json");
    final String expectedBodyPart = loadFileOrFail("SpaceApi/TC0201_createDupSpace/response.json");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces", duplicatedSpace, streamId);

    // When: registering space for the second time
    final HttpResponse<String> response = getNakshaClient().post("hub/spaces", duplicatedSpace, streamId);

    // 3. Perform assertions
    assertThat(response)
        .hasStatus(409)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart);
  }

  @Test
  void tc0220_testGetSpaceById() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}
    // Given: registered space
    final String space = loadFileOrFail("SpaceApi/TC0220_getSpaceById/create_space.json");
    final String expectedBodyPart = loadFileOrFail("SpaceApi/TC0220_getSpaceById/response.json");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces", space, streamId);

    // When: fetching registered space by id
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/tc_220_test_space", streamId);

    // 3. Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart);
  }

  @Test
  void tc0221_testGetSpaceByWrongId() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}
    // 1. Load test data
    final String streamId = UUID.randomUUID().toString();

    // 2. Perform REST API call
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/not-real-space", streamId);

    // 3. Perform assertions
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId);
  }

  @Test
  void tc0240_testGetSpaces() throws Exception {
    // Given: created spaces
    List<String> expectedSpaceIds = List.of("tc_240_space_1", "tc_240_space_2");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces", loadFileOrFail("SpaceApi/TC0240_getSpaces/create_space_1.json"), streamId);
    getNakshaClient().post("hub/spaces", loadFileOrFail("SpaceApi/TC0240_getSpaces/create_space_2.json"), streamId);

    // When: Fetching all spaces
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces", streamId);

    // Then: Expect all saved spaces are returned
    assertEquals(200, response.statusCode(), "ResCode mismatch");
    assertEquals(streamId, getHeader(response, HDR_STREAM_ID), "StreamId mismatch");
    List<XyzFeature> returnedXyzFeatures =
        parseJson(response.body(), XyzFeatureCollection.class).getFeatures();
    boolean allReturnedFeaturesAreSpaces =
        returnedXyzFeatures.stream().allMatch(feature -> Space.class.isAssignableFrom(feature.getClass()));
    Assertions.assertTrue(allReturnedFeaturesAreSpaces);
    List<String> spaceIds =
        returnedXyzFeatures.stream().map(XyzFeature::getId).toList();
    Assertions.assertTrue(spaceIds.containsAll(expectedSpaceIds));
  }

  @Test
  void tc0260_testUpdateSpace() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given: registered space
    final String createStorageJson = loadFileOrFail("SpaceApi/TC0260_updateSpace/create_space.json");
    final String updateStorageJson = loadFileOrFail("SpaceApi/TC0260_updateSpace/update_space.json");
    final String expectedRespBody = loadFileOrFail("SpaceApi/TC0260_updateSpace/response.json");
    final String streamId = UUID.randomUUID().toString();
    getNakshaClient().post("hub/spaces", createStorageJson, streamId);

    // When: updating existing space
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/tc_260_test_space", updateStorageJson, streamId);

    // Then: space got updated
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedRespBody);
  }

  @Test
  void tc0261_testUpdateNonexistentSpace() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given:
    final String updateSpaceJson = loadFileOrFail("SpaceApi/TC0261_updateNonexistentSpace/update_space.json");
    final String expectedErrorResponse = loadFileOrFail("SpaceApi/TC0261_updateNonexistentSpace/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/non-existent-space", updateSpaceJson, streamId);

    // Then:
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedErrorResponse);
  }

  @Test
  void tc0263_testUpdateSpaceWithWithMismatchingId() throws Exception {
    // Test API : PUT /hub/spaces/{spaceId}
    // Given:
    final String bodyWithDifferentSpaceId = loadFileOrFail("SpaceApi/TC0263_updateSpaceWithMismatchingId/update_space.json");
    final String expectedErrorResponse = loadFileOrFail("SpaceApi/TC0263_updateSpaceWithMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        getNakshaClient().put("hub/spaces/test-space", bodyWithDifferentSpaceId, streamId);

    // Then:
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedErrorResponse);
  }
}
