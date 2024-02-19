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

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;
import static com.here.naksha.app.common.CommonApiTestSetup.*;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

class FeatureViolationSegregationTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String COMMON_SPACE = "common-violations";
  private static final String TOPOLOGY_SPACE = "topology-violations";
  private static final String SIGN_SPACE = "sign-violations";

  /*
  For this test suite, we perform following setup:
  1. Create new storage definition
  2. Create 3 handlers:
    a. common-violations-handler using DefaultStorageHandler and above storage
    b. topology-violations-handler using TagFilterHandler to apply tag filtering/manipulation on "violated_ftype_topology"
    c. sign-violations-handler using TagFilterHandler to apply tag filtering/manipulation on "violated_ftype_sign"
  3. Create 3 spaces:
    a. common-violations using:
      - common-violations-handler
      - and collection as "violations"
    b. topology-violations using:
      - topology-violations-handler
      - common-violations-handler
      - and (importantly) same collection as "violations"
    c. sign-violations using:
      - sign-violations-handler
      - common-violations-handler
      - and (importantly) same collection as "violations"
  4. Insert both topology and sign specific violation features into common-violations space
  5. Then validate Tag filtering and manipulation using various Read/Write test cases.
  */
  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    createStorage(nakshaClient, "FeatureViolationSegregation/setup/create_storage.json");
    createHandler(nakshaClient, "FeatureViolationSegregation/setup/handler_common_violations.json");
    createHandler(nakshaClient, "FeatureViolationSegregation/setup/handler_topology_violations.json");
    createHandler(nakshaClient, "FeatureViolationSegregation/setup/handler_sign_violations.json");
    createSpace(nakshaClient, "FeatureViolationSegregation/setup/space_common_violations.json");
    createSpace(nakshaClient, "FeatureViolationSegregation/setup/space_topology_violations.json");
    createSpace(nakshaClient, "FeatureViolationSegregation/setup/space_sign_violations.json");
    final String initialFeaturesJson = loadFileOrFail("FeatureViolationSegregation/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + COMMON_SPACE + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  @Test
  void tc01_testBBoxOnTopologyViolations() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/bbox
    // Validate we get only Topology specific violations in the response

    // Given: Features By BBox request (against topology-violations space)
    final String bboxQueryParam = "west=8.6476&south=50.1175&east=8.6729&north=50.1248";
    final String expectedBodyPart =
        loadFileOrFail("FeatureViolationSegregation/TC01_BBoxOnTopologyViolations/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By BBox request is submitted to NakshaHub
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + TOPOLOGY_SPACE + "/bbox?" + bboxQueryParam, streamId);

    // Then: Perform assertions
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc02_testGetByIdOnTopologyViolations() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/{featureId}
    // Validate we get Topology specific violation in the response

    // Given: Feature By Id request (against topology-violations space)
    final String featureId = "my-custom-id-01";
    final String expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC02_ByIdOnTopologyViolations/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By Id request is submitted to NakshaHub
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + TOPOLOGY_SPACE + "/features/" + featureId, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc03_testGetByIdForNonMatchingSignViolation() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate we don't return violation when Topology violation is requested from sign-violations space

    // Given: Feature By Id request (against sign-violations space)
    final String featureId = "my-custom-id-01";
    final String expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC03_IdNotMatchingSignViolation/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    // When: Get Features By Id request is submitted to NakshaHub
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SIGN_SPACE + "/features/" + featureId, streamId);

    // Then: Perform assertions
    assertThat(response)
            .hasStatus(404)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
  }

  @Test
  void tc04_testTagManipulation() throws Exception {
    /*
      Although as a combined steps, below scenario doesn't exactly simulate real usecase,
      we are still able to test the tag based space segregation using individual steps.
      1. Execute GetById (my-custom-id-03) for Sign Violation from topology-violations space (should return no record)
      2. Do the same from sign-violations space (should get one record)
      3. Execute Update of Sign violation on topology-violations space (which should replace tag, and make it a Topology violation)
      4. Do GetById again from topology-violations space (now we should get the updated record as Topology Violation)
      5. Do GetById from sign-violations space (should return no record, as sign violation got tagged as topology violation)
    */

    // Given: Feature Id belonging to Sign Violation
    final String featureId = "my-custom-id-03";
    final String streamId = UUID.randomUUID().toString();

    // When: GetById request is triggered on topology-violations space
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + TOPOLOGY_SPACE + "/features/" + featureId, streamId);
    // Then: Perform assertions (no record found)
    String expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC04_TagManipulation/no_feature_found.json");
    assertThat(response)
            .hasStatus(404)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // When: GetById request is triggered on sign-violations space
    response = getNakshaClient().get("hub/spaces/" + SIGN_SPACE + "/features/" + featureId, streamId);
    // Then: Perform assertions (that we got Sign Violation)
    expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC04_TagManipulation/orig_sign_violation.json");
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // When: Update Sign Violation is sent to topology-violations space
    response = getNakshaClient().put("hub/spaces/" + TOPOLOGY_SPACE + "/features/"+featureId, expectedBodyPart, streamId);
    // Then: Perform assertions (to validate that we got the tags updated)
    expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC04_TagManipulation/update_sign_violation_response.json");
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Update Feature response body doesn't match");

    // When: GetById request is again triggered on topology-violations space
    response = getNakshaClient().get("hub/spaces/" + TOPOLOGY_SPACE + "/features/" + featureId, streamId);
    // Then: Perform assertions (this time we get updated record as Topology Violation)
    expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC04_TagManipulation/updated_topology_violation.json");
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // When: GetById request is again triggered on sign-violations space
    response = getNakshaClient().get("hub/spaces/" + SIGN_SPACE + "/features/" + featureId, streamId);
    // Then: Perform assertions (this time we don't get record as Sign Violation got tagged as Topology Violation)
    expectedBodyPart =
            loadFileOrFail("FeatureViolationSegregation/TC04_TagManipulation/no_feature_found.json");
    assertThat(response)
            .hasStatus(404)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

  }

}
