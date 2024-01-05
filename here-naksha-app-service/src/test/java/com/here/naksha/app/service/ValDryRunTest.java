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

import static com.here.naksha.app.common.CommonApiTestSetup.*;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static com.here.naksha.app.common.TestUtil.*;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ValDryRunTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "local-space-4-val-dry-run";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    createHandler(nakshaClient, "ValDryRun/setup/create_context_loader_handler.json");
    createHandler(nakshaClient, "ValDryRun/setup/create_validation_handler.json");
    createHandler(nakshaClient, "ValDryRun/setup/create_endorsement_handler.json");
    createHandler(nakshaClient, "ValDryRun/setup/create_echo_handler.json");
    createSpace(nakshaClient, "ValDryRun/setup/create_space.json");
  }

  @Test
  void tc3000_testValDryRunReturningViolations() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features returned with mock violations
    final String streamId = UUID.randomUUID().toString();

    // Given: PUT features request
    final String bodyJson = loadFileOrFail("ValDryRun/TC3000_WithViolations/upsert_features.json");
    final String expectedBodyPart = loadFileOrFail("ValDryRun/TC3000_WithViolations/feature_response_part.json");

    // When: Request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Validation dry-run response body doesn't match")
            .hasMatchingUpdatedCount(3)
            .hasUpdatedIdsMatchingFeatureIds(null)
            .hasFeatureReferencedByViolations(0, new int []{0})
            .hasFeatureReferencedByViolations(1, new int []{1,2})
            .hasFeatureReferencedByViolations(2, new int []{3,4,5})
    ;
  }

  @Test
  void tc3001_testValDryRunNoViolations() throws Exception {
    // Test API : POST /hub/spaces/{spaceId}/features
    // Validate features returned without any violations
    final String streamId = UUID.randomUUID().toString();

    // Given: PUT features request
    final String bodyJson = loadFileOrFail("ValDryRun/TC3001_WithoutViolations/upsert_features.json");
    final String expectedBodyPart = loadFileOrFail("ValDryRun/TC3001_WithoutViolations/feature_response_part.json");

    // When: Request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Validation dry-run response body doesn't match")
            .hasNoViolations()
    ;
  }

  @Test
  void tc3002_testValDryRunUnsupportedOperation() throws Exception {
    // Test API : DELETE /hub/spaces/{spaceId}/features
    // Validate request gets rejected as validation is not supported for DELETE endpoint
    final String streamId = UUID.randomUUID().toString();

    // Given: DELETE features request
    final String expectedBodyPart =
        loadFileOrFail("ValDryRun/TC3002_UnsupportedOperation/feature_response_part.json");

    // When: Request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response =
        nakshaClient.delete("hub/spaces/" + SPACE_ID + "/features?id=some-feature-id", streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(501)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Validation dry-run response body doesn't match");
  }

}
