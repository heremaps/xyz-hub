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

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WriteFeaturesAtomicityTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
  private static final String SPACE_ID = "write_features_atomicity_test_space";

  public WriteFeaturesAtomicityTest() {
    super(nakshaClient);
  }

  @BeforeAll
  static void prepareEnv() {
    setupSpaceAndRelatedResources(nakshaClient, "WriteFeaturesAtomicity/setup");
  }

  @Test
  void tc_1101_duplicatedCreateShouldFail() throws URISyntaxException, IOException, InterruptedException {
    // Given: multiple features to save, some of which are invalid
    String createFeaturesJson = loadFileOrFail("WriteFeaturesAtomicity/TC1101_duplicatedCreateShouldFail/create_features.json");

    // When: saving these features
    String streamId = UUID.randomUUID().toString();
    HttpResponse<String> saveResp = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);

    // Then: response indicates conflict
    assertThat(saveResp)
        .hasStatus(409)
        .hasStreamIdHeader(streamId);

    // And: none of the features got saved
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + SPACE_ID + "/features?id=tc_1101_feature", streamId);
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBodyFromFile("WriteFeaturesAtomicity/TC1101_duplicatedCreateShouldFail/empty_response.json");
  }

  @Test
  void tc_1102_duplicatedUpdateShouldFail() throws URISyntaxException, IOException, InterruptedException {
    // Given: multiple features to save (all valid)
    String streamId = UUID.randomUUID().toString();
    List<String> featureIds = List.of("tc_1102_feature_1", "tc_1102_feature_2");
    String createFeatureJson = loadFileOrFail("WriteFeaturesAtomicity/TC1102_duplicatedUpdateShouldFail/create_features.json");
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);

    // When: updating these features and one of the updates is invalid (missing points coordinates)
    HttpResponse<String> updateResp = nakshaClient.put(
        "hub/spaces/" + SPACE_ID + "/features",
        loadFileOrFail("WriteFeaturesAtomicity/TC1102_duplicatedUpdateShouldFail/update_features.json"),
        streamId
    );

    // Then: response indicates failure
    assertThat(updateResp)
        .hasStatus(409)
        .hasStreamIdHeader(streamId);

    // And: none of the features got updated - they are equal to initial (creation) state
    String idsQuery = "?id=" + String.join("&id=", featureIds);
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + SPACE_ID + "/features" + idsQuery, streamId);
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBodyFromFile("WriteFeaturesAtomicity/TC1102_duplicatedUpdateShouldFail/get_response.json");
  }

  @Test
  void tc_1103_partialDeleteShouldSucceed() throws URISyntaxException, IOException, InterruptedException {
    // Given: multiple features to save (all valid)
    String streamId = UUID.randomUUID().toString();
    List<String> createdFeatureIds = List.of("tc_1103_feature_1", "tc_1103_feature_2");
    String createFeatureJson = loadFileOrFail("WriteFeaturesAtomicity/TC1103_partialDeleteShouldSucceed/create_features.json");
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);

    // When: deleting these features, including non-existing ones
    List<String> idsToDelete = List.of("tc_1103_feature_1", "tc_1103_feature_2", "non-existing-feature");
    String deleteIdsQuery = "?id=" + String.join("&id=", idsToDelete);
    HttpResponse<String> deleteResp = nakshaClient.delete("hub/spaces/" + SPACE_ID + "/features" + deleteIdsQuery, streamId);
    assertThat(deleteResp).hasStatus(200);

    // Then: response indicates success
    assertThat(deleteResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBodyFromFile("WriteFeaturesAtomicity/TC1103_partialDeleteShouldSucceed/delete_response.json");

    // And: existing features got deleted
    String getIdsQuery = "?id=" + String.join("&id=", createdFeatureIds);
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + SPACE_ID + "/features" + getIdsQuery, streamId);
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBodyFromFile("WriteFeaturesAtomicity/TC1103_partialDeleteShouldSucceed/empty_get_response.json");
  }
}
