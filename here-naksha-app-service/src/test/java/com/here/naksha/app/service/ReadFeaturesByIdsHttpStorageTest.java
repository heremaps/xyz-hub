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

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static com.here.naksha.app.service.testutil.GzipUtil.stubOkGzipEncoded;

/**
 * Tests for GET /hub/spaces/{spaceId}/features/{featureId} against {@link com.here.naksha.storage.http.HttpStorage}
 */
@WireMockTest(httpPort = 9091)
class ReadFeaturesByIdsHttpStorageTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String HTTP_SPACE_ID = "read_features_by_ids_http_test_space";
  private static final String PSQL_SPACE_ID = "read_features_by_ids_http_test_psql_space";
  private static final String VIEW_SPACE_ID = "read_features_by_ids_http_test_view_space";

  private static final String ENDPOINT = "/my_env/my_storage/my_feat_type/features";


  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    // Set up Http Storage based Space
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByIdsHttpStorage/setup/http_storage_space");
    // Set up (standard) Psql Storage based Space
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByIdsHttpStorage/setup/psql_storage_space");
    // Set up View Space over Psql and Http Storage based spaces
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByIdsHttpStorage/setup/view_space");
    // Load some test data in PsqlStorage based Space
    final String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByIdsHttpStorage/setup/psql_storage_space/create_features.json");
    final HttpResponse<String> response = nakshaClient.post("hub/spaces/" + PSQL_SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
    assertThat(response).hasStatus(200);
  }

  @Test
  void tc0400_testReadFeaturesByIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate features getting returned for existing Ids and not failing due to missing ids
    // Given: Features By Ids request (against above space)
    final String idsQueryParam =
            "id=my-custom-id-400-1" + "&id=my-custom-id-400-2" + "&id=missing-id-1" + "&id=missing-id-2";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0400_ExistingAndMissingIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("id", havingCommaSeparatedValue("my-custom-id-400-1"))
            .withQueryParam("id", havingCommaSeparatedValue("my-custom-id-400-2"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + HTTP_SPACE_ID + "/features?" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0401_testReadFeaturesForMissingIds() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features
    // Validate empty collection getting returned for missing ids
    // Given: Features By Ids request (against configured space)
    final String idsQueryParam = "?id=1000" + "&id=missing-id-1" + "&id=missing-id-2";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0401_MissingIds/feature_response_part.json");
    String streamId = UUID.randomUUID().toString();

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
    stubFor(get(endpointPath)
            .withQueryParam("id", havingCommaSeparatedValue("1000"))
            .willReturn(okJson(expectedBodyPart)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + HTTP_SPACE_ID + "/features" + idsQueryParam, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0404_testReadFeatureById() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate feature getting returned for given Id
    // Given: Feature By Id request (against already existing space)
    final String featureId = "1";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0404_ExistingId/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/1");
    stubFor(get(endpointPath).willReturn(okJson(expectedBodyPart)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + HTTP_SPACE_ID + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0405_testReadFeatureForMissingId() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate request gets failed when attempted to load feature for missing Id
    // Given: Feature By Id request, against existing space, for missing feature Id
    final String featureId = "missing-id";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0405_MissingId/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/" + featureId);
    stubFor(get(endpointPath).willReturn(notFound()));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + HTTP_SPACE_ID + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(404)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0406_testGetByIdOnViewWhenFeatureMissingInBase() throws Exception {
    // Test API : GET /hub/spaces/{spaceId}/features/{featureId}
    // Validate feature is retrieved from top layer (Psql Storage) of a View,
    // when Http Storage (base layer) has it missing for given featureId

    // Given: Feature Id which is missing in base layer
    // Given: Expected response body
    final String featureId = "my-custom-id-01";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0406_ViewWithMissingIdInHttpStorage/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // Given: Mock Base layer (Http Storage) response indicating error 404 (feature not found)
    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/" + featureId);
    stubFor(get(endpointPath).willReturn(notFound()));

    // When: Get Feature by Id request is submitted to View space of NakshaHub
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + VIEW_SPACE_ID + "/features/" + featureId, streamId);

    // Then: Validate that we successfully get a feature back from top layer (Psql Storage)
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc0500_testGzipEncodedResponse() throws URISyntaxException, IOException, InterruptedException {
    final String featureId = "tc500-id";
    final String expectedBodyPart =
            loadFileOrFail("ReadFeatures/ByIdsHttpStorage/TC0500_GzipEncodedResponse/feature_response_part.json");
    final String streamId = UUID.randomUUID().toString();

    final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/" + featureId);
    stubOkGzipEncoded(get(endpointPath), expectedBodyPart);

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    final HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + HTTP_SPACE_ID + "/features/" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  private StringValuePattern havingCommaSeparatedValue(String value) {
    return matching(".*(^|,)" + value + "(,|$).*");
  }
}
