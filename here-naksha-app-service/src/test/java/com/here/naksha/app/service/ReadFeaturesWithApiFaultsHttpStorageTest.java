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

import com.github.tomakehurst.wiremock.common.Gzip;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Api communication faults in {@link com.here.naksha.storage.http.HttpStorage}.
 * API used: GET /hub/spaces/{spaceId}/features/{featureId}
 */
@WireMockTest(httpPort = 9093)
class ReadFeaturesWithApiFaultsHttpStorageTest extends ApiTest {

  public static final String CORRECT_ENDPOINT_RESPONSE = loadFileOrFail("ReadFeatures/WithApiFaultsHttpStorage/correct_response.json");
  public static final int EXCEED_TIMEOUT_MILLIS = 1010;
  public static final int INTERNAL_ERROR = 500;
  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
  private static final String SPACE_ID = "read_features_api_faults_http_test_space";
  private static final String ENDPOINT = "/my_env/my_storage/my_feat_type/features";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/WithApiFaultsHttpStorage/setup");
  }

  @Test
  void tc01_testTimeout() throws Exception {
    // Validate behavior on response exceeding timeout
    String streamId = UUID.randomUUID().toString();

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/1");
    stubFor(get(endpointPath).willReturn(okJson(CORRECT_ENDPOINT_RESPONSE).withFixedDelay(EXCEED_TIMEOUT_MILLIS)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/1", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasStreamIdHeader(streamId);

    assertTrue(response.body().contains("Timeout"));

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc02_testTimeoutWithChunkedResponse() throws Exception {
    // Validate behavior on response split to chunks. First chunk arrives on time but the whole response exceeds timeout.
    String streamId = UUID.randomUUID().toString();
    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/2");
    stubFor(get(endpointPath).willReturn(okJson(CORRECT_ENDPOINT_RESPONSE).withChunkedDribbleDelay(5, EXCEED_TIMEOUT_MILLIS)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/2", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasStreamIdHeader(streamId);

    assertTrue(response.body().contains("Timeout"));

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc03_testMalformedResponseChunk() throws Exception {
    // Validate behavior on scenario when server send an OK status header, then garbage, then close the connection.
    String streamId = UUID.randomUUID().toString();

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/3");
    stubFor(get(endpointPath).willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/3", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasStreamIdHeader(streamId);

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc04_testConnectionReset() throws Exception {
    // Validate behavior on connection reset
    String streamId = UUID.randomUUID().toString();

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + "/4");
    stubFor(get(endpointPath).willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features/4", streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasStreamIdHeader(streamId);

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @ParameterizedTest
  @ValueSource(
          ints = {
                  HttpURLConnection.HTTP_UNAUTHORIZED,
                  HttpURLConnection.HTTP_FORBIDDEN,
                  429,
                  HttpURLConnection.HTTP_NOT_IMPLEMENTED}
  )
  void tc05_testErrorCodes(int errorCode) throws Exception {
    // Validate behavior on connection reset
    String streamId = UUID.randomUUID().toString();
    String featureId = "/tc05_" + errorCode;

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + featureId);
    stubFor(get(endpointPath).willReturn(aResponse().withStatus(errorCode)));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(errorCode)
            .hasStreamIdHeader(streamId);


    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc06_testMalformedGzip() throws Exception {
    String streamId = UUID.randomUUID().toString();
    String featureId = "/tc06";

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + featureId);
    stubFor(get(endpointPath).willReturn(
            ok("not_a_gzip").withHeader("Content-Encoding", "gzip")
    ));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasJsonBody("""
                    {
                        "type": "ErrorResponse",
                        "error": "Exception",
                        "errorMessage": "java.util.zip.ZipException: Not in GZIP format",
                        "streamId": "%s"
                    }
                    """.formatted(streamId)
            )
            .hasStreamIdHeader(streamId);


    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc07_testNotSupportedEncoding() throws Exception {
    String streamId = UUID.randomUUID().toString();
    String featureId = "/tc07";

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + featureId);
    stubFor(get(endpointPath).willReturn(
            ok("encoded_with_not_supported_encoding")
                    .withHeader("Content-Encoding", "not_supported_encoding")
    ));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + featureId, streamId);

    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasJsonBody("""
                    {
                        "type": "ErrorResponse",
                        "error": "Exception",
                        "errorMessage": "Encoding not_supported_encoding not recognized",
                        "streamId": "%s"
                    }
                    """.formatted(streamId))
            .hasStreamIdHeader(streamId);


    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc08_testMultipleEncodings() throws Exception {
    String streamId = UUID.randomUUID().toString();
    String featureId = "/tc08";

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + featureId);
    stubFor(get(endpointPath).willReturn(
            ok().withBody(Gzip.gzip("""
                            {
                              "id": "tc08",
                              "type": "Feature"
                            }
                            """))
                    .withHeader("Content-Encoding", "not_gzip")
                    .withHeader("Content-Encoding", "gzip")
    ));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + featureId, streamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasJsonBody("""
                    {
                        "type": "ErrorResponse",
                        "error": "Exception",
                        "errorMessage": "There are more than one Content-Encoding value in response",
                        "streamId": "%s"
                    }
                    """.formatted(streamId))
            .hasStreamIdHeader(streamId);

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }

  @Test
  void tc09_testGzipEncodedWithoutHeader() throws Exception {
    String streamId = UUID.randomUUID().toString();
    String featureId = "/tc09";

    UrlPattern endpointPath = urlPathEqualTo(ENDPOINT + featureId);
    stubFor(get(endpointPath).willReturn(
            ok().withBody(Gzip.gzip("""
                    {
                      "id": "tc09",
                      "type": "Feature"
                    }
                    """))
    ));

    // When: Get Features request is submitted to NakshaHub Space Storage instance
    HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features" + featureId, streamId);
    // Then: Perform assertions
    ResponseAssertions.assertThat(response)
            .hasStatus(INTERNAL_ERROR)
            .hasStreamIdHeader(streamId);

    // Then: Verify request reached endpoint once
    verify(1, getRequestedFor(endpointPath));
  }
}
