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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

@WireMockTest(httpPort = 9092)
class ReadFeaturesByTileHttpStorageTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_tile_http_test_space";
  private static final String TYPE_QUADKEY = "quadkey";

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByTileHttpStorage/setup");
  }

  private static Stream<Arguments> standardTestParams() {
    return Stream.of(
            standardTestSpec(
                    // for given Tile and limit
                    "tc0806_testGetByTileWithLimit",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "limit=2"
                    ),
                    "ReadFeatures/ByTileHttpStorage/TC0806_WithLimit/feature_response_part.json",
                    200,
                    false,
                    true
            ),
            standardTestSpec(
                    // for empty Tile Id
                    "tc0809_testGetByTileWithoutTile",
                    TYPE_QUADKEY,
                    "",
                    null,
                    "ReadFeatures/ByTileHttpStorage/TC0809_WithoutTile/feature_response_part.json",
                    400,
                    false,
                    false
            ),
            standardTestSpec(
                    // for invalid Tile Id
                    "tc0810_testGetByTileWithInvalidTileId",
                    TYPE_QUADKEY,
                    "A",
                    null,
                    "ReadFeatures/ByTileHttpStorage/TC0810_InvalidTileId/feature_response_part.json",
                    400,
                    false,
                    false
            ),
            standardTestSpec(
                    // for given Tile condition and margin
                    "tc0817_testGetByTileWithMargin",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=20"
                    ),
                    "ReadFeatures/ByTileHttpStorage/TC0817_TileWithMargin/feature_response_part.json",
                    200,
                    false,
                    true
            ),
            standardTestSpec(
                    // for supported Tile Id but Margin value is invalid
                    "tc0818_testGetByTileWithInvalidMargin",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=-1"
                    ),
                    "ReadFeatures/ByTileHttpStorage/TC0818_InvalidMargin/feature_response_part.json",
                    400,
                    false,
                    false
            ),
            standardTestSpec(
                    // for supported Tile Id but Margin value is invalid
                    "tc0900_testGetByTileWithNonQuadkeyTileType",
                    "not_supported_file_type",
                    "120203302030322200",
                    null,
                    "ReadFeatures/ByTileHttpStorage/TC0818_InvalidMargin/feature_response_part.json",
                    400, // Http storage returns 501 but at Hub level validation is preformed an 400 thrown
                    false,
                    false
            )
    );
  }

  private static Arguments standardTestSpec(final String testDesc,
                                            final @NotNull String tileType,
                                            final @NotNull String tileId,
                                            final @Nullable List<String> queryParamList,
                                            final @NotNull String fPathOfExpectedResBody,
                                            final int expectedResCode,
                                            final boolean strictChecking,
                                            final boolean shouldReachEndpoint) {
    return Arguments.arguments(tileType, tileId, queryParamList, fPathOfExpectedResBody, expectedResCode, Named.named(testDesc, strictChecking), shouldReachEndpoint);
  }

  @ParameterizedTest
  @MethodSource("standardTestParams")
  void standardTestExecution(
          final @NotNull String tileType,
          final @NotNull String tileId,
          final @Nullable List<String> queryParamList,
          final @NotNull String fPathOfExpectedResBody,
          final int expectedResCode,
          final boolean strictChecking,
          final boolean shouldReachEndpoint) throws Exception {
    // Given: Request parameters
    String urlQueryParams = "";
    if (queryParamList != null && !queryParamList.isEmpty()) {
      urlQueryParams += String.join("&", queryParamList);
    }
    final String streamId = UUID.randomUUID().toString();

    // Given: Http endpoint
    final UrlPathPattern urlPathPattern = urlPathEqualTo("/my_env/my_storage/my_feat_type/quadkey/" + tileId);
    final MappingBuilder mappingBuilder = get(urlPathPattern);
    withQueryParams(mappingBuilder, queryParamList);

    // Given: Expected response body
    final String loadedString = loadFileOrFail(fPathOfExpectedResBody);
    final String expectedBodyPart = (strictChecking) ? loadedString.replaceAll("\\{\\{streamId}}", streamId) : loadedString;
    if (shouldReachEndpoint) {
      stubFor(mappingBuilder.willReturn(jsonResponse(expectedBodyPart, expectedResCode)));
    }

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/tile/" + tileType + "/" + tileId + "?" + urlQueryParams, streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(expectedResCode)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Response body doesn't match", strictChecking);

    // Then: Verify request reached/not reached endpoint
    verify(shouldReachEndpoint ? 1 : 0, getRequestedFor(urlPathPattern));
  }

  /**
   * Handles only key=1&key=2 format, not key=1,2 format
   */
  private void withQueryParams(MappingBuilder mappingBuilder, List<String> queryParamList) {
    if (queryParamList != null) queryParamList.forEach(
            str -> {
              String[] split = str.split("=");
              mappingBuilder.withQueryParam(split[0], equalTo(split[1]));
            });
  }

}
