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
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ReadFeaturesByTileTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_tile_test_space";
  private static final String TYPE_QUADKEY = "quadkey";

  /*
  For this test suite, we upfront create various Features using different combination of Tags and Geometry.
  To know what exact features we create, check the create_features.json.
  And then in subsequent tests, we validate the various GetByTile APIs using different query parameters.
  */
  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByTile/setup");
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByTile/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  private static Stream<Arguments> standardTestParams() {
    return Stream.of(
            standardTestSpec(
                    // for given Tile and given single tag value
                    "tc0800_testGetByTileWithSingleTag",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=one"
                    ),
                    "ReadFeatures/ByTile/TC0800_SingleTag/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and Tag OR condition
                    "tc0801_testGetByTileWithTagOrCondition",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=two,three"
                    ),
                    "ReadFeatures/ByTile/TC0801_TagOrCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and Tag AND condition
                    "tc0802_testGetByTileWithTagAndCondition",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=four+five"
                    ),
                    "ReadFeatures/ByTile/TC0802_TagAndCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and Tag OR condition using comma separated value
                    "tc0803_testGetByTileWithTagOrOrConditions",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=three",
                            "tags=four,five"
                    ),
                    "ReadFeatures/ByTile/TC0803_TagOrOrCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and combination of Tag OR and AND conditions
                    "tc0804_testGetByTileWithTagOrAndConditions",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=one",
                            "tags=two+three"
                    ),
                    "ReadFeatures/ByTile/TC0804_TagOrAndCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and combination of Tag AND, OR, AND conditions
                    "tc0805_testGetByTileWithTagAndOrAndConditions",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=three+four",
                            "tags=four+five"
                    ),
                    "ReadFeatures/ByTile/TC0805_TagAndOrAndCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile and limit
                    "tc0806_testGetByTileWithLimit",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=one",
                            "limit=2"
                    ),
                    "ReadFeatures/ByTile/TC0806_WithLimit/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile condition
                    "tc0807_testGetByTile",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    null,
                    "ReadFeatures/ByTile/TC0807_TileOnly/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile condition and Tag AND condition
                    "tc0808_testGetByTile2AndTagAndCondition",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "tags=three+four"
                    ),
                    "ReadFeatures/ByTile/TC0808_Tile2_TagAndCondition/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for empty Tile Id
                    "tc0809_testGetByTileWithoutTile",
                    TYPE_QUADKEY,
                    "",
                    null,
                    "ReadFeatures/ByTile/TC0809_WithoutTile/feature_response_part.json",
                    400,
                    false
            ),
            standardTestSpec(
                    // for invalid Tile Id
                    "tc0810_testGetByTileWithInvalidTileId",
                    TYPE_QUADKEY,
                    "A",
                    null,
                    "ReadFeatures/ByTile/TC0810_InvalidTileId/feature_response_part.json",
                    400,
                    false
            ),
            standardTestSpec(
                    // for Tile condition but invalid Tag delimiter is used
                    "tc0811_testGetByTileWithInvalidTagDelimiter",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=one@two"
                    ),
                    "ReadFeatures/ByTile/TC0811_InvalidTagDelimiter/feature_response_part.json",
                    400,
                    false
            ),
            standardTestSpec(
                    // for Tile condition and Tag combination having NonNormalized Tag value
                    "tc0812_testGetByTileWithNonNormalizedTag",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=non-matching-tag+" + urlEncoded("@ThRee")
                    ),
                    "ReadFeatures/ByTile/TC0812_NonNormalizedTag/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for Tile condition and Tag combination having mixed AND/OR conditions
                    "tc0813_testGetByTileWithMixedTagConditions",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=six,three+four",
                            "&tags=non-existing-tag"
                    ),
                    "ReadFeatures/ByTile/TC0813_MixedTagConditions/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile, but the given tags don't match features
                    "tc0814_testGetByTileWithTagMismatch",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=non-existing-tag"
                    ),
                    "ReadFeatures/ByTile/TC0814_NonMatchingTag/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tags, but the given Tile id doesn't match features
                    "tc0815_testGetByTileWithTileMismatch",
                    TYPE_QUADKEY,
                    "0",
                    List.of(
                            "tags=one"
                    ),
                    "ReadFeatures/ByTile/TC0815_NonMatchingTile/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for unsupported Tile Type, even though tileId and Tags are valid
                    "tc0816_testGetByTileWithUnsupportedTileType",
                    "here-quadkey",
                    "1",
                    List.of(
                            "tags=one"
                    ),
                    "ReadFeatures/ByTile/TC0816_UnsupportedTileType/feature_response_part.json",
                    400,
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
                    "ReadFeatures/ByTile/TC0817_TileWithMargin/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for supported Tile Id but Margin value is invalid
                    "tc0818_testGetByTileWithInvalidMargin",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=-1"
                    ),
                    "ReadFeatures/ByTile/TC0818_InvalidMargin/feature_response_part.json",
                    400,
                    false
            ),
            standardTestSpec(
                    // for given Tile condition and margin and property search condition
                    "tc0819_testGetByTileWithTagsWithPropSearch",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=0",
                            "tags=%s".formatted(urlEncoded("@ThRee")),
                            "p.speedLimit='70'"
                    ),
                    "ReadFeatures/ByTile/TC0819_TileWithTagsWithPropSearch/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for combined filters don't match any existing feature
                    "tc0820_testGetByTileWithNoFilterMatch",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "tags=one",
                            "p.speedLimit='90'"
                    ),
                    "ReadFeatures/ByTile/TC0820_TileWithNoFilterMatch/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given Tile condition and Prop search condition
                    "tc0821_testGetByTileWithOnlyPropMatch",
                    TYPE_QUADKEY,
                    "1",
                    List.of(
                            "p.speedLimit='80'"
                    ),
                    "ReadFeatures/ByTile/TC0821_TileWithOnlyPropMatch/feature_response_part.json",
                    200,
                    false
            ),
            standardTestSpec(
                    // for given filter parameters and selecting only few properties of Feature
                    "tc0822_testGetByTileWithPropSelection",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=0",
                            "tags=%s".formatted(urlEncoded("@ThRee")),
                            "p.speedLimit='70'",
                            "selection=p.speedLimit,%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags")),
                            "clip=false"
                    ),
                    "ReadFeatures/ByTile/TC0822_TileWithPropSelection/feature_response_part.json",
                    200,
                    true
            ),
            standardTestSpec(
                    // for given filter parameters and selecting unknown properties of Feature
                    "tc0823_testGetByTileWithUnknownPropSelection",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=0",
                            "tags=%s".formatted(urlEncoded("@ThRee")),
                            "p.speedLimit='70'",
                            "selection=p.unknown_prop",
                            "clip=false"
                    ),
                    "ReadFeatures/ByTile/TC0823_TileWithUnknownPropSelection/feature_response_part.json",
                    200,
                    true
            ),
            standardTestSpec(
                    // for given filter parameters and invalid delimiter in property selection
                    "tc0824_testGetByTileWithInvalidPropSelectionDelimiter",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=0",
                            "tags=%s".formatted(urlEncoded("@ThRee")),
                            "p.speedLimit='70'",
                            "selection=p.speedLimit+p.length",
                            "clip=false"
                    ),
                    "ReadFeatures/ByTile/TC0824_TileWithInvalidPropSelectionDelimiter/feature_response_part.json",
                    400,
                    false
            ),
            standardTestSpec(
                    // for given filter parameters and property selection and when clip is true
                    "tc0825_testGetByTileWithClip",
                    TYPE_QUADKEY,
                    "120203302030322200",
                    List.of(
                            "margin=0",
                            "tags=%s".formatted(urlEncoded("@ThRee")),
                            "p.speedLimit='70'",
                            "selection=p.speedLimit,%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags")),
                            "clip=true"
                    ),
                    "ReadFeatures/ByTile/TC0825_TileWithClip/feature_response_part.json",
                    200,
                    true
            )
    );
  }

  private static Arguments standardTestSpec(final String testDesc,
                                            final @NotNull String tileType,
                                            final @NotNull String tileId,
                                            final @Nullable List<String> queryParamList,
                                            final @NotNull String fPathOfExpectedResBody,
                                            final int expectedResCode,
                                            final boolean strictChecking) {
    return Arguments.arguments(tileType, tileId, queryParamList, fPathOfExpectedResBody, expectedResCode, Named.named(testDesc, strictChecking));
  }

  @ParameterizedTest
  @MethodSource("standardTestParams")
  void standardTestExecution(
          final @NotNull String tileType,
          final @NotNull String tileId,
          final @Nullable List<String> queryParamList,
          final @NotNull String fPathOfExpectedResBody,
          final int expectedResCode,
          final boolean strictChecking) throws Exception {
    // Given: Request parameters
    String urlQueryParams = "";
    if (queryParamList!=null && !queryParamList.isEmpty()) {
      urlQueryParams += String.join("&", queryParamList);
    }
    final String streamId = UUID.randomUUID().toString();

    // Given: Expected response body
    final String loadedString = loadFileOrFail(fPathOfExpectedResBody);
    final String expectedBodyPart = (strictChecking) ? loadedString.replaceAll("\\{\\{streamId}}",streamId) : loadedString;

    // When: Get Features By Tile request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/tile/"+ tileType +"/"+ tileId +"?" + urlQueryParams, streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(expectedResCode)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Response body doesn't match",strictChecking);
  }

}
