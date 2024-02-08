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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

class ReadFeaturesByRadiusPostTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_radius_post_test_space";

  /*
  For this test suite, we upfront create various Features using different combination of Geometry, Tags and properties.
  And then in subsequent tests, we validate the REST API behaviour by providing different filter conditions

  Main Space:
    - feature 1 - Point1, Tag-1, Tag-Ref, Property-1
    - feature 2 - Point2-within-5m-of-Point1, Tag-2, Tag-Ref, Property-2
    - feature 3 - Point2-within-5m-of-Point1, Tag-3, Tag-Ref, Property-1, Property-2
    - feature 4 - Point3-outside-5m-of-Point1, all above tags, all above properties

  Test Cases:
    TC  1 - Point1, radius=0 (should return feature 1 only)
    TC  2 - Point1, radius=5m (should return features 1,2,3)
    TC  3 - Point1, radius=5m, Prop-2 (should return features 2,3)
    TC  4 - Point1, radius=5m, Tag-1 (should return feature 1 only)
    TC  5 - Point1, radius=5m, Prop-1, Tag-3 (should return feature 3 only)
    TC  6 - Point1, radius=5m, Limit-2 (should return features 1,2)
    TC  7 - Point4-outside-100m-of-Point1, radius=5m (should return no features)
    TC  8 - No Geometry (should return 400)
    TC  9 - Point1, radius=-1m (should return 400)
    TC 10 - LineString, radius=5 (should return features 1,2,3)
    TC 11 - Point1, radius=5m, Prop-2 (should return features 2,3 with appropriate selected json prop)
    TC 12 - Point1, radius=5m, Prop-2 (should return features 2,3 disregarding invalid prop selection)
    TC 13 - Invalid delimiter of prop selection (should return 400)

  */

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByRadiusPost/setup");
    // create features in main space
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByRadiusPost/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  private static Arguments standardTestSpec(final String testDesc,
                                            final @Nullable String fPathOfRequestBody,
                                            final @Nullable List<String> queryParamList,
                                            final @NotNull String fPathOfExpectedResBody,
                                            final int expectedResCode) {
    return Arguments.arguments(fPathOfRequestBody, queryParamList, fPathOfExpectedResBody, Named.named(testDesc, expectedResCode));
  }

  private static Stream<Arguments> standardTestParams() {
    return Stream.of(
            standardTestSpec(
                    "tc01_testGetByRadiusPostWithPoint",
                    "ReadFeatures/ByRadiusPost/TC01_withPoint/request_body.json",
                    null,
                    "ReadFeatures/ByRadiusPost/TC01_withPoint/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc02_testGetByRadiusPostWithPointRadius",
                    "ReadFeatures/ByRadiusPost/TC02_withPointRadius/request_body.json",
                    List.of(
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadiusPost/TC02_withPointRadius/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc03_testGetByRadiusPostWithPointRadiusProp",
                    "ReadFeatures/ByRadiusPost/TC03_withPointRadiusProp/request_body.json",
                    List.of(
                            "radius=5",
                            "p.length=10"
                    ),
                    "ReadFeatures/ByRadiusPost/TC03_withPointRadiusProp/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc04_testGetByRadiusPostWithPointRadiusTag",
                    "ReadFeatures/ByRadiusPost/TC04_withPointRadiusTag/request_body.json",
                    List.of(
                            "radius=5",
                            "tags=tag-1"
                    ),
                    "ReadFeatures/ByRadiusPost/TC04_withPointRadiusTag/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc05_testGetByRadiusPostWithPointRadiusTagProp",
                    "ReadFeatures/ByRadiusPost/TC05_withPointRadiusTagProp/request_body.json",
                    List.of(
                            "radius=5",
                            "tags=tag-3",
                            "p.speedLimit='60'"
                    ),
                    "ReadFeatures/ByRadiusPost/TC05_withPointRadiusTagProp/feature_response_part.json",
                    200
            ),
            // NOTE - tc06 is implemented as separate test
            // to run custom assertion, as the response ordering is not guaranteed
            standardTestSpec(
                    "tc07_testGetByRadiusPostWithPointOutOfRadius",
                    "ReadFeatures/ByRadiusPost/TC07_withPointOutOfRadius/request_body.json",
                    List.of(
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadiusPost/TC07_withPointOutOfRadius/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc08_testGetByRadiusPostWithoutGeometry",
                    null,
                    null,
                    "ReadFeatures/ByRadiusPost/TC08_withoutGeometry/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc09_testGetByRadiusPostWithInvalidRadius",
                    "ReadFeatures/ByRadiusPost/TC09_withInvalidRadius/request_body.json",
                    List.of(
                            "radius=-1"
                    ),
                    "ReadFeatures/ByRadiusPost/TC09_withInvalidRadius/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc10_testGetByRadiusPostWithLineString",
                    "ReadFeatures/ByRadiusPost/TC10_withLineString/request_body.json",
                    List.of(
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadiusPost/TC10_withLineString/feature_response_part.json",
                    200
            ),
            // TC 11 and 12 are in another params set strictJsonTestParams()
            standardTestSpec(
                    "tc13_withInvalidDelimiterPropSelection",
                    "ReadFeatures/ByRadiusPost/TC13_withInvalidDelimiterPropSelection/request.json",
                    List.of(
                            "radius=5",
                            "p.length=10",
                            "selection=p.speedLimit+%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags")),
                            "clip=false"
                    ),
                    "ReadFeatures/ByRadiusPost/TC13_withInvalidDelimiterPropSelection/response.json",
                    400
            )
    );

  }

    private static Stream<Arguments> strictJsonTestParams() {
        return Stream.of(
                standardTestSpec(
                        "tc11_withPropSelection",
                        "ReadFeatures/ByRadiusPost/TC11_withPropSelection/request.json",
                        List.of(
                                "radius=5",
                                "p.length=10",
                                "selection=p.speedLimit,%s".formatted(urlEncoded("p.@ns:com:here:xyz.tags")),
                                "clip=false"
                        ),
                        "ReadFeatures/ByRadiusPost/TC11_withPropSelection/response.json",
                        200
                ),
                standardTestSpec(
                        "tc12_withInvalidPropSelection",
                        "ReadFeatures/ByRadiusPost/TC12_withInvalidPropSelection/request.json",
                        List.of(
                                "radius=5",
                                "p.length=10",
                                "selection=p.speedLimit,p.unknown_prop"
                        ),
                        "ReadFeatures/ByRadiusPost/TC12_withInvalidPropSelection/response.json",
                        200
                )
        );
    }

    void baseTestExecution(
            final @Nullable String fPathOfRequestBody,
            final @Nullable List<String> queryParamList,
            final @NotNull String fPathOfExpectedResBody,
            final int expectedResCode,
            boolean strictChecking) throws Exception {
        // Given: Request parameters
        String urlQueryParams = "";
        if (queryParamList!=null && !queryParamList.isEmpty()) {
            urlQueryParams += String.join("&", queryParamList);
        }
        final String streamId = UUID.randomUUID().toString();

        // Given: Request body
        final String requestBody = (fPathOfRequestBody!=null) ? loadFileOrFail(fPathOfRequestBody) : "";

        // Given: Expected response body
        final String loadedString = loadFileOrFail(fPathOfExpectedResBody);
        final String expectedBodyPart = (strictChecking) ? loadedString.replaceAll("\\{\\{streamId}}",streamId) : loadedString;
        // When: Get Features By Radius request is submitted to NakshaHub
        final HttpResponse<String> response = nakshaClient
                .post("hub/spaces/" + SPACE_ID + "/spatial?" + urlQueryParams, requestBody, streamId);

        // Then: Perform standard assertions
        assertThat(response)
                .hasStatus(expectedResCode)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Response body doesn't match", strictChecking);
    }

    @ParameterizedTest
    @MethodSource("strictJsonTestParams")
    void strictResponseTestExecution(
            final @Nullable String fPathOfRequestBody,
            final @Nullable List<String> queryParamList,
            final @NotNull String fPathOfExpectedResBody,
            final int expectedResCode) throws Exception {
        baseTestExecution(fPathOfRequestBody,queryParamList,fPathOfExpectedResBody,expectedResCode,true);
    }

    @ParameterizedTest
    @MethodSource("standardTestParams")
    void commonTestExecution(
            final @Nullable String fPathOfRequestBody,
            final @Nullable List<String> queryParamList,
            final @NotNull String fPathOfExpectedResBody,
            final int expectedResCode) throws Exception {
        baseTestExecution(fPathOfRequestBody,queryParamList,fPathOfExpectedResBody,expectedResCode,false);
    }

  @Test
  void tc06_testGetByRadiusPostWithPointRadiusLimit() throws Exception {
      // Given: Request parameters
      final List<String> queryParamList = List.of(
              "radius=5",
              "limit=2"
      );
      final String urlQueryParams = String.join("&", queryParamList);
      final String streamId = UUID.randomUUID().toString();

      // Given: Request body
      final String requestBody = loadFileOrFail("ReadFeatures/ByRadiusPost/TC06_withPointRadiusLimit/request_body.json");

      // When: Get Features By Radius request is submitted to NakshaHub
      final HttpResponse<String> response = nakshaClient
              .post("hub/spaces/" + SPACE_ID + "/spatial?" + urlQueryParams, requestBody, streamId);

      // Then: Perform custom assertions
      assertThat(response)
              .hasStatus(200)
              .hasStreamIdHeader(streamId)
              .hasFeatureCount(2)
              .hasFeatureIdsAmongst(List.of("my-custom-id-1","my-custom-id-2","my-custom-id-3"));
  }

}
