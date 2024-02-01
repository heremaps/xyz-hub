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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import static com.here.naksha.app.common.CommonApiTestSetup.createSpace;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

class ReadFeaturesByRadiusTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  private static final String SPACE_ID = "read_features_by_radius_test_space";
  private static final String REF_SPACE_ID = "read_features_by_radius_test_ref_space";

  /*
  For this test suite, we upfront create various Features using different combination of Geometry, Tags and properties.
  And then in subsequent tests, we validate the REST API behaviour by providing different filter conditions

  Main Space:
    - feature 1 - Point1, Tag-1, Tag-Ref, Property-1
    - feature 2 - Point2-within-5m-of-Point1, Tag-2, Tag-Ref, Property-2
    - feature 3 - Point2-within-5m-of-Point1, Tag-3, Tag-Ref, Property-1, Property-2
    - feature 4 - Point3-outside-5m-of-Point1, all above tags, all above properties

  Ref Space:
    - feature 1 - Point4-same-as-point1, Tag-Ref, Property-5
    - feature 2 - Point5-outside-100m-of-Point1
    - feature 3 - NoGeometry

  Test Cases:
    TC  1 - Point1, radius=0 (should return feature 1 only)
    TC  2 - Point1, radius=5m (should return features 1,2,3)
    TC  3 - Point1, radius=5m, Prop-2 (should return features 2,3)
    TC  4 - Point1, radius=5m, Tag-1 (should return feature 1 only)
    TC  5 - Point1, radius=5m, Prop-1, Tag-3 (should return feature 3 only)
    TC  6 - Point1, radius=5m, Limit-2 (should return features 1,2)
    TC  7 - RefSpace, RefFeature1, radius=5m, Tag-2 (should return feature 2 only)
    TC  8 - RefSpace, RefFeature1, radius=5m, Tag-3, Prop-1 (should return only feature 3)
    TC  9 - RefSpace, RefFeature2, radius=5m (should return no features)
    TC 10 - Point5-outside-100m-of-Point1, radius=5m (should return no features)
    TC 11 - Invalid RefSpace (should return 404)
    TC 12 - RefSpace, Invalid RefFeature4 (should return 404)
    TC 13 - Missing Lat/Lon (should return 400)
    TC 14 - Point1, radius=-1m (should return 400)
    TC 15 - Invalid Lat (should return 400)
    TC 16 - Invalid Lon (should return 400)
    TC 17 - RefSpace, RefFeature3 (missing geometry) (should return 404)
  */

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    setupSpaceAndRelatedResources(nakshaClient, "ReadFeatures/ByRadius/setup");
    // create another space that will be used for providing feature reference
    createSpace(nakshaClient, "ReadFeatures/ByRadius/setup/create_ref_space.json");
    // create features in main space
    String initialFeaturesJson = loadFileOrFail("ReadFeatures/ByRadius/setup/create_features.json");
    nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
    // create features in reference space
    initialFeaturesJson = loadFileOrFail("ReadFeatures/ByRadius/setup/create_ref_features.json");
    nakshaClient.post("hub/spaces/" + REF_SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
  }

  private static Arguments standardTestSpec(final String testDesc,
                                            final @Nullable List<String> queryParamList,
                                            final @NotNull String fPathOfExpectedResBody,
                                            final int expectedResCode) {
    return Arguments.arguments(queryParamList, fPathOfExpectedResBody, Named.named(testDesc, expectedResCode));
  }

  private static Stream<Arguments> standardTestParams() {
    return Stream.of(
            standardTestSpec(
                    "tc01_testGetByRadiusWithLatLon",
                    List.of(
                            "lon=8.6123&lat=50.1234"
                    ),
                    "ReadFeatures/ByRadius/TC01_withLatLon/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc02_testGetByRadiusWithLatLonRadius",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadius/TC02_withLatLonRadius/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc03_testGetByRadiusWithLatLonRadiusProp",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=5",
                            "p.length=10"
                    ),
                    "ReadFeatures/ByRadius/TC03_withLatLonRadiusProp/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc04_testGetByRadiusWithLatLonRadiusTag",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=5",
                            "tags=tag-1"
                    ),
                    "ReadFeatures/ByRadius/TC04_withLatLonRadiusTag/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc05_testGetByRadiusWithLatLonRadiusTagProp",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=5",
                            "tags=tag-3",
                            "p.speedLimit='60'"
                    ),
                    "ReadFeatures/ByRadius/TC05_withLatLonRadiusTagProp/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc06_testGetByRadiusWithLatLonRadiusLimit",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=5",
                            "limit=2"
                    ),
                    "ReadFeatures/ByRadius/TC06_withLatLonRadiusLimit/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc07_testGetByRadiusWithRefRadiusTag",
                    List.of(
                            "refSpaceId="+REF_SPACE_ID,
                            "refFeatureId=my-custom-ref-id-1",
                            "radius=5",
                            "tags=tag-2"
                    ),
                    "ReadFeatures/ByRadius/TC07_withRefRadiusTag/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc08_testGetByRadiusWithRefRadiusTagProp",
                    List.of(
                            "refSpaceId="+REF_SPACE_ID,
                            "refFeatureId=my-custom-ref-id-1",
                            "radius=5",
                            "tags=tag-3",
                            "p.speedLimit='60'"
                    ),
                    "ReadFeatures/ByRadius/TC08_withRefRadiusTagProp/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc09_testGetByRadiusWithRef3OutOfRadius",
                    List.of(
                            "refSpaceId="+REF_SPACE_ID,
                            "refFeatureId=my-custom-ref-id-2",
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadius/TC09_withRef3OutOfRadius/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc10_testGetByRadiusWithLatLonOutOfRadius",
                    List.of(
                            "lon=8.6133&lat=50.1234",
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadius/TC10_withLatLonOutOfRadius/feature_response_part.json",
                    200
            ),
            standardTestSpec(
                    "tc11_testGetByRadiusWithMissingRefSpace",
                    List.of(
                            "refSpaceId=missing-space-id",
                            "refFeatureId=my-custom-ref-id-1"
                    ),
                    "ReadFeatures/ByRadius/TC11_withMissingRefSpace/feature_response_part.json",
                    404
            ),
            standardTestSpec(
                    "tc12_testGetByRadiusWithMissingRefFeature",
                    List.of(
                            "refSpaceId="+REF_SPACE_ID,
                            "refFeatureId=missing-feature-id"
                    ),
                    "ReadFeatures/ByRadius/TC12_withMissingRefFeature/feature_response_part.json",
                    404
            ),
            standardTestSpec(
                    "tc13_testGetByRadiusWithoutLatLon",
                    null,
                    "ReadFeatures/ByRadius/TC13_withoutLatLon/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc14_testGetByRadiusWithInvalidRadius",
                    List.of(
                            "lon=8.6123&lat=50.1234",
                            "radius=-1"
                    ),
                    "ReadFeatures/ByRadius/TC14_withInvalidRadius/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc15_testGetByRadiusWithInvalidLat",
                    List.of(
                            "lon=8.6123&lat=-91",
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadius/TC15_withInvalidLat/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc16_testGetByRadiusWithInvalidLon",
                    List.of(
                            "lon=-181&lat=50.1234",
                            "radius=5"
                    ),
                    "ReadFeatures/ByRadius/TC16_withInvalidLon/feature_response_part.json",
                    400
            ),
            standardTestSpec(
                    "tc17_testGetByRadiusWithRefFeatureMissingGeometry",
                    List.of(
                            "refSpaceId="+REF_SPACE_ID,
                            "refFeatureId=my-custom-ref-id-3"
                    ),
                    "ReadFeatures/ByRadius/TC17_withRefFeatureMissingGeometry/feature_response_part.json",
                    404
            )
    );

  }

  @ParameterizedTest
  @MethodSource("standardTestParams")
  void commonTestExecution(
          final @Nullable List<String> queryParamList,
          final @NotNull String fPathOfExpectedResBody,
          final int expectedResCode) throws Exception {
    // Given: Request parameters
    String urlQueryParams = "";
    if (queryParamList!=null && !queryParamList.isEmpty()) {
      urlQueryParams += String.join("&", queryParamList);
    }
    final String streamId = UUID.randomUUID().toString();

    // Given: Expected response body
    final String expectedBodyPart = loadFileOrFail(fPathOfExpectedResBody);

    // When: Get Features By Radius request is submitted to NakshaHub
    final HttpResponse<String> response = nakshaClient
            .get("hub/spaces/" + SPACE_ID + "/spatial?" + urlQueryParams, streamId);

    // Then: Perform standard assertions
    assertThat(response)
            .hasStatus(expectedResCode)
            .hasStreamIdHeader(streamId)
            .hasJsonBody(expectedBodyPart, "Response body doesn't match");
  }

}
