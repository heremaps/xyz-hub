/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.auth;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.CombinableMatcher.either;

import com.here.xyz.hub.rest.RestAssuredTest;
import com.here.xyz.hub.util.Compression;
import com.jayway.restassured.response.ValidatableResponse;
import java.util.Base64;
import java.util.zip.DataFormatException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthTestsIT extends RestAssuredTest {

  private static String cleanUpId;
  private static boolean zeroSpaces = true;

  @BeforeClass
  public static void setupClass() {
    removeAllSpaces();
    zeroSpaces = zeroSpaces();
  }

  @After
  public void tearDown() {
    removeAllSpaces();
  }

  public static void removeSpace(String id) {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete("/spaces/" + id);
  }

  public static void removeAllSpaces() {
    //NOTE: Not actually remove *all* spaces but the ones being relevant for this test-class (stay runnable in other envs)
    removeSpace("x-auth-test-space");
    removeSpace("x-auth-test-space-shared");

    if (cleanUpId != null
        && !"x-auth-test-space".equals(cleanUpId)
        && !"x-auth-test-space-shared".equals(cleanUpId)
    ) removeSpace(cleanUpId);

  }

  private static boolean zeroSpaces()
  {
    int nrCurrentSpaces =
     getSpacesList("*", AuthProfile.ACCESS_ALL)
      .statusCode(OK.code())
       .extract().path("$.size()");

    return nrCurrentSpaces == 0;
  }

  @SuppressWarnings("SameParameterValue")
  private static ValidatableResponse createSpaceWithFeatures(String spaceFile, String featuresFile, AuthProfile profile) {
    cleanUpId = createSpace(spaceFile, profile)
        .statusCode(OK.code()).extract().path("id");

    return given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(profile))
        .body(content(featuresFile))
        .when()
        .put("/spaces/" + cleanUpId + "/features")
        .then();
  }

  private static ValidatableResponse getSpace(String spaceId, AuthProfile profile) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .when()
        .get("/spaces/" + spaceId).then();
  }

  private static ValidatableResponse createSpace(String fileName, AuthProfile profile) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .body(content(fileName))
        .when()
        .post("/spaces")
        .then();
  }

  private static ValidatableResponse updateSpace(String spaceId, String body, AuthProfile profile) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .body(body)
        .when()
        .patch("/spaces/" + spaceId)
        .then();
  }

  private static ValidatableResponse deleteSpace(String spaceId, AuthProfile profile) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .when()
        .delete("/spaces/" + spaceId)
        .then();
  }

  @SuppressWarnings("SameParameterValue")
  private static ValidatableResponse updateSpaceStorage(String storageId, AuthProfile profile) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .body("{\"storage\": {\"id\": \"" + storageId + "\"}}")
        .when()
        .patch("/spaces/x-auth-test-space")
        .then();
  }

  private static ValidatableResponse getSpacesList(String owner, AuthProfile profile) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .when()
        .get("/spaces" + (owner != null ? "?owner=" + owner : ""))
        .then();
  }

  private static ValidatableResponse testSpaceListWithShared(String owner, AuthProfile profile) {
    createSpace("/xyz/hub/auth/createSharedSpace.json", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code());

    createSpace("/xyz/hub/auth/createDefaultSpace.json", AuthProfile.ACCESS_OWNER_2)
        .statusCode(OK.code());

    return getSpacesList(owner, profile);
  }

  private static ValidatableResponse testSpaceListWithShared(String owner) {
    return testSpaceListWithShared(owner, AuthProfile.ACCESS_OWNER_2);
  }

  @Test
  public void createDefaultSpaceNegative() {
    createSpace("/xyz/hub/auth/createDefaultSpace.json", AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_ONLY)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createDefaultSpacePositive() {
    createSpace("/xyz/hub/auth/createDefaultSpace.json", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("id", notNullValue());
  }

  @Test
  public void createPsqlSpaceNegative() {
    createSpace("/xyz/hub/auth/createPsqlSpace.json", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createPsqlSpacePositive() {
    createSpace("/xyz/hub/auth/createPsqlSpace.json", AuthProfile.STORAGE_AUTH_TEST_PSQL_ONLY)
        .statusCode(OK.code());
  }

  @Test
  public void createSpaceWithNoAccessNegative() {
    createSpace("/xyz/hub/auth/createDefaultSpace.json", AuthProfile.NO_ACCESS)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createSpaceWithCustomIdNegative1() {
    createSpace("/xyz/hub/auth/createDefaultSpace.json", AuthProfile.ACCESS_OWNER_3)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createSpaceWithCustomIdNegative2() {
    createSpace("/xyz/hub/auth/createCustomIdSpace.json", AuthProfile.ACCESS_OWNER_3)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createSpaceWithCustomIdPositive() {
    cleanUpId = createSpace("/xyz/hub/auth/createCustomIdSpace.json", AuthProfile.ACCESS_OWNER_3_WITH_CUSTOM_SPACE_IDS)
        .statusCode(OK.code())
        .extract()
        .path("id");
  }
  @Test
  public void createTestStorageSpaceNegative() {
    createSpace("/xyz/hub/auth/createTestStorageSpace.json", AuthProfile.STORAGE_AUTH_TEST_PSQL_ONLY)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createTestStorageSpacePositive() {
    createSpace("/xyz/hub/auth/createTestStorageSpace.json", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(OK.code())
        .body("owner", equalTo(AuthProfile.STORAGE_AUTH_TEST_C1_ONLY.payload.aid))
        .body("storage", notNullValue())
        .body("storage.id", equalTo("c1"));

    getSpace("x-auth-test-space", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(OK.code())
        .body("owner", equalTo(AuthProfile.STORAGE_AUTH_TEST_C1_ONLY.payload.aid))
        .body("storage", notNullValue())
        .body("storage.id", equalTo("c1"));
  }

  @Test
  public void updateSpaceStorageNegative() {
    createSpace("/xyz/hub/auth/createTestStorageSpace.json", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(OK.code());

    updateSpaceStorage("psql", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void updateSpaceStoragePositive() {
    createSpace("/xyz/hub/auth/createTestStorageSpace.json", AuthProfile.STORAGE_AUTH_TEST_C1_ONLY)
        .statusCode(OK.code());

    updateSpaceStorage("psql", AuthProfile.STORAGE_AUTH_TEST_PSQL_ONLY)
        .statusCode(OK.code());
  }

  @Test
  public void createSpaceWithListenerPositive() {
    createSpace("/xyz/hub/auth/createSpaceWithListener.json", AuthProfile.CONNECTOR_AUTH_TEST_C1_AND_C2)
        .statusCode(OK.code());
  }

  @Test
  public void testCreateSharedSpace() {
    createSpace("/xyz/hub/auth/createSharedSpace.json", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code());

    getSpace("x-auth-test-space-shared", AuthProfile.ACCESS_OWNER_1_NO_ADMIN)
        .body("storage", equalTo(null));
  }

  @Test
  public void testCreateSharedSpaceAdminRead() {
    createSpace("/xyz/hub/auth/createSharedSpace.json", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code());

    getSpace("x-auth-test-space-shared", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .body("storage.id", equalTo("psql"));
  }

  @Test
  public void testSpaceListWithSharedOwnerDefault() {
    testSpaceListWithShared(null)
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("[0].id", equalTo("x-auth-test-space"));
  }

  @Test
  public void testSpaceListWithSharedOwnerMe() {
    testSpaceListWithShared("me")
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("[0].id", equalTo("x-auth-test-space"));
  }

  @Test
  public void testSpaceListWithSharedOwnerOthers() {
    testSpaceListWithShared("others")
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("[0].id", equalTo("x-auth-test-space-shared"));
  }

  @Test
  public void testSpaceListWithSharedOwnerOthersNegative() {
    testSpaceListWithShared("others", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("$.size()", equalTo(0));
  }

  @Test
  public void testSpaceListWithSharedOwnerAll() {
    testSpaceListWithShared("*")
        .statusCode(OK.code())
        .body("$.size()", equalTo(2))
        .body("id", hasItems("x-auth-test-space", "x-auth-test-space-shared"));
  }

  @Test
  public void testSpaceListWithSharedOwnerAllNegative() {
    testSpaceListWithShared("*", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("id", hasItems("x-auth-test-space-shared"));
  }

  @Test
  public void testSpaceListWithSharedOwner1() {
    testSpaceListWithShared("XYZ-01234567-89ab-cdef-0123-456789aUSER1")
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("[0].id", equalTo("x-auth-test-space-shared"));
  }

  @Test
  public void testSpaceListWithSharedOwner2() {
    testSpaceListWithShared("XYZ-01234567-89ab-cdef-0123-456789aUSER2")
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("[0].id", equalTo("x-auth-test-space"));
  }

  @Test
  public void testSpaceListWithSharedOwner2Negative() {
    testSpaceListWithShared("XYZ-01234567-89ab-cdef-0123-456789aUSER2", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("$.size()", equalTo(0));
  }

  @Test
  public void testSpaceListWithAccessAll() {
    testSpaceListWithShared("*", AuthProfile.ACCESS_ALL)
        .statusCode(OK.code())
        .body("$.size()", zeroSpaces ? equalTo(2) : greaterThanOrEqualTo(2) )
        .body("id", hasItems("x-auth-test-space", "x-auth-test-space-shared"));
  }

  @Test
  public void testCreateSpaceWithPackage() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2)
        .statusCode(FORBIDDEN.code())
        .extract()
        .path("id");

    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code())
        .extract()
        .path("id");

    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(FORBIDDEN.code())
        .extract()
        .path("id");

    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");
  }

  @Test
  public void testDeleteSpaceWithPackage() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_NO_ADMIN)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_READ_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(FORBIDDEN.code());

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_2)
        .statusCode(OK.code());
  }


  @Test
  public void testDeleteSpaceWithManageSpacesPackage() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    deleteSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_SPACES_PACKAGE_HERE)
        .statusCode(OK.code());
  }

  @Test
  public void testModifyOwner1SpaceAddPackages() {
    cleanUpId = createSpace("/xyz/hub/createSpaceWithoutId.json", AuthProfile.ACCESS_OWNER_1_NO_ADMIN)
        .statusCode(OK.code())
        .extract()
        .path("id");

    updateSpace(cleanUpId, "{\"packages\": [\"OSM\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(OK.code());
  }

  @Test
  public void testModifyOwner2SpaceAddPackages() {
    cleanUpId = createSpace("/xyz/hub/createSpaceWithoutId.json", AuthProfile.ACCESS_OWNER_2)
        .statusCode(OK.code())
        .extract()
        .path("id");

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\", \"OSM\"]}", AuthProfile.ACCESS_OWNER_2)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": [\"OSM\"]}", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\"]}", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    updateSpace(cleanUpId, "{\"packages\": [\"OSM\"]}", AuthProfile.ACCESS_OWNER_1_WITH_MS_PACKAGE_HERE_AND_MP_OSM)
        .statusCode(OK.code());
  }

  @Test
  public void testModifySpaceRemovePackages() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    updateSpace(cleanUpId, "{\"packages\": [\"HERE\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": [\"OSM\"]}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(OK.code());

    updateSpace(cleanUpId, "{\"packages\": []}", AuthProfile.ACCESS_OWNER_1_MANAGE_PACKAGES_HERE)
        .statusCode(FORBIDDEN.code());

    updateSpace(cleanUpId, "{\"packages\": []}", AuthProfile.ACCESS_OWNER_2)
        .statusCode(OK.code());
  }

  @Test
  public void testAddFeatureWithPackage() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));
  }

  @Test
  public void testListSpaceWithPackage() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    getSpacesList("*", AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("$.size()", equalTo(0));

    getSpacesList("*", AuthProfile.ACCESS_OWNER_1_READ_PACKAGES_HERE)
        .statusCode(OK.code())
        .body("$.size()", equalTo(1));
  }

  @Test
  public void testGetSpaceWithPackageForbidden() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    getSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void testGetSpaceWithPackageAccess() {
    cleanUpId = createSpace("/xyz/hub/auth/createSpaceWithPackage.json", AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    getSpace(cleanUpId, AuthProfile.ACCESS_OWNER_1_READ_PACKAGES_HERE)
        .statusCode(OK.code())
        .body("owner", equalTo(AuthProfile.ACCESS_OWNER_2.payload.aid))
        .body("packages", hasItems("HERE", "OSM"));
  }

  @Test
  public void testListFeaturesWithPackageAccess() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code())
        .extract()
        .path("id");

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_PACKAGES_HERE))
        .when()
        .get("/spaces/" + cleanUpId + "/iterate?limit=100")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(100));
  }

  @Test
  public void testGetFeatureWithPackageAccess() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + cleanUpId + "/features/Q2838923")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_PACKAGES_HERE))
        .when()
        .get("/spaces/"  + cleanUpId +"/features/Q2838923")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("Q2838923"))
        .body("properties.name", equalTo("Estadio Universidad San Marcos"));
  }

  @Test
  public void testDeleteFeatureWithPackageAccess() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/" + cleanUpId + "/features/Q2838923")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_WRITE_PACKAGES_HERE))
        .when()
        .delete("/spaces/"  + cleanUpId +"/features/Q2838923")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void testUpdateFeatureWithPackageAccess() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/updateFeature.json"))
        .when()
        .put("/spaces/" + cleanUpId + "/features/Q2838923?addTags=baseball&removeTags=soccer")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_WRITE_PACKAGES_HERE))
        .body(content("/xyz/hub/updateFeature.json"))
        .when()
        .put("/spaces/" + cleanUpId + "/features/Q2838923?addTags=baseball&removeTags=soccer")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("Q2838923"))
        .body("properties.name", equalTo("Estadio Universidad San Marcos Updated"))
        .body("properties.occupant", equalTo("National University of San Marcos Updated"))
        .body("properties.sport", equalTo("association baseball"))
        .body("properties.capacity", equalTo(67470))
        .body("properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void testCreateFeatureWithPackageAccess() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createSpaceWithPackage.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM)
        .statusCode(OK.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/featureWithNumberId.json"))
        .when()
        .post("/spaces/" + cleanUpId + "/features")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_WRITE_PACKAGES_HERE))
        .body(content("/xyz/hub/featureWithNumberId.json"))
        .when()
        .post("/spaces/" + cleanUpId + "/features")
        .then()
        .statusCode(OK.code())
        .body("features[0].id", equalTo("1234"));
  }

  @Test
  public void testCreateSpaceWithSearchablePropertiesNoAdmin() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body(content("/xyz/hub/createSpaceWithSearchableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(FORBIDDEN.code());
  }

  @Test
  public void testCreateSpaceWithSearchablePropertiesAdmin() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithSearchableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(FORBIDDEN.code());
  }

  @Test
  public void testCreateSpaceWithSearchableProperties() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES_AND_ADMIN))
        .body(content("/xyz/hub/createSpaceWithSearchableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response
        .statusCode(OK.code())
        .body("searchableProperties.name", equalTo(true))
        .body("searchableProperties.other", equalTo(false));
  }

  @Test
  public void testCreateSpaceWithSortablePropertiesAdmin() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithSortableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(FORBIDDEN.code());
  }

  @Test
  public void testCreateSpaceWithSortableProperties() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES_AND_ADMIN))
        .body(content("/xyz/hub/createSpaceWithSortableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response
        .statusCode(OK.code())
        .body("sortableProperties[0][0]", equalTo("name"))
        .body("sortableProperties[1][0]", equalTo("other"));
  }


  @Test
  public void testListFeaturesByBBoxWithClusteringNegative() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createDefaultSpace.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_ONLY))
        .when()
        .get("/spaces/x-auth-test-space/bbox?west=179&north=89&east=-179&south=-89&clustering=hexbin")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES))
        .when()
        .get("/spaces/x-auth-test-space/bbox?west=179&north=89&east=-179&south=-89&clustering=abc123")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testListFeaturesByBBoxWithClusteringPositive() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createDefaultSpace.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES))
        .when()
        .get("/spaces/x-auth-test-space/bbox?west=179&north=89&east=-179&south=-89&clustering=hexbin")
        .then()
        .statusCode(either(is(200)).or(is(502)));
  }

  @Test
  public void testListFeaturesByTileIdWithClusteringNegative() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createDefaultSpace.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_ONLY))
        .when()
        .get("/spaces/x-auth-test-space/tile/quadkey/120?clustering=hexbin")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES))
        .when()
        .get("/spaces/x-auth-test-space/tile/quadkey/120?clustering=abc123")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testListFeaturesByTileIdWithClusteringPositive() {
    createSpaceWithFeatures(
        "/xyz/hub/auth/createDefaultSpace.json",
        "/xyz/hub/processedData.json",
        AuthProfile.ACCESS_OWNER_1_ADMIN)
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_USE_CAPABILITIES))
        .when()
        .get("/spaces/x-auth-test-space/tile/quadkey/120?clustering=hexbin")
        .then()
        .statusCode(either(is(200)).or(is(502)));
  }

  @Test
  public void createSpaceWithCompressedJWT() throws DataFormatException {
    final String compressedToken = Base64.getEncoder().encodeToString(Compression.compressUsingInflate(AuthProfile.ACCESS_OWNER_1_ADMIN.jwt_string.getBytes()));

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers("Authorization", "Bearer " + compressedToken)
        .body(content("/xyz/hub/auth/createDefaultSpace.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .body("id", notNullValue());
  }

  @Test
  public void createSpaceWithCompressedJWTOnAccessTokenQueryParam() throws DataFormatException {
    final String compressedToken = Base64.getEncoder().encodeToString(Compression.compressUsingInflate(AuthProfile.ACCESS_OWNER_1_ADMIN.jwt_string.getBytes()));

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .body(content("/xyz/hub/auth/createDefaultSpace.json"))
        .when()
        .post("/spaces?access_token=" + compressedToken)
        .then()
        .statusCode(OK.code())
        .body("id", notNullValue());
  }
}
