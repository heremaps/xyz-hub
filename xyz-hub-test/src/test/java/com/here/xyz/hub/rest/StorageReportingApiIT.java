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

package com.here.xyz.hub.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageReportingApiIT extends TestSpaceWithFeature {
  private static Set<String> cleanUpIds = new HashSet<>();

  private static void cleanUp() {
    remove();
    cleanUpIds.forEach(TestWithSpaceCleanup::removeSpace);
  }

  @BeforeClass
  public static void setup() { cleanUp(); }

  @After
  public void tearDown() { cleanUp(); }

  @Test
  public void testEmptyResponsePositive() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L));
  }

  @Test
  public void testMissingPermission() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void testWithOneEmptySpacePositive() {
    createSpace();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes.x-psql-test.contentBytes.value", equalTo(8192));
  }

  @Test
  public void testWithOneFilledSpacePositive() {
    createSpace();
    addFeatures();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes.x-psql-test.contentBytes.value", greaterThan(3000));
  }

  @Test
  public void testWithMultipleStorageIds() {
    cleanUpIds.add(createSpaceWithCustomStorage("x-psql-test-1", "psql", null));
    cleanUpIds.add(createSpaceWithCustomStorage("x-psql-test-2", "psql_db2_hashed", null));
    addFeatures("x-psql-test-2");
    given()
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
            .when()
            .get("/admin/statistics/spaces/storage")
            .then()
            .statusCode(OK.code())
            .body("type", equalTo("StorageStatistics"))
            .body("createdAt", greaterThan(0L))
            .body("byteSizes.x-psql-test-1.contentBytes.value", equalTo(8192))
            .body("byteSizes.x-psql-test-1.storageId", equalTo("psql"))
            .body("byteSizes.x-psql-test-2.contentBytes.value", greaterThan(8192))
            .body("byteSizes.x-psql-test-2.storageId", equalTo("psql_db2_hashed"));
  }

}
