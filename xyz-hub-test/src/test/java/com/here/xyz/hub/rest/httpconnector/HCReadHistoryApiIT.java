/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.rest.httpconnector;

import com.here.xyz.hub.rest.ReadHistoryApiIT;
import com.here.xyz.hub.rest.RestTests;
import com.jayway.restassured.response.ValidatableResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.HashMap;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_CHANGESET_COLLECTION;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

@Category(RestTests.class)
public class HCReadHistoryApiIT extends ReadHistoryApiIT {

  @BeforeClass
  public static void setupClass() {
    usedStorageId = httpStorageId;
    ReadHistoryApiIT.setupClass();
  }

  @AfterClass
  public static void tearDownClass() {
    usedStorageId = embeddedStorageId;
  }

  @Before
  public void setup() {
    remove();
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSpaceWithGlobalVersioning.json",usedStorageId))
        .when().post(getCreateSpacePath()).then();

    response.statusCode(OK.code())
        .body("id", equalTo(getSpaceId()))
        .body("title", equalTo("My Demo Space"))
        .body("enableHistory", equalTo(true))
        .body("enableUUID", equalTo(true))
        .body("enableGlobalVersioning", equalTo(true))
        .body("storage.id", equalTo(usedStorageId));

    /** Check Empty History */
    given().
            accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/history").
            then().
            statusCode(OK.code()).
            body("startVersion", equalTo(0)).
            body("endVersion", equalTo(0)).
            body("versions", equalTo(new HashMap()));

    given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/history/statistics").
            then().
            statusCode(OK.code()).
            body("count.value", equalTo(0)).
            body("maxVersion.value", equalTo(0));

    /**
     * Perform:
     * Insert id: 1-500 (v1) isFree=true
     * Insert id: 501-100 (v2) isFree=true
     * Insert id: 1001-1500 (v3) isFree=true
     * ...
     * Insert id: 4501-5000 (v10) free=true
     *
     * */
    writeFeatures(5000, 500, 1, true);


    /**
     * Perform:
     * Update id: 100-150 (v.11) isFree=false
     * Update id: 150-200 (v.12) isFree=false
     * Update id: 100-200 (v.13) isFree=tue
     * Delete ids: 100,150,200,300 (v.14)
     * */
    modifyFeatures();
  }
}
