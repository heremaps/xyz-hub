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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.hub.Subscription;
import com.jayway.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TagApiIT extends TestSpaceWithFeature {

    @BeforeClass
    public static void setupClass() {
        removeSpace(getSpaceId());
    }

    @Before
    public void setup() {
        createSpaceWithVersionsToKeep(getSpaceId(), 2, false);
    }

    @After
    public void teardown() {
        removeSpace(getSpaceId());
    }

    private ValidatableResponse _createTag() {
        return given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body("{\"id\":\"XYZ_1\"}")
                .post("/spaces/" + getSpaceId() + "/tags")
                .then();
    }

    @Test
    public void testDeleteSpace() {
        _createTag();
        removeSpace(getSpaceId());
        createSpace();

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(NOT_FOUND.code());
    }

    @Test
    public void createTag() {
        _createTag()
                .statusCode(OK.code())
                .body("id", equalTo("XYZ_1"))
                .body("spaceId", equalTo(getSpaceId()))
                .body("version", equalTo(-1));

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(OK.code());
    }

    @Test
    public void deleteTag() {
        _createTag();

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(OK.code());

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(NOT_FOUND.code());
    }

    @Test
    public void getTagVersion() {
        _createTag();

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(OK.code())
                .body("version", equalTo(-1));
    }

    @Test
    public void updateTagVersion() {
        _createTag();

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body("{\"version\":999}")
                .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(OK.code());

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
                .then()
                .statusCode(OK.code())
                .body("version", equalTo(999));
    }

    @Test
    public void testSubscriptions() {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(content("/xyz/hub/createSubscription.json"))
                .post("/spaces/" + getSpaceId() + "/subscriptions");

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
                .then()
                .statusCode(OK.code());

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + getSpaceId() + "/subscriptions/test-subscription-1")
                .then()
                .statusCode(OK.code());
    }

    @Test
    public void testRemoveSubscriptions() {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(content("/xyz/hub/createSubscription.json"))
                .post("/spaces/" + getSpaceId() + "/subscriptions");

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + getSpaceId() + "/subscriptions/test-subscription-1")
                .then()
                .statusCode(OK.code());

        given()
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
                .then()
                .statusCode(NOT_FOUND.code());
    }
}
