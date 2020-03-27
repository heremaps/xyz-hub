/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateSpaceApiIT extends TestSpaceWithFeature {

    @BeforeClass
    public static void setup() {
        remove();
        createSpace();
    }

    @AfterClass
    public static void tearDown() {
        remove();
    }

    //TODO: Add tests for changing the owner of a space (positive & negative)

    @Test
    public void updateSpace() {
        final ValidatableResponse response = given().
            accept(APPLICATION_JSON).
            contentType(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body(content("/xyz/hub/updateSpace.json")).
            when().patch("/spaces/x-psql-test").then();

        long createdAt = response.extract().path("createdAt");

        response.statusCode(OK.code()).
            body("id", equalTo("x-psql-test")).
            body("title", equalTo("My Demo Space Updated")).
            body("storage.id", equalTo("psql")).
            body("updatedAt", not(equalTo(createdAt))).
            body("tags.size", equalTo(2));

        /** Test immutable UUID */
        given().
            accept(APPLICATION_JSON).
            contentType(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body(content("/xyz/hub/createSpaceWithUUID.json")).
            when().patch("/spaces/x-psql-test").then()
                .statusCode(BAD_REQUEST.code());
    }
}
