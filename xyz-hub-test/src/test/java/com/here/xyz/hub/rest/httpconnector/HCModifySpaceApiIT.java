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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.here.xyz.hub.rest.ModifySpaceApiIT;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class HCModifySpaceApiIT extends ModifySpaceApiIT {

  @Before
  public void setup() {
    usedStorageId = httpStorageId;
    super.setup();
  }

  @AfterClass
  public static void tearDownClass() {
    usedStorageId = embeddedStorageId;
  }

  @Test
  public void setSearchablePropertiesPositive() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/updateSpaceWithSearchableProperties.json", usedStorageId))
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("searchableProperties.name", equalTo(true))
        .body("searchableProperties.other", equalTo(false));
  }

  public void setSearchablePropertiesNegative() {}

  public void removeAllListeners() {}

  public void removeAllProcessors() {}

  public void addProcessorToExistingSpace() {}

  @Test
  public void testConnectorResponseInModifiedSpace() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body("{\"description\": \"Added description\"}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("title", is("My Demo Space"))
        .body("description", is("Added description"))
        .body("storage", notNullValue())
        .body("storage.id", is(usedStorageId));
  }

  @Test
  public void testRemoveStorage() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body("{\"storage\": null}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void patchWithoutChange() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"title\": \"My Demo Space\"}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code());
  }
}
