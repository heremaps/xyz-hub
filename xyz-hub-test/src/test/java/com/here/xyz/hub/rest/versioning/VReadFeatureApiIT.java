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

package com.here.xyz.hub.rest.versioning;

import com.here.xyz.hub.rest.ReadFeatureApiIT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class VReadFeatureApiIT extends ReadFeatureApiIT {

  @BeforeClass
  public static void setup() {
    VersioningBaseIT.setup();
  }
  @AfterClass
  public static void tearDown() {
    VersioningBaseIT.tearDown();
  }

  @Test
  @Override
  public void testStatistics() {
    given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/statistics").
            then().
            statusCode(OK.code()).
            body("minVersion.value", equalTo(0)).
            body("maxVersion.value", equalTo(1)).
            body("count.value",  equalTo(252)).
            body("count.estimated", equalTo(false)).
            body("byteSize.value", greaterThan(0)).
            body("byteSize.estimated", equalTo(true)).
            body("properties", notNullValue());
  }

  @Test
  @Override
  public void testStatisticsFastMode() {
    given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/statistics?fastMode=true").
            then().
            statusCode(OK.code()).
            body("minVersion.value", equalTo(0)).
            body("maxVersion.value", equalTo(1)).
            body("count.value", equalTo(252)).
            body("count.estimated", equalTo(false)).
            body("byteSize.value", greaterThan(0)).
            body("byteSize.estimated", equalTo(true)).
            body("properties", nullValue());
  }
}
