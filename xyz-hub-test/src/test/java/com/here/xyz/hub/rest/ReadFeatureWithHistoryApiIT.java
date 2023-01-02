/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RestTests.class)
public class ReadFeatureWithHistoryApiIT extends ReadFeatureApiIT {

  @BeforeClass
  public static void setup() {
    remove();
    createSpace(true);
    addFeatures();
  }

  @Test
  @Override
  public void testHeaderOutputSizeReporting() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/tile/quadkey/2100300120310022").
        then().
        header("X-Decompressed-Input-Size", "0").
        header("X-Decompressed-Output-Size", "541").
        statusCode(OK.code());
  }
}
