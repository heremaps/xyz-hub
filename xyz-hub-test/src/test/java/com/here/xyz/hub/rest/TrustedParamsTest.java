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
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TrustedParamsTest extends TestWithSpaceCleanup {

  static final Map<String, String> exampleHeaders = new HashMap<String, String>() {{
    putAll(getAuthHeaders(AuthProfile.ACCESS_ALL));
    put("AuthN", "bearer abc");
    put("never", "none");
    put("X-request-ID", "REQ-123-456");
    put("custom_header", "my_custom_header_value");
  }};

  static final Map<String, String> exampleCookies = new HashMap<String, String>() {{
    put("here_access", "my_PRD_jwt_cookie");
    put("here_access_st", "my_DEV_jwt_cookie");
  }};

  @BeforeClass
  public static void beforeClass() {
    cleanUpId = "space-with-js-connector";
  }

  @Before
  public void before() {
    removeSpace(cleanUpId);
  }

  @Ignore("Disabled until javascript engine is working in EvalConnector for Java16")
  @Test
  public void untrustedConnector() {
    final String code = "if (event.getTrustedParams() == null) throw 'valid';";
    final String body = "{\"id\":\""+cleanUpId+"\",\"title\":\""+cleanUpId+"\",\"storage\":{\"id\":\"evaljs-untrusted\",\"params\":{\"code\":\""+code+"\"}}}";

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .cookies(exampleCookies)
        .headers(exampleHeaders)
        .body(body)
        .when()
        .post("/spaces")
        .then()
        .body("errorMessage", equalTo("valid"));
  }

  @Ignore("Disabled until javascript engine is working in EvalConnector for Java16")
  @Test
  public void trustedConnector() {
    final String code = "if (event.getTrustedParams() != null && event.getTrustedParams().getCookies().get('here_access') == 'my_PRD_jwt_cookie' && !event.getTrustedParams().getCookies().containsKey('here_access_st')) throw 'valid';";
    final String body = "{\"id\":\""+cleanUpId+"\",\"title\":\""+cleanUpId+"\",\"storage\":{\"id\":\"evaljs-trusted\",\"params\":{\"code\":\""+code+"\"}}}";

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .cookies(exampleCookies)
        .headers(exampleHeaders)
        .body(body)
        .when()
        .post("/spaces")
        .then()
        .body("errorMessage", equalTo("valid"));
  }
}
