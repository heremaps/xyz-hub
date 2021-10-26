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

import com.google.common.base.Strings;
import com.here.xyz.hub.auth.TestAuthenticator;
import org.junit.After;

public class TestWithSpaceCleanup extends RestAssuredTest {

  static String cleanUpId;

  static void removeSpace(String spaceId) {
    given().
        accept(APPLICATION_JSON).
        headers(TestAuthenticator.getAuthHeaders(AuthProfile.ACCESS_ALL)).
        when().
        delete(getSpacesPath() + "/" + spaceId);
  }

  protected static String getCreateSpacePath() {
    final String spaceName = (System.getenv().containsKey("SPACE_NAME") ? System.getenv("SPACE_NAME") : "");
    return getCreateSpacePath(spaceName);
  }

  static String getCreateSpacePath(String spaceName) {
    if (System.getenv().containsKey("SPACES_PATH"))
      return getSpacesPath() + (Strings.isNullOrEmpty(spaceName) ? "" : ("/" + spaceName));
    return getSpacesPath();
  }

  protected static String getSpacesPath() {
    return (System.getenv().containsKey("SPACES_PATH") ? System.getenv("SPACES_PATH") : "spaces");
  }

  protected static String getSpaceId() {
    return (System.getenv().containsKey("SPACE_ID") ? System.getenv("SPACE_ID") : "x-psql-test");
  }

  @After
  public void tearDownTest() {
    if (cleanUpId != null) {
      removeSpace(cleanUpId);
    }
  }
}
