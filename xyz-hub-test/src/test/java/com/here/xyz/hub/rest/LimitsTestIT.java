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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import io.restassured.response.ValidatableResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

public class LimitsTestIT extends TestSpaceWithFeature {

  private String cleanUpId;

  @BeforeClass
  public static void setupTests() {
    remove();
  }

  @Before
  public void setup() {
    createSpaceWithTokenLimits();
  }

  private void createSpaceWithTokenLimits() {
    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LIMITS)).
        body(content("/xyz/hub/createSpace.json")).
        when().post("/spaces").then().
        statusCode(OK.code()).
        body("id", equalTo("x-psql-test"));
  }

  @After
  public void tearDown() {
    remove();
    if (cleanUpId != null) {
      removeSpace(cleanUpId);
    }
  }

  @Test
  public void create2SpacesTest() {
    ValidatableResponse response = given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LIMITS)).
        body(content("/xyz/hub/createSpaceWithoutId.json")).
        when().post("/spaces").then();

    // Extract the ID in case it needs to be deleted.
    cleanUpId = response.extract().path("id");
    response.statusCode(FORBIDDEN.code());
  }

  @Test
  public void addMultipleFeatures() throws InterruptedException {
    add1Feature();
    Thread.sleep(750);
    given().
        contentType(APPLICATION_GEO_JSON).
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LIMITS)).
        body(content("/xyz/hub/processedData.json")).
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void addMoreThen10Mb() throws InterruptedException {
    int featureCount = 1024*12; // around 12 megabytes
    var content = generateFeatureCollection(featureCount);
    var headers = getAuthHeaders(AuthProfile.ACCESS_ALL);
    var response = given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(headers).
        body(content).
        when().post("/spaces/x-psql-test/features");
    response.then().statusCode(OK.code());
  }

  private String generateFeatureCollection(int count) {
    String content = "{\"type\":\"FeatureCollection\",\"features\":[";
    for (int i = 0; i < count; i++) {
      content += generateContentLine(i);
      if (i < count - 1) {
        content += ",";
      }
    }
    content += "]}";
    return content;
  }

  private String generateContentLine(int id) {
    Random rd = new Random();
    String randomString = RandomStringUtils.randomAlphanumeric(900);
    return "{\"id\":\""+id+"\",\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},\"properties\":{\"test\":\""+ randomString+"\"}}";
  }

  @Test
  public void add1Feature() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LIMITS)).
        body(content("/xyz/hub/createFeatureById.json")).
        when().post("/spaces/x-psql-test/features").then().
        statusCode(OK.code()).
        body("features[0].id", equalTo("Q271455"));
  }
}
