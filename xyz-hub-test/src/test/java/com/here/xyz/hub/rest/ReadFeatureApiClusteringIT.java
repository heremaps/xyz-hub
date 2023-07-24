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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

@Category(RestTests.class)
public class ReadFeatureApiClusteringIT extends TestSpaceWithFeature {

  private static final String DB_URL = System.getenv().getOrDefault("STORAGE_DB_URL", "jdbc:postgresql://localhost/postgres");
  private static final String DB_USER = System.getenv().getOrDefault("STORAGE_DB_USER", "postgres");
  private static final String DB_PASSWORD = System.getenv().getOrDefault("STORAGE_DB_PASSWORD", "password");
  private static final String SPACE_ID = "space_with_large_data_test";

  @BeforeClass
  public static void setup() {
    removeSpace(SPACE_ID);
    createSpaceWithCustomStorage(SPACE_ID, "psql", null);
    addFeaturesForClustering();
    executeAnalyzeQuery();
  }

  @AfterClass
  public static void tearDownClass() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void testBBoxAndTileClusteringParamNegative() {
    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/"+ SPACE_ID + "/bbox?west=179&north=89&east=-179&south=-89&clustering=abc123")
        .then()
        .statusCode(BAD_REQUEST.code()).extract().body().asString();

    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/"+ SPACE_ID + "/tile/quadkey/120?clustering=abc123")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void readByBoundingBoxWithQuadbinClustering() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/bbox?west=12&north=51&east=15&south=54&clustering=quadbin").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(4)).
            body("features.properties.count.sum()", equalTo(30000));
  }

  @Test
  public void readByBoundingBoxWithQuadbinClusteringAndPropertySearch() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/bbox?west=12&north=51&east=15&south=54&clustering=quadbin&p.type=type1").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(4)).
            body("features.properties.count.sum()", equalTo(7500));
  }

  @Test
  public void readByBoundingBoxWithHexbinClustering() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/bbox?west=12&north=51&east=15&south=54&clustering=hexbin").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(120)).
            body("features.properties.aggregation.qty.sum()", equalTo(30000));
  }

  @Test
  public void readByBoundingBoxWithHexbinClusteringAndPropertySearch() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/bbox?west=12&north=51&east=15&south=54&clustering=hexbin&p.type=type2").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(118)).
            body("features.properties.aggregation.qty.sum()", equalTo(7500));
  }

  @Test
  public void readByTileWithQuadbinClustering() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/5_17_10?clustering=quadbin&clustering.relativeResolution=2").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(4)).
            body("features.properties.count.sum()", equalTo(30000));
  }

  @Test
  public void readByTileWithHexbinClustering() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/5_17_10?clustering=hexbin").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(23)).
            body("features.properties.aggregation.qty.sum()", equalTo(30000));
  }

  @Test
  public void readByTileWithVizSamplingLow() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/5_17_10?mode=viz&vizSampling=low").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(30000));
  }

  @Test
  public void readByTileWithVizSamplingHigh() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/5_17_10?mode=viz&vizSampling=high").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(9434));
  }

  private static void addFeaturesForClustering() {
    given()
            .contentType(APPLICATION_GEO_JSON)
            .accept(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            // This json contains 30000 random features
            .body(content("/xyz/hub/largeDatasetWith30kFeatures.json"))
            .when()
            .put(getSpacesPath() + "/" + SPACE_ID + "/features")
            .then()
            .statusCode(OK.code());
  }

  private static void executeAnalyzeQuery() {

    try(Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        Statement stmt = conn.createStatement();
    ) {
      stmt.executeUpdate("ANALYZE VERBOSE");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
