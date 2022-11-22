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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.jayway.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;

public class TestSpaceWithFeature extends TestWithSpaceCleanup {
  protected static String embeddedStorageId = "psql";
  protected static String httpStorageId = "psql-http";

  protected static String usedStorageId = embeddedStorageId;

  protected static void remove() {
    TestWithSpaceCleanup.removeSpace("x-psql-test");
  }

  private static ValidatableResponse createSpace(String content) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content)
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());
  }

  protected static void createSpace() {
    createSpace(false);
  }

  protected static void createSpace(boolean useHistory) {
    String path = useHistory ? "/xyz/hub/createSpaceWithGlobalVersioning.json" :"/xyz/hub/createSpace.json";
    String content = usedStorageId == embeddedStorageId ? content(path) : content(path, usedStorageId);
    createSpace(content)
            .body("id", equalTo(getSpaceId()))
            .body("title", equalTo("My Demo Space"))
            .body("storage.id", equalTo((usedStorageId)));
  }

  protected static String createSpaceWithCustomStorage(String spaceId, String storageId, JsonObject storageParams) {
    JsonObject storage = new JsonObject()
        .put("id", storageId);
    if (storageParams != null)
      storage.put("params", storageParams);
    JsonObject space = new JsonObject()
        .put("id", spaceId)
        .put("title", "Demo Space with storage " + storageId)
        .put("storage", storage);

    return given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .body(space.encode())
        .when()
        .post(getCreateSpacePath(spaceId))
        .then()
        .statusCode(200).extract().body().path("id");
  }

  protected static String createSpace(final AuthProfile authProfile, final String title, final boolean shared) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(authProfile))
        .body("{\"title\": \""+title+"\", \"description\": \"description for "+title+" space\", \"shared\": " +shared+ "}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .extract()
        .body()
        .path("id");
  }

  protected static void addFeatures() {
    addFeatures("x-psql-test");
  }

  protected static void addFeatures(String toSpace) {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/processedData.json"))
        .when()
        .put(getSpacesPath() + "/" + toSpace + "/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(252));
  }

  static void add10ThousandFeatures() throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(10);

    for (int i = 0; i < 50; i++) {
      final int ii = i;
      executorService.execute(()->{
        FeatureCollection f = new FeatureCollection();
        for (int j = 0; j < 200; j++) {
          try {
            f.getFeatures().add(new Feature().withProperties(new Properties().with("ticketPrice", ii * j))
                .withGeometry(new Point().withCoordinates(new PointCoordinates(ii, j % 90))));
          }
          catch (JsonProcessingException ignored) {}
        }

        given()
            .contentType(APPLICATION_GEO_JSON)
            .accept(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(f.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .then()
            .statusCode(OK.code());
      });
    }

    executorService.awaitTermination(10, TimeUnit.SECONDS);
  }

  @SuppressWarnings("SameParameterValue")
  static void publishSpace(String spaceId) {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/publishSpace.json"))
        .when()
        .patch("/spaces/" + spaceId).then()
        .statusCode(OK.code())
        .body("id", equalTo(spaceId))
        .body("shared", equalTo(true));
  }

  @SuppressWarnings("SameParameterValue")
  static void addListener(String spaceId) {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LISTENER))
        .body(content("/xyz/hub/createSpaceWithListener.json"))
        .when().patch("/spaces/" + spaceId).then();

    response.statusCode(OK.code())
        .body("id", equalTo(spaceId));
  }

  @SuppressWarnings("SameParameterValue")
  static void addProcessor(String spaceId) {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body(content("/xyz/hub/createSpaceWithProcessor.json"))
        .when().patch("/spaces/" + spaceId).then();

    response.statusCode(OK.code())
        .body("id", equalTo(spaceId));
  }

  static void countFeatures(int expected) {
    given().
        accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/x-psql-test/statistics")
        .then()
        .statusCode(OK.code())
        .body("count.value", equalTo(expected),"count.estimated", equalTo(false));
  }

  @SuppressWarnings("SameParameterValue")
  static FeatureCollection generateRandomFeatures(int featureCount, int propertyCount) {
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(propertyCount).collect(Collectors.toList());
    collection.setFeatures(new ArrayList<>());
    try {
      collection.getFeatures().addAll(
          Stream.generate(() -> {
            Feature f = new Feature()
                .withGeometry(
                    new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
                .withProperties(new Properties());
            pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
            return f;
          }).limit(featureCount).collect(Collectors.toList()));
    }
    catch (JsonProcessingException ignored) {}
    return collection;
  }

  static FeatureCollection generateEVFeatures(int startId, int chunksize, Boolean isFree) {
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<Feature> fList = new ArrayList<>();

    for(int i=startId; i < startId+chunksize ; i++){
      Feature f = new Feature()
              .withId(""+i)
              .withGeometry(
                      new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties().with("free" , isFree == null ? random.nextBoolean() : isFree));
      fList.add(f);
    }

    collection.setFeatures(fList);
    return collection;
  }

  protected void validateFeatureProperties(ValidatableResponse resp, String featureId) {
    JsonObject body = new JsonObject(resp
        .extract()
        .body()
        .as(Map.class));

    JsonObject feature = body
        .getJsonArray("features")
        .getJsonObject(0);

    assertEquals(featureId, feature.getString("id"));
    assertEquals("Feature", feature.getString("type"));
    assertEquals("Point", feature.getJsonObject("geometry").getString("type"));

    validateXyzNs(feature.getJsonObject("properties").getJsonObject("@ns:com:here:xyz"));
  }

  protected void validateXyzNs(JsonObject xyzNs) {
    String[] allowedFieldNames = {"space", "createdAt", "updatedAt", "tags"};
    assertThat(xyzNs.fieldNames(), everyItem(isIn(allowedFieldNames)));
  }

  protected void getFeature(String spaceId, String featureId) {
    getFeature(spaceId, featureId, null);
  }
  protected void getFeature(String spaceId, String featureId, Integer statusCode) {
    getFeature(spaceId, featureId, statusCode, null, null);
  }

  protected void getFeature(String spaceId, String featureId, Integer statusCode, String path, String value) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + spaceId + "/features/" + featureId)
        .then();

    if (statusCode != null)
      response.statusCode(statusCode);

    if (path != null && value != null)
      response.body(path, equalTo(value));
  }

  protected void deleteFeature(String spaceId, String featureId) {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/" + spaceId + "/features/" + featureId)
        .then()
        .statusCode(NO_CONTENT.code());
  }

  public void postFeature(String spaceId, Feature feature) {
    postFeature(spaceId, feature, AuthProfile.ACCESS_OWNER_1_ADMIN);
  }

  public void postFeature(String spaceId, Feature feature, AuthProfile authProfile) {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(authProfile))
        .body(feature.serialize())
        .when()
        .post("/spaces/" + spaceId + "/features")
        .then()
        .statusCode(OK.code())
        .body("features[0].id", equalTo(feature.getId()));
  }
}
