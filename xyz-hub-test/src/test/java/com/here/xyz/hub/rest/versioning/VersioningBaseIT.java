package com.here.xyz.hub.rest.versioning;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.hub.rest.TestSpaceWithFeature;

public class VersioningBaseIT extends TestSpaceWithFeature {

  protected static void createSpace(String spaceId, String spacePath, long versionsToKeep) {
    createSpace(spaceId, spacePath, versionsToKeep, false);
  }

  protected static void createSpace(String spaceId, String spacePath, long versionsToKeep, boolean enableUUID) {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\""+spaceId+"\",\"title\":\""+spaceId+"\",\"versionsToKeep\":"+versionsToKeep+",\"enableUUID\":"+enableUUID+"}")
        .when()
        .post(spacePath)
        .then()
        .statusCode(OK.code());
  }

  protected static void setup() {
    String spaceId = getSpaceId();
    removeSpace(spaceId);
    createSpace(spaceId, getSpacesPath(), 10);
    addFeatures(spaceId);
  }

  protected static void tearDown() {
    removeSpace(getSpaceId());
  }

  protected static void countExpected(int expected) {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/x-psql-test/iterate")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(expected));
  }
}
