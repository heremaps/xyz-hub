package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageReportingApiIT extends TestSpaceWithFeature {


  @BeforeClass
  public static void setup() {
    remove();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void testEmptyResponsePositive() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L));
  }

  @Test
  public void testMissingPermission() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void testWithOneEmptySpacePositive() {
    createSpace();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes.x-psql-test.contentBytes.value", equalTo(8192));
  }

  @Test
  public void testWithOneFilledSpacePositive() {
    createSpace();
    addFeatures();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes.x-psql-test.contentBytes.value", greaterThan(3000));
  }

}
