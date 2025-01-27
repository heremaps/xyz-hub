package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.Commons;
import io.restassured.response.Response;

import static io.restassured.RestAssured.withArgs;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

public class OutputFeature {

  private static final String UUID_KEY = Commons.UUID_KEY;
  private static final String PUUID_KEY = "puuid";
  private static final String CREATED_AT_KEY = "createdAt";
  private static final String UPDATED_AT_KEY = "updatedAt";

  private static final long TEN_SECONDS_IN_MS = 100000;

  private final String shortId;
  private final Response response;
  private final boolean singleFeatureResponse;
  private final String uuid;
  private final String puuid;
  private final long createdAt;
  private final long updatedAt;

  /**
   * From features collection
   */
  public OutputFeature(String shortId, Response response) {
    this.shortId = shortId;
    this.response = response;
    this.singleFeatureResponse = false;
    this.uuid = getXyzNamespaceProperty(UUID_KEY);
    this.puuid = getXyzNamespaceProperty(PUUID_KEY);
    this.createdAt = getXyzNamespaceProperty(CREATED_AT_KEY);
    this.updatedAt = getXyzNamespaceProperty(UPDATED_AT_KEY);
  }

  /**
   * From single feature
   */
  public OutputFeature(Response response) {
    this.shortId = response.then()
      .extract()
      .path("id");
    this.response = response;
    this.singleFeatureResponse = true;
    this.uuid = getXyzNamespaceProperty(UUID_KEY);
    this.puuid = getXyzNamespaceProperty(PUUID_KEY);
    this.createdAt = getXyzNamespaceProperty(CREATED_AT_KEY);
    this.updatedAt = getXyzNamespaceProperty(UPDATED_AT_KEY);
  }

  public String getUuid() {
    return uuid;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void performNewFeatureAssertions() {
    assertOnlyOneFeatureWithId();
    assertNotNull(uuid);
    assertNull(puuid);
    assertEquals(createdAt, updatedAt);
    assertTrue(System.currentTimeMillis() > createdAt);
    assertTrue(System.currentTimeMillis() < createdAt + TEN_SECONDS_IN_MS);
  }

  public void performUpdatedFeatureAssertions(String expectedPuuid, long expectedCreatedAt) {
    assertOnlyOneFeatureWithId();
    assertNotNull(uuid);
    assertEquals(expectedPuuid, puuid);

    assertEquals(expectedCreatedAt, createdAt);
    assertTrue(updatedAt > createdAt);
    assertTrue(System.currentTimeMillis() > updatedAt);
    assertTrue(System.currentTimeMillis() < updatedAt + TEN_SECONDS_IN_MS);
  }

  public void performExistingAssertions(OutputFeature expectedFeature) {
    assertOnlyOneFeatureWithId();
    assertEquals(expectedFeature.uuid, uuid);
    assertEquals(expectedFeature.puuid, puuid);
    assertEquals(expectedFeature.createdAt, createdAt);
    assertEquals(expectedFeature.updatedAt, updatedAt);
  }

  public void assertOnlyOneFeatureWithId() {
    if (singleFeatureResponse) {
      return;
    }
    response.then()
      .assertThat()
      .body("features.findAll{it.id.endsWith(':%s')}.size()", withArgs(shortId), equalTo(1));
  }

  public <T> T getXyzNamespaceProperty(String propertyName) {
    if (singleFeatureResponse) {
      return response.then()
        .extract()
        .path("properties.@ns:com:here:xyz.%s", propertyName);
    } else {
      return response.then()
        .extract()
        .path("features.find{it.id.endsWith(':%s')}.properties.@ns:com:here:xyz.%s", shortId, propertyName);
    }
  }
}
