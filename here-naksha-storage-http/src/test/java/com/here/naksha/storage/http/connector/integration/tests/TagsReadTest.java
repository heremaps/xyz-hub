package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TagsReadTest {

  public static final String BBOX_PATH_AND_PARAMS = "bbox?west=-10&north=10&east=10&south=-10&";

  private static RequestSpecification reqBase() {
    return DataHub.request()
      .with()
      .contentType("application/json");
  }

  @BeforeAll
  static void setUp() {
    rmAllFeatures();
    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {"prop" : "A"}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:1A?addTags=tag1");

    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {"prop" : "A"}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:2A?addTags=tag2");

    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {"prop" : "A"}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:12A?addTags=tag1,tag2");

    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {"prop" : "B"}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:12B?addTags=tag1,tag2");

    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:123?addTags=tag1,tag2,tag3");

    reqBase()
      .body(
        readTestResourcesFile("tags/feature_template.json").formatted("""
          {}""")
      )
      .put("/features/urn:here::here:landmark3d.Landmark3dPhotoreal:3?addTags=tag3");

  }

  @Test
  void testTags() {
    Response response1 = Naksha.request().urlEncodingEnabled(false).get(BBOX_PATH_AND_PARAMS + "tags=tag1");
    assertTrue(responseHasExactShortIds(List.of("1A", "12A", "12B", "123"), response1));

    Response response13 = Naksha.request().urlEncodingEnabled(false).get(BBOX_PATH_AND_PARAMS + "tags=tag1+tag3");
    assertTrue(responseHasExactShortIds(List.of("123"), response13));

    Response response1or3 = Naksha.request().urlEncodingEnabled(false).get(BBOX_PATH_AND_PARAMS + "tags=tag2,tag3");
    assertTrue(responseHasExactShortIds(List.of("2A", "12A", "12B", "123", "3"), response1or3));

    Response responseComplex = Naksha.request().urlEncodingEnabled(false).get(BBOX_PATH_AND_PARAMS + "tags=tag1+tag2,tag3");
    assertTrue(responseHasExactShortIds(List.of("12A", "12B", "123", "3"), responseComplex));

    Response responseWithPropSearch = Naksha.request().urlEncodingEnabled(false).get(BBOX_PATH_AND_PARAMS + "tags=tag1+tag2,tag3&p.prop=A");
    assertTrue(responseHasExactShortIds(List.of("12A"), responseWithPropSearch));
  }

}
