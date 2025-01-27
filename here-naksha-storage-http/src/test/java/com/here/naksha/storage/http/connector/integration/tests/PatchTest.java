package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PatchTest {

  private static final String SHORT_A_ID = "A";
  public static final String FULL_A_ID = URN_PREFIX + SHORT_A_ID;

  @BeforeEach
  void rmFeatures() {
    rmAllFeatures();
  }

  @Test
  void patch() {
    createFeatureInDb(FULL_A_ID, "{}");
    Response responseANew = DataHub.request().with().get("/features/" + FULL_A_ID);
    OutputFeature outputFeatureANew = new OutputFeature(responseANew);
    outputFeatureANew.performNewFeatureAssertions();

    Response responseAUpdated = patchFeature(new InputFeature(SHORT_A_ID, Map.of())); // update single
    assertStatusCode200(responseAUpdated);
    OutputFeature outputFeatureAUpdated = new OutputFeature(responseAUpdated);
    outputFeatureAUpdated.performUpdatedFeatureAssertions(
      outputFeatureANew.getUuid(),
      outputFeatureANew.getCreatedAt()
    );
  }

    @Test
  void addAndDeleteTags() {
    createFeatureInDb(FULL_A_ID, "{}");

    InputFeature featureA = new InputFeature(SHORT_A_ID, Map.of());
    Response responseANew = createPatchFeatureRequest(featureA)
      .with().queryParam("addTags", "tag1", "tag2")
      .patch("/features/{featureId}", URN_PREFIX + featureA.shortId);
    assertStatusCode200(responseANew);
    OutputFeature outputFeatureA = new OutputFeature(responseANew);
    assertEquals(List.of("tag1", "tag2"), outputFeatureA.getXyzNamespaceProperty("tags"));

    Response responseAChangedTags = createPatchFeatureRequest(featureA)
      .with().queryParam("addTags", "tag3")
      .with().queryParam("removeTags", "tag1")
      .patch("/features/{featureId}", URN_PREFIX + featureA.shortId);
    assertStatusCode200(responseAChangedTags);
    OutputFeature outputFeatureAChangedTags = new OutputFeature(responseAChangedTags);
    assertEquals(List.of("tag2", "tag3"), outputFeatureAChangedTags.getXyzNamespaceProperty("tags"));
  }

  @Test
  void patchShouldPatchProperties() {
    createFeatureInDb(FULL_A_ID, """
      {"p":"1","q":"1"}
      """);

    Response responseAUpdated = patchFeature(new InputFeature(SHORT_A_ID, Map.of("p", "2")));
    assertStatusCode200(responseAUpdated);

    Response iterateResponse = DataHub.request().get("/iterate");
    new OutputFeature(SHORT_A_ID, iterateResponse).assertOnlyOneFeatureWithId();

    // assert that properties are patched, not replaced as a whole
    iterateResponse.then().body("features.find{it.id.endsWith(':A')}.properties.p", equalTo("2"));
    iterateResponse.then().body("features.find{it.id.endsWith(':A')}.properties.q", equalTo("1"));
  }

  @Test
  void errorOnNoBody() {
    Response responseEmpty = Naksha.request().patch("/features/{featureId}", FULL_A_ID);
    responseEmpty.then().assertThat().statusCode(400)
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("error", equalTo("IllegalArgument"))
      .and().body("errorMessage", equalTo("Bad Request: [Bad Request] Body required"));

    assertDbEmpty();
  }

  @Test
  void errorOnNotMatchingIds() {
    Response responseEmpty = Naksha
      .request()
      .with().body(readTestResourcesFile("patch/feature_template.json").formatted(FULL_A_ID, "{}"))
      .with().header("Content-Type", "application/json")
      .patch("/features/{featureId}", URN_PREFIX + "DEFINITELY_NOT_SHORT_A_ID");
    responseEmpty.then().assertThat().statusCode(400)
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("error", equalTo("IllegalArgument"))
      .and().body("errorMessage", equalTo("URI path parameter featureId is not the same as id in feature request body."));

    assertDbEmpty();
  }

  private Response patchFeature(InputFeature feature) {
    return createPatchFeatureRequest(feature)
      .patch("/features/{featureId}", URN_PREFIX + feature.shortId);
  }

  private RequestSpecification createPatchFeatureRequest(InputFeature feature) {
    return Naksha.request()
      .with().body(feature.toJson())
      .with().header("Content-Type", "application/json");
  }

  private static void createFeatureInDb(String fullId, String propertiesJson) {
    DataHub.createFeatureFromJsonTemplateFile("patch/feature_template.json", fullId, propertiesJson);
  }

  private record InputFeature(String shortId, Map properties) {
    String toJson() {
      String fullId = URN_PREFIX + shortId;
      return readTestResourcesFile("patch/feature_template.json")
        .formatted(fullId, JsonSerializable.serialize(properties));
    }
  }
}
