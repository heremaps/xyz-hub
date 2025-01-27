package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutTest extends WriteCollectionTest {
  @BeforeEach
  void rmFeatures() {
    rmAllFeatures();
  }

  @Override
  Response sendRequest(RequestSpecification requestSpecification) {
    return requestSpecification.put("/features");
  }

  @Test
  void putShouldReplaceProperties() {
    Response responseANew = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "1", "q", "1")));
    assertStatusCode200(responseANew);

    Response responseAUpdated = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "2")));
    assertStatusCode200(responseAUpdated);

    Response iterateResponse = DataHub.request().get("/iterate");
    new OutputFeature(FEATURE_A_ID, iterateResponse).assertOnlyOneFeatureWithId();

    // assert that properties are replaced as a whole, not only patched
    iterateResponse.then().body("features.find{it.id.endsWith(':A')}.properties.p", equalTo("2"));
    iterateResponse.then().body("features.find{it.id.endsWith(':A')}.properties.q", equalTo(null));
  }

  @Test
  void putShouldReplaceTags() {
    InputFeature featureA = new InputFeature(FEATURE_A_ID, Map.of("p", "1", "q", "1"));
    Response responseANew = createWriteFeaturesRequest(featureA)
      .with().queryParam("addTags", "tag1", "tag2")
      .put("/features");
    assertStatusCode200(responseANew);
    OutputFeature outputFeatureA = new OutputFeature(FEATURE_A_ID, responseANew);
    assertEquals(List.of("tag1", "tag2"), outputFeatureA.getXyzNamespaceProperty("tags"));

    Response responseAChangedTags = createWriteFeaturesRequest(featureA)
      .with().queryParam("addTags", "tag3")
      .with().queryParam("removeTags", "tag1") // Ignored as PUT is about replacing not patching
      .put("/features");
    assertStatusCode200(responseAChangedTags);
    OutputFeature outputFeatureAChangedTags = new OutputFeature(FEATURE_A_ID, responseAChangedTags);
    assertEquals(List.of("tag3"), outputFeatureAChangedTags.getXyzNamespaceProperty("tags"));
  }
}
