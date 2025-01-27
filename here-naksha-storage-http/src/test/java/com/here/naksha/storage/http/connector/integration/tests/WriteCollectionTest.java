package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace.*;
import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public abstract class WriteCollectionTest {
  static final String FEATURE_A_ID = "A";
  static final String FEATURE_B_ID = "B";
  static final String FEATURE_C_ID = "C";
  static final String FEATURE_D_ID = "D";

  abstract Response sendRequest(RequestSpecification requestSpecification);

  Response writeFeatures(List<InputFeature> features) {
    return sendRequest(createWriteFeaturesRequest(features));
  }

  Response writeFeature(InputFeature feature) {
    return sendRequest(createWriteFeaturesRequest(feature));
  }

  @Test
  void write() {
    Response responseANew = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "1"))); // insert single to empty database
    assertStatusCode200(responseANew);
    OutputFeature outputFeatureANew = new OutputFeature(FEATURE_A_ID, responseANew);
    outputFeatureANew.performNewFeatureAssertions();

    Response responseAUpdated = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "2"))); // update single
    assertStatusCode200(responseAUpdated);
    OutputFeature outputFeatureAUpdated = new OutputFeature(FEATURE_A_ID, responseAUpdated);
    outputFeatureAUpdated.performUpdatedFeatureAssertions(
      outputFeatureANew.getUuid(),
      outputFeatureANew.getCreatedAt()
    );

    Response responseBNew = writeFeature(new InputFeature(FEATURE_B_ID, Map.of("p", "1"))); // insert single to non empty database
    assertStatusCode200(responseBNew);
    OutputFeature outputFeatureBNew = new OutputFeature(FEATURE_B_ID, responseBNew);
    outputFeatureBNew.performNewFeatureAssertions();

    Response responseComplex = writeFeatures(List.of( // complex request, update single again
      new InputFeature(FEATURE_A_ID, Map.of("p", "3")),
      new InputFeature(FEATURE_B_ID, Map.of("p", "2")),
      new InputFeature(FEATURE_C_ID, Map.of("p", "1")),
      new InputFeature(FEATURE_D_ID, Map.of("p", "1"))
    ));
    assertStatusCode200(responseComplex);
    OutputFeature outputFeatureAUpdatedAgain = new OutputFeature(FEATURE_A_ID, responseComplex);
    OutputFeature outputFeatureBUpdated = new OutputFeature(FEATURE_B_ID, responseComplex);
    OutputFeature outputFeatureCNew = new OutputFeature(FEATURE_C_ID, responseComplex);
    OutputFeature outputFeatureDNew = new OutputFeature(FEATURE_D_ID, responseComplex);
    outputFeatureAUpdatedAgain.performUpdatedFeatureAssertions(
      outputFeatureAUpdated.getUuid(),
      outputFeatureAUpdated.getCreatedAt()
    );
    outputFeatureBUpdated.performUpdatedFeatureAssertions(
      outputFeatureBNew.getUuid(),
      outputFeatureBNew.getCreatedAt()
    );
    outputFeatureCNew.performNewFeatureAssertions();
    outputFeatureDNew.performNewFeatureAssertions();

    Response iterateResponse = DataHub.request().get("/iterate");
    new OutputFeature(FEATURE_A_ID, iterateResponse).performExistingAssertions(outputFeatureAUpdatedAgain);
    new OutputFeature(FEATURE_B_ID, iterateResponse).performExistingAssertions(outputFeatureBUpdated);
    new OutputFeature(FEATURE_C_ID, iterateResponse).performExistingAssertions(outputFeatureCNew);
    new OutputFeature(FEATURE_D_ID, iterateResponse).performExistingAssertions(outputFeatureDNew);
  }

  RequestSpecification createWriteFeaturesRequest(InputFeature feature) {
    return createWriteFeaturesRequest(List.of(feature));
  }

  RequestSpecification createWriteFeaturesRequest(List<InputFeature> features) {
    String featuresArrayJson = features.stream()
      .map(InputFeature::toJson)
      .collect(Collectors.joining(", ", "[", "]"));
    String featuresCollectionJson = readTestResourcesFile("postAndPut/feature_collection_template.json").formatted(featuresArrayJson);
    RequestSpecification request = Naksha.request()
      .with().body(featuresCollectionJson)
      .with().header("Content-Type", "application/json");
    return request;
  }

  @Test
  void writeEmpty() {
    Response responseEmpty = writeFeatures(List.of());
    responseEmpty.then().assertThat().statusCode(400)
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("error", equalTo("IllegalArgument"))
      .and().body("errorMessage", matchesRegex("Can't .* empty features"));

    assertDbEmpty();
  }

  @Test
  void errorOnNewWithUuid() {
    Map propertiesWithUuid = Map.of("@ns:com:here:xyz",
      Map.of(UUID_KEY, randomUUID())
    );
    Response responseUuid = writeFeature(new InputFeature(FEATURE_A_ID, propertiesWithUuid));
    responseUuid.then().assertThat().statusCode(greaterThanOrEqualTo(400))
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("errorMessage", matchesRegex("The feature with id .* cannot be created. Property UUID should not be provided as input."));
    assertDbEmpty();
  }

  @Test
  void updateWithMatchingUuid() {
    Response responseNew = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "1")));
    assertStatusCode200(responseNew);
    OutputFeature outputNew = new OutputFeature(FEATURE_A_ID, responseNew);

    Map propertiesWithUuid = Map.of("@ns:com:here:xyz",
      Map.of(UUID_KEY, outputNew.getUuid())
    );
    Response responseUpdated = writeFeature(new InputFeature(FEATURE_A_ID, propertiesWithUuid));
    assertStatusCode200(responseUpdated);
    new OutputFeature(FEATURE_A_ID, responseUpdated).performUpdatedFeatureAssertions(
      outputNew.getUuid(),
      outputNew.getCreatedAt()
    );
  }

  @Test
  void errorOnUpdateWithNonMatchingUuid() {
    Response responseNew = writeFeature(new InputFeature(FEATURE_A_ID, Map.of("p", "1")));
    assertStatusCode200(responseNew);
    OutputFeature outputNew = new OutputFeature(FEATURE_A_ID, responseNew);

    Map propertiesWithUuid = Map.of("@ns:com:here:xyz",
      Map.of(UUID_KEY, randomUUID())
    );
    Response responseUpdated = writeFeature(new InputFeature(FEATURE_A_ID, propertiesWithUuid));
    String expectedErrorMessage = "The feature with id urn:here::here:landmark3d.Landmark3dPhotoreal:A cannot be replaced. The provided UUID doesn't match the UUID of the head state: %s"
      .formatted(outputNew.getUuid());
    responseUpdated.then()
      .assertThat().statusCode(HTTP_CONFLICT)
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("error", equalTo("Conflict"))
      .and().body("errorMessage", equalTo(expectedErrorMessage));
  }

  @Test
  void ignoreCreateAtUpdateAtAndPuuidProvidedInRequest() {
    Map xyzPropertiesToBeIgnored = Map.of(
      PUUID, randomUUID(),
      CREATED_AT, System.currentTimeMillis(),
      UPDATED_AT, System.currentTimeMillis()
    );

    Map propertiesNew = Map.of(
      "p", "1",
      "@ns:com:here:xyz", xyzPropertiesToBeIgnored);
    Response responseNew = writeFeature(new InputFeature(FEATURE_A_ID, propertiesNew));
    assertStatusCode200(responseNew);
    OutputFeature outputNew = new OutputFeature(FEATURE_A_ID, responseNew);
    assertNotEquals(xyzPropertiesToBeIgnored.get(PUUID), outputNew.getXyzNamespaceProperty(PUUID));
    assertNotEquals(xyzPropertiesToBeIgnored.get(CREATED_AT), outputNew.getXyzNamespaceProperty(CREATED_AT));
    assertNotEquals(xyzPropertiesToBeIgnored.get(UPDATED_AT), outputNew.getXyzNamespaceProperty(UPDATED_AT));

    Map propertiesUpdated = Map.of(
      "p", "2",
      "@ns:com:here:xyz", xyzPropertiesToBeIgnored);
    Response responseUpdated = writeFeature(new InputFeature(FEATURE_A_ID, propertiesUpdated));
    assertStatusCode200(responseUpdated);
    OutputFeature outputUpdated = new OutputFeature(FEATURE_A_ID, responseUpdated);
    assertNotEquals(xyzPropertiesToBeIgnored.get(PUUID), outputUpdated.getXyzNamespaceProperty(PUUID));
    assertNotEquals(xyzPropertiesToBeIgnored.get(CREATED_AT), outputUpdated.getXyzNamespaceProperty(CREATED_AT));
    assertNotEquals(xyzPropertiesToBeIgnored.get(UPDATED_AT), outputUpdated.getXyzNamespaceProperty(UPDATED_AT));
  }

  @Test
  void errorOnNoBody() {
    Response responseEmpty = Naksha.request()
      .with().header("Content-Type", "application/json")
      .post("/features");
    responseEmpty.then().assertThat().statusCode(400)
      .and().body("type", equalTo("ErrorResponse"))
      .and().body("error", equalTo("IllegalArgument"))
      .and().body("errorMessage", matchesRegex("(?s)\\[Bad Request].*No content.*"));

    assertDbEmpty();
  }

  record InputFeature(String shortId, Map properties) {
    String toJson() {
      return readTestResourcesFile("postAndPut/feature_template.json")
        .formatted(shortId, JsonSerializable.serialize(properties));
    }
  }
}
