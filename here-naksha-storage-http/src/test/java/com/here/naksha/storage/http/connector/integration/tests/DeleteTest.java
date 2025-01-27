package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.Commons;
import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.*;

public class DeleteTest {

  public static final String NO_ID_PROVIDED_ERROR_MSG = "Invalid request input parameter value for QUERY-parameter 'id'. Reason: MISSING_PARAMETER_WHEN_REQUIRED_ERROR";
  public static final String INVALID_URN_ERROR_MSG = "Error response : Incorrect URN format";

  @BeforeEach
  void setUp() {
    rmAllFeatures();
    createTestFeatures();
  }


  @Test
  void deleteOneFeature() {
    Response response = Naksha.request().with().queryParam("id", URN_PREFIX + "1").delete("features");
    assertStatusCode200(response);
    assertHasExactlyTheseIds(response, "1");

    Response leftInDb = DataHub.request().get("iterate");
    assertHasExactlyTheseIds(leftInDb, "2", "3");
  }

  @Test
  void returnEmptyCollectionOnNothingToDelete() {
    Response response = Naksha.request().with().queryParam("id", URN_PREFIX + "notExist").delete("features");
    assertStatusCode200(response);
    response.then().assertThat().body("features", hasSize(0));

    Response leftInDb = DataHub.request().get("iterate");
    assertHasExactlyTheseIds(leftInDb, "1", "2", "3");
  }

  @Test
  void shouldDeleteMultipleFeatures() {
    Response response = Naksha.request().with()
      // comma should not be encoded
      .urlEncodingEnabled(false)
      .queryParam("id", encode(URN_PREFIX, UTF_8) + "1," + encode(URN_PREFIX, UTF_8) + "2")
      .delete("features");
    assertStatusCode200(response);
    assertHasExactlyTheseIds(response, "1", "2");

    Response leftInDb = DataHub.request().get("iterate");
    assertHasExactlyTheseIds(leftInDb, "3");
  }

  @Test
  void shouldDeleteMultipleFeatures_MultipleIdParams() {
    Response response = Naksha.request().with()
      .queryParam("id", URN_PREFIX + "1")
      .queryParam("id", URN_PREFIX + "2")
      .delete("features");
    assertStatusCode200(response);
    assertHasExactlyTheseIds(response, "1", "2");

    Response leftInDb = DataHub.request().get("iterate");
    assertHasExactlyTheseIds(leftInDb, "3");
  }

  @Test
  void shouldDeleteMultipleFeatureIfSomeFeaturesNotExist() {
    Response response = Naksha.request().with()
      .queryParam("id", URN_PREFIX + "1")
      .queryParam("id", URN_PREFIX + "2")
      .queryParam("id", URN_PREFIX + "i_dont_exist")
      .queryParam("id", URN_PREFIX + "me_neither")
      .delete("features");
    assertStatusCode200(response);
    assertHasExactlyTheseIds(response, "1", "2");

    Response leftInDb = DataHub.request().get("iterate");
    assertHasExactlyTheseIds(leftInDb, "3");
  }

  @Test
  void returnErrorOnNoIdsProvided() {
    Response response = Naksha.request().delete("features");
    response.then().assertThat()
      .body("error", equalTo("IllegalArgument"))
      .and()
      .body("errorMessage", equalTo(NO_ID_PROVIDED_ERROR_MSG));
  }

  @Test
  void shouldThrowOnBadUrn() {
    Response response = Naksha.request()
      .with().queryParam("id", "not_valid_feature_urn")
      .delete("features");
    response.then().assertThat()
      .body("error", equalTo("IllegalArgument"))
      .and()
      .body("errorMessage", equalTo(INVALID_URN_ERROR_MSG));
  }

  /**
   * Asserts that response contains these and only these features
   * that has given short ids (ids without {@link Commons#URN_PREFIX})
   */
  private void assertHasExactlyTheseIds(Response response, String... shortIds) {
    ValidatableResponse assertThat = response.then().assertThat();
    assertThat.body("features", hasSize(shortIds.length));
    for (String shortId : shortIds) {
      assertThat.body("features.id", hasItems(endsWith(shortId)));
    }
  }

  private void createTestFeatures() {
    DataHub.createFeatureFromJsonTemplateFile("delete/feature_template.json", "1");
    DataHub.createFeatureFromJsonTemplateFile("delete/feature_template.json", "2");
    DataHub.createFeatureFromJsonTemplateFile("delete/feature_template.json", "3");
  }
}
