package com.here.naksha.app.service;

import static com.here.naksha.app.common.CommonApiTestSetup.createHandler;
import static com.here.naksha.app.common.CommonApiTestSetup.createSpace;
import static com.here.naksha.app.common.CommonApiTestSetup.createStorage;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MissingCollectionTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient(300);

  private static final String SPACE_WITH_AUTO_CREATE_ON = "space_with_auto_create";
  private static final String SPACE_WITH_AUTO_CREATE_OFF = "space_without_auto_create";
  private static final String SPACE_WITH_UNDEFINED_AUTO_CREATE = "space_with_undefined_auto_create";

  @BeforeAll
  static void setup() throws Exception {
    createStorage(nakshaClient, "MissingCollection/setup/create_storage.json");
    createHandler(nakshaClient, "MissingCollection/setup/create_event_handler_with_auto_create.json");
    createHandler(nakshaClient, "MissingCollection/setup/create_event_handler_with_undefined_auto_create.json");
    createHandler(nakshaClient, "MissingCollection/setup/create_event_handler_without_auto_create.json");
    createSpace(nakshaClient, "MissingCollection/setup/create_space_with_auto_create.json");
    createSpace(nakshaClient, "MissingCollection/setup/create_space_with_undefined_auto_create.json");
    createSpace(nakshaClient, "MissingCollection/setup/create_space_without_auto_create.json");
  }

  @Test
  void tc1200_shouldFailWritingToMissingCollection() throws Exception {
    // Given: feature to create on space without autoCreate enabled
    final String bodyJson = loadFileOrFail("MissingCollection/TC1200_failOnWrite/create_features.json");
    final String expectedFailure = loadFileOrFail("MissingCollection/TC1200_failOnWrite/error_response.json");
    String streamId = UUID.randomUUID().toString();

    // When: trying to create features
    HttpResponse<String> response = nakshaClient.post("hub/spaces/" + SPACE_WITH_AUTO_CREATE_OFF + "/features", bodyJson, streamId);

    // Then: we get en 404 due to missing collection
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedFailure);
  }

  @Test
  void tc1201_shouldFailReadingFromMissingCollection() throws Exception {
    // Given: expected failure response
    String streamId = UUID.randomUUID().toString();
    final String expectedNotFoundFailure = loadFileOrFail("MissingCollection/TC1201_failOnRead/error_response.json");

    // When: trying to read features
    HttpResponse<String> response = nakshaClient.get(notExistingFeaturesPath(SPACE_WITH_AUTO_CREATE_OFF), streamId);

    // Then: we get en 404 due to missing collection
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedNotFoundFailure);
  }

  @ParameterizedTest
  @ValueSource(strings = {SPACE_WITH_AUTO_CREATE_ON, SPACE_WITH_UNDEFINED_AUTO_CREATE})
  void tc1202_shouldSucceedWritingToMissingCollection(String spaceId) throws Exception {
    // Given: feature to create on space without autoCreate enabled
    final String bodyJson = loadFileOrFail("MissingCollection/TC1202_succeedOnWrite/create_features.json");
    final String expectedResponse = loadFileOrFail("MissingCollection/TC1202_succeedOnWrite/response.json");
    String streamId = UUID.randomUUID().toString();

    // When: trying to create features
    HttpResponse<String> response = nakshaClient.post("hub/spaces/" + spaceId + "/features", bodyJson, streamId);

    // Then: we get en 404 due to missing collection
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedResponse);
  }

  @ParameterizedTest
  @ValueSource(strings = {SPACE_WITH_AUTO_CREATE_ON, SPACE_WITH_UNDEFINED_AUTO_CREATE})
  void tc1203_shouldSucceedReadingFromMissingCollection(String spaceId) throws Exception {
    // Given: expected OK response with no (empty) features returned
    String streamId = UUID.randomUUID().toString();
    final String expectedEmptyResponse = loadFileOrFail("MissingCollection/TC1203_succeedOnRead/empty_response.json");

    // When: trying to read features
    HttpResponse<String> response = nakshaClient.get(notExistingFeaturesPath(spaceId), streamId);

    // Then: we get 200 response with empty features array
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedEmptyResponse);
  }

  private String notExistingFeaturesPath(String spaceId){
    return "hub/spaces/" + spaceId + "/features?id=not_important_1&id=not_important_2";
  }
}
